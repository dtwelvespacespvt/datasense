import asyncio
import logging
import re
from typing import AsyncGenerator, cast, Dict, Annotated
from uuid import UUID

from collections import defaultdict
from fastapi import Depends
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from openai._exceptions import APIError
from pydantic import ValidationError, BaseModel, Field, constr


from dataline.config import config
from dataline.errors import UserFacingError
from dataline.models.conversation.schema import (
    ConversationOut,
    ConversationWithMessagesWithResultsOut,
)
from dataline.models.llm_flow.enums import QueryStreamingEventType
from dataline.models.llm_flow.schema import (
    QueryOptions,
    RenderableResultMixin,
    ResultType,
    SQLQueryStringResultContent,
    StorableResultMixin, SQLQueryStringResult,
)
from dataline.models.message.schema import (
    BaseMessageType,
    MessageCreate,
    MessageOptions,
    MessageOut,
    MessageWithResultsOut,
    QueryOut, MessageFeedBack,
)
from dataline.models.result.schema import ResultUpdate
from dataline.models.user.schema import UserWithKeys
from dataline.repositories.base import AsyncSession
from dataline.repositories.conversation import (
    ConversationCreate,
    ConversationRepository,
    ConversationUpdate,
)
from dataline.repositories.message import MessageRepository
from dataline.repositories.result import ResultRepository
from dataline.repositories.user import UserRepository
from dataline.services.connection import ConnectionService
from dataline.services.llm_flow.graph import QueryGraphService
from dataline.services.llm_flow.llm_calls.conversation_title_generator import (
    ConversationTitleGeneratorResponse,
    conversation_title_generator_prompt,
)
from dataline.services.llm_flow.llm_calls.mirascope_utils import (
    OpenAIClientOptions,
    call,
)
from dataline.services.llm_flow.prompt import PROMPT_MEMORY
from dataline.services.settings import SettingsService
from dataline.utils.memory import PersistentChatMemory
from dataline.utils.slack import slack_push
from dataline.utils.utils import stream_event_str, format_transcript

from dataline.auth import AuthManager, get_auth_manager

logger = logging.getLogger(__name__)

class MemorySummaryDecision(BaseModel):
    summary: str = Field(..., description="Concise summary of the conversation")
    store_decision: constr(pattern="^(YES|NO)$") = Field(..., description="Whether to store in long-term memory")


class ConversationService:
    conversation_repo: ConversationRepository
    message_repo: MessageRepository
    result_repo: ResultRepository
    def __init__(
        self,
        auth_manager: Annotated[AuthManager,Depends(get_auth_manager)],
        conversation_repo: ConversationRepository = Depends(ConversationRepository),
        message_repo: MessageRepository = Depends(MessageRepository),
        result_repo: ResultRepository = Depends(ResultRepository),
        connection_service: ConnectionService = Depends(ConnectionService),
        settings_service: SettingsService = Depends(SettingsService),
        user_repo: UserRepository = Depends(UserRepository),
        persistent_chat_memory: PersistentChatMemory = Depends(PersistentChatMemory)
    ) -> None:
        self.persistent_chat_memory = persistent_chat_memory
        self.conversation_repo = conversation_repo
        self.message_repo = message_repo
        self.result_repo = result_repo
        self.connection_service = connection_service
        self.settings_service = settings_service
        self.auth_manager = auth_manager
        self.user_repo = user_repo

    connection_service: ConnectionService
    settings_service: SettingsService
    user_repo: UserRepository

    async def generate_title(self, session: AsyncSession, conversation_id: UUID) -> str:
        conversation = await self.get_conversation_with_messages(session, conversation_id)
        if not conversation.messages:
            return "Untitled chat"

        user_details = await self.settings_service.get_model_details(session)
        api_key = user_details.openai_api_key.get_secret_value()
        base_url = user_details.openai_base_url
        first_message_content = conversation.messages[0].message.content

        try:
            title_generator_response = call(
                "gpt-4o-mini",
                response_model=ConversationTitleGeneratorResponse,
                prompt_fn=conversation_title_generator_prompt,
                client_options=OpenAIClientOptions(api_key=api_key, base_url=base_url),
            )(user_message=first_message_content)

            title = title_generator_response.title
            updated_conversation = await self.update_conversation_name(session, conversation_id, title)
            return updated_conversation.name
        except APIError as e:
            raise UserFacingError(e)

    async def create_conversation(
        self,
        session: AsyncSession,
        connection_id: UUID,
        name: str,
    ) -> ConversationOut:
        conversation = await self.conversation_repo.create(
            session, ConversationCreate(connection_id=connection_id, name=name, user_id=await self.auth_manager.get_user_id())
        )
        return ConversationOut.model_validate(conversation)

    async def get_conversation(self, session: AsyncSession, conversation_id: UUID) -> ConversationOut:
        conversation = await self.conversation_repo.get_by_uuid(session, conversation_id)
        return ConversationOut.model_validate(conversation)

    async def get_conversation_with_messages(
        self, session: AsyncSession, conversation_id: UUID
    ) -> ConversationWithMessagesWithResultsOut:
        conversation = await self.conversation_repo.get_with_messages_with_results(session, conversation_id)
        return ConversationWithMessagesWithResultsOut.from_conversation(conversation)

    async def get_conversations(self, session: AsyncSession, skip:int=0, limit=10) -> list[ConversationWithMessagesWithResultsOut]:
        conversations = await self.conversation_repo.list_with_messages_with_results_user(session, await self.auth_manager.get_user_id(), skip, limit)
        return [
            ConversationWithMessagesWithResultsOut.from_conversation(conversation) for conversation in conversations
        ]

    async def delete_conversation(self, session: AsyncSession, conversation_id: UUID) -> None:
        try:
            await self.persistent_chat_memory.delete_conversation_memory(session, conversation_id)
        except Exception as e:
            logger.error("Error while deleting memory for conversation_id: {} error:{}".format(conversation_id, e))
        await self.conversation_repo.delete_by_uuid(session, record_id=conversation_id)

    async def update_conversation_name(
        self, session: AsyncSession, conversation_id: UUID, name: str
    ) -> ConversationOut:
        conversation = await self.conversation_repo.update_by_uuid(
            session, conversation_id, ConversationUpdate(name=name)
        )
        return ConversationOut.model_validate(conversation)

    @classmethod
    def _add_glossary_util(cls, glossary:Dict[str,str], query:str, history:list[BaseMessage])->str:
        pattern = f"<(.+?)>"
        glossary_words =  re.findall(pattern, query)
        for message in history:
            if message.type == BaseMessageType.HUMAN.value and message.content:
                glossary_words.extend(re.findall(pattern, message.content))
        if not glossary_words:
            return query
        glossary_words = set(glossary_words)
        query += "\n\n#####Glossary#######\n"
        for glossary_word in glossary_words:
            if glossary_word in glossary:
                query += glossary_word +": "+glossary.get(glossary_word)+"\n"
        return query

    @classmethod
    def _add_reverse_look_up_util(cls, unique_value_dict: Dict[str,list[tuple[str,str]]], query:str):

        pattern = r"\[(.+?)\]"
        keywords = re.findall(pattern, query)
        if not keywords:
            return query
        keywords = set(keywords)
        query += "\n\n#####Table Look Up#######\n"
        for keyword in keywords:
            for city, table in unique_value_dict.get(keyword , []):
                query += "{}: Column:  {} , Table:  {} \n".format(keyword, city, table)
        return query

    async def query(
        self,
        session: AsyncSession,
        conversation_id: UUID,
        query: str,
        secure_data: bool = True,
        debug: bool = False,
        background_tasks = None,
    ) -> AsyncGenerator[str, None]:

        # Get conversation, connection, user settings
        conversation = await self.get_conversation(session, conversation_id=conversation_id)
        connection = await self.connection_service.get_connection(session, connection_id=conversation.connection_id)
        user_with_model_details = await self.settings_service.get_model_details(session)

        # Create query graph
        query_graph = QueryGraphService(connection=connection)
        history = await self.get_conversation_history(session, conversation.connection_id, conversation.id)

        # Build Memory
        try:
            await self.build_memory(session, connection.id, user_with_model_details)
        except Exception as e:
            logger.error("Cant Build memory ... for conversation_id: {} error:{}".format(conversation_id, e))

        messages: list[BaseMessage] = []
        results: list[ResultType] = []
        # Perform query and execute graph
        langsmith_api_key = user_with_model_details.langsmith_api_key
        cleaned_query = self._add_glossary_util(connection.glossary, query, history)
        cleaned_query =  cleaned_query.strip(' \t\n\r')
        if connection.unique_value_dict is not None:
            cleaned_query = self._add_reverse_look_up_util(connection.unique_value_dict, cleaned_query)

        long_term_memory = None
        try:
            long_term_memory = await self.persistent_chat_memory.get_relevant_memories(session, cleaned_query)
        except Exception as e:
            logger.error("Error Getting Memory For user: {} connectionId: {} e: {}".format(await self.auth_manager.get_user_id(), connection.id, e))

        async for chunk in (query_graph.query(
            query=cleaned_query,
            options=QueryOptions(
                secure_data=secure_data,
                openai_api_key=user_with_model_details.openai_api_key.get_secret_value(),  # type: ignore
                openai_base_url=user_with_model_details.openai_base_url,
                langsmith_api_key=langsmith_api_key.get_secret_value() if langsmith_api_key else None,  # type: ignore
                llm_model=user_with_model_details.preferred_openai_model,
            ),
            history=history,
            long_term_memory=long_term_memory
        )):
            (chunk_messages, chunk_results) = chunk
            if chunk_messages is not None:
                messages.extend(chunk_messages)

            if chunk_results is not None:
                results.extend(chunk_results)
                for result in chunk_results:
                    if isinstance(result, RenderableResultMixin):
                        yield stream_event_str(
                            event=QueryStreamingEventType.ADD_RESULT.value,
                            data=result.serialize_result().model_dump_json(),
                        )

        # Find first AI message from the back
        last_ai_message = None
        for message in reversed(messages):
            if message.type == BaseMessageType.AI.value:
                last_ai_message = message
                break
        else:
            raise Exception("No AI message found in conversation")

        # Store human message and final AI message without flushing
        human_message = await self.message_repo.create(
            session,
            MessageCreate(
                role=BaseMessageType.HUMAN.value,
                content=query,
                conversation_id=conversation_id,
                options=MessageOptions(secure_data=secure_data),
            ),
            flush=False,
        )

        # Store final AI message in history
        stored_ai_message = await self.message_repo.create(
            session,
            MessageCreate(
                role=BaseMessageType.AI.value,
                content=str(last_ai_message.content),
                conversation_id=conversation_id,
                options=MessageOptions(secure_data=secure_data),
            ),
            flush=True,
        )

        # Store results and final message in database
        for result in results:
            if isinstance(result, StorableResultMixin):
                await result.store_result(session, self.result_repo, stored_ai_message.id)

        # Go over stored results, replace linked_id with the stored result_id
        for result in results:
            if hasattr(result, "linked_id"):
                # Find corresponding result with this ephemeral ID
                linked_result = cast(
                    StorableResultMixin,
                    next(
                        (r for r in results if r.ephemeral_id == getattr(result, "linked_id")),
                        None,
                    ),
                )
                # Update linked_id with the stored result_id
                if linked_result:
                    # Update result
                    setattr(result, "linked_id", linked_result.result_id)

                    if isinstance(result, StorableResultMixin) and result.result_id:
                        await self.result_repo.update_by_uuid(
                            session, result.result_id, ResultUpdate(linked_id=linked_result.result_id)
                        )

        # Render renderable results
        serialized_results = [
            result.serialize_result() for result in results if isinstance(result, RenderableResultMixin)
        ]

        query_out = QueryOut(
            human_message=MessageOut.model_validate(human_message),
            ai_message=MessageWithResultsOut(
                message=MessageOut.model_validate(stored_ai_message), results=serialized_results
            ),
        )

        background_tasks.add_task(
            self.build_memory,
            session=session,
            connection_id=connection.id,
            user_with_model_details=user_with_model_details,
            new_user_message=cleaned_query,
            new_ai_message=stored_ai_message.content,
            new_results=results,
            conversation_id=conversation_id,
        )

        yield stream_event_str(event=QueryStreamingEventType.STORED_MESSAGES.value, data=query_out.model_dump_json())


    async def get_conversation_history(self, session: AsyncSession, connection_id: UUID, conversation_id: UUID) -> list[BaseMessage]:
        """
        Get the last 10 messages of a conversation (AI, Human, and System)
        """
        messages = await self.message_repo.get_by_connection_and_user_with_sql_results(session, connection_id, conversation_id, await self.auth_manager.get_user_id(),  n=config.default_conversation_history_limit)
        base_messages = []
        for message in reversed(messages):  # Reverse to get the oldest messages first (chat format)
            if message.role == BaseMessageType.HUMAN.value:
                base_messages.append(HumanMessage(content=message.content))
            elif message.role == BaseMessageType.AI.value:
                base_messages.append(AIMessage(content=message.content))
                if message.results:
                    sqls = [
                        SQLQueryStringResultContent.model_validate_json(result.content).sql
                        for result in message.results
                    ]
                    base_messages.append(AIMessage(content=f"Generated SQL: {str(sqls)}"))
            elif message.role == BaseMessageType.SYSTEM.value:
                base_messages.append(SystemMessage(content=message.content))
            else:
                logger.exception(Exception(f"Unknown message role: {message.role}"))

        return base_messages

    async def _summarize_and_store_transcript(
        self,
        session: AsyncSession,
        transcript: str,
        user_with_model_details: UserWithKeys,
        connection_id: UUID,
        conversation_id: UUID,
    ):
        """Helper to summarize a transcript and store it in persistent memory."""
        try:
            memory_llm = ChatOpenAI(
                model="gpt-4.1-nano",
                temperature=1,
                api_key=user_with_model_details.openai_api_key,
            ).with_structured_output(MemorySummaryDecision)

            result: MemorySummaryDecision = await memory_llm.ainvoke(
                PROMPT_MEMORY.format(conversation=transcript)
            )
        except Exception as e:
            logger.warning(f"Summary generation failed for conversation {conversation_id}: {e}")
            return

        if result.store_decision == "YES":
            await self.persistent_chat_memory.add_conversation(
                session=session,
                result=result.summary,
                conversation_id=conversation_id,
                connection_id=connection_id,
            )
            logger.info(f"🧠 Stored summarized memory for conversation {conversation_id}")
        else:
            logger.info(f"Skipping memory for conversation {conversation_id}")


    async def build_memory(
        self,
        session: AsyncSession,
        connection_id: UUID,
        user_with_model_details: UserWithKeys,
        new_user_message: str | None = None,
        new_ai_message: str | None = None,
        new_results: list | None = None,
        conversation_id: UUID | None = None,
    ):
        """
        Build or update long-term memory using *summarized* content.
        """
        user_id = await self.auth_manager.get_user_id()

        try:
            if await self.persistent_chat_memory.collection_exists(session, connection_id):
                if new_user_message and new_ai_message:
                    sqls = []
                    for r in new_results or []:
                        try:
                            sql_result = SQLQueryStringResult.model_validate(r)
                            sqls.append(sql_result.sql)
                        except ValidationError:
                            continue

                    transcript = format_transcript(
                        user_message=new_user_message,
                        ai_message=new_ai_message,
                        sqls=sqls,
                    )

                    await self._summarize_and_store_transcript(
                        session=session,
                        transcript=transcript,
                        user_with_model_details=user_with_model_details,
                        connection_id=connection_id,
                        conversation_id=conversation_id,
                    )
                return

            messages = await self.message_repo.get_prev_by_connection_and_user_with_sql_results(
                session, connection_id, user_id, n=config.default_memory_conversation_depth
            )
            logger.info(f"Building initial summarized memory for user {user_id} with {len(messages)} messages")

            conversation_doc = defaultdict(list)
            for message in messages:
                conversation_doc[message.conversation_id].append(message)

            for conversation_id, conversations in conversation_doc.items():
                formatted_messages = []
                sqls = []
                for message in conversations:
                    role = "User" if message.role == BaseMessageType.HUMAN.value else "Assistant"
                    formatted_messages.append({"role": role, "content": message.content})
                    if message.results:
                        sqls.extend([
                            SQLQueryStringResultContent.model_validate_json(result.content).sql
                            for result in message.results
                        ])

                transcript = format_transcript(messages=formatted_messages, sqls=sqls)

                if transcript:
                    await self._summarize_and_store_transcript(
                        session=session,
                        transcript=transcript,
                        user_with_model_details=user_with_model_details,
                        connection_id=connection_id,
                        conversation_id=conversation_id,
                    )

        except Exception as e:
            logger.error(f"Error building/updating summarized memory for connection {connection_id}: {e}")


    async def update_feedback(self, session: AsyncSession, message_feedback: MessageFeedBack) -> None:
        conversation_uuid = await self.message_repo.update_feedback(session, message_feedback)
        user_info = await self.auth_manager.get_user_info()
        feedback_action = "👍 *Upvoted*" if message_feedback.is_positive else "👎 *Downvoted*"
        asyncio.create_task(slack_push(
            message=(
                f"📝 *New Message Feedback*\n"
                f"{feedback_action} by *{user_info.id}* (`{user_info.name}`)\n"
                f"• *Message ID:* `{message_feedback.message_id}`\n"
                f"• *Conversation ID:* `{conversation_uuid}`\n"
                f"• *Feedback:* {message_feedback.content}" if message_feedback.content else ""
            )))
