from typing import Annotated
from uuid import UUID

from fastapi.params import Depends
from langchain_core.documents import Document
from langchain_core.vectorstores import InMemoryVectorStore
from langchain_postgres import PGVector
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from dataline.auth import AuthManager, get_auth_manager
from dataline.config import config
from dataline.services.settings import SettingsService
from dataline.utils.utils import get_postgresql_dsn_async

from sqlalchemy import delete, MetaData, Table
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import NoSuchTableError

import logging

logger = logging.getLogger(__name__)


class PersistentChatMemory:
    def __init__(self, auth_manager: Annotated[AuthManager,Depends(get_auth_manager)], settings_service: SettingsService = Depends(SettingsService)):

        self.vector_db_url = config.vector_db_url
        self.auth_manager = auth_manager
        self.settings_service = settings_service

    async def _initialize_embeddings(self, session: AsyncSession) -> OpenAIEmbeddings:
        user_with_model_details = await self.settings_service.get_model_details(session)
        api_key = user_with_model_details.openai_api_key.get_secret_value()
        return OpenAIEmbeddings(
            openai_api_key=api_key,
            model=config.default_embedding_model
        )

    async def _get_vectorstore(self, session: AsyncSession) -> PGVector | InMemoryVectorStore:

        embeddings = await self._initialize_embeddings(session)

        if config.vector_db_type == "pgvector":
            engine = create_async_engine(get_postgresql_dsn_async(config.connection_string), echo=config.echo)
            return PGVector(
                connection=engine,
                use_jsonb=True,
                embeddings=embeddings,
                collection_name="chat_memory_v2",
                create_extension = False
            )

        else:
           return InMemoryVectorStore(embedding=embeddings)



    async def add_conversation(self, session, result: str, conversation_id:UUID, connection_id:UUID):
        """Add conversation with metadata"""

        vectorstore = await self._get_vectorstore(session)

        await vectorstore.aadd_texts(
            texts=[result],
            metadatas=[{
                "conversation_id": str(conversation_id),
                "connection_id": str(connection_id),
                "user_id" : str(await self.auth_manager.get_user_id()),
            }]
        )

    async def get_relevant_memories(self, session: AsyncSession, query: str, k: int = 2):
        """Retrieve relevant past conversations"""

        vectorstore = await self._get_vectorstore(session)

        retriever = vectorstore.as_retriever(
            search_kwargs={
                "k": k,
                "filter": {"user_id": str(await self.auth_manager.get_user_id())}
            }
        )

        try:
            docs: list[Document] = await retriever.ainvoke(query)
            return "\n".join(doc.page_content for doc in docs)
        except Exception as e:
            logger.error(f"Error retrieving long-term memory: {e}")
            return ""


    async def collection_exists(self, session: AsyncSession, connection_id:UUID) -> bool:

        """Checks if collection of a user exists"""

        vectorstore = await self._get_vectorstore(session)
        results = await vectorstore.asimilarity_search(query="", filter={"connection_id": str(connection_id)}, k=1)
        return len(results) > 0

    async def delete_conversation_memory(self, session: AsyncSession, conversation_id: UUID):

        """delete past conversation memory"""

        vectorstore = await self._get_vectorstore(session)
        if config.vector_db_type == "pgvector":
            docs = await vectorstore.asimilarity_search(
                query="",
                k=1000,  
                filter={"conversation_id": str(conversation_id)}
            )
            
            logger.info(f"Found {len(docs)} records for conversation_id: {conversation_id}")
            
            if docs:
                doc_ids = [doc.metadata.get("id") for doc in docs if doc.metadata.get("id")]
                if doc_ids:
                    await vectorstore.adelete(ids=doc_ids)
                    logger.info(f"Deleted {len(doc_ids)} records for conversation_id: {conversation_id}")
        else:
            await vectorstore.adelete(filter={"conversation_id": str(conversation_id)})
            logger.info(f"Deleted records for conversation_id: {conversation_id}")

