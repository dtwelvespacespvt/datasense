import logging
import time
from typing import Annotated, Dict, List
from uuid import UUID

from fastapi.params import Depends
from langchain_core.vectorstores import InMemoryVectorStore
from langchain_postgres import PGVector
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from dataline.auth import AuthManager, get_auth_manager
from dataline.config import config
from dataline.services.settings import SettingsService
from dataline.utils.utils import get_postgresql_dsn_async

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

    async def _get_vectorstore(self, session: AsyncSession, collection_name, embeddings = None) -> PGVector | InMemoryVectorStore:

        if embeddings is None:
            embeddings = await self._initialize_embeddings(session)

        if config.vector_db_type == "pgvector":
            engine = create_async_engine(get_postgresql_dsn_async(config.connection_string), echo=config.echo)
            return PGVector(
                connection=engine,
                use_jsonb=True,
                embeddings=embeddings,
                collection_name=collection_name,
                create_extension = False
            )

        else:
           return InMemoryVectorStore(embedding=embeddings)

    async def retrieve_document(self, session: AsyncSession, query:str, collection_id:UUID, k:int, filter_query:dict, min_score:float = 0.75):
        embeddings = await self._initialize_embeddings(session)
        query_embedding = await embeddings.aembed_query(query)
        vectorstore = await self._get_vectorstore(session, collection_id)
        
        results_with_scores = await vectorstore.asimilarity_search_with_score_by_vector(query_embedding, k=k*3, filter=filter_query)
        results_with_scores = [(doc,score) for doc,score in results_with_scores if score>= min_score]
        
        current_time = time.time()
        scored_results = []
        for doc, score in results_with_scores:
            timestamp = doc.metadata.get("timestamp")
            
            final_score = score
            if timestamp:
                hours_old = (current_time - float(timestamp)) / 3600
                recency_score = 1 / (1 + max(0, hours_old))
                final_score = score + (recency_score * 0.1)
            
            scored_results.append((doc, final_score))
            
        scored_results.sort(key=lambda x: x[1], reverse=True)
        
        return [doc for doc, _ in scored_results[:k]]

    async def delete_document(self, session: AsyncSession, collection_id:UUID, filter_query:dict):
        vectorstore = await self._get_vectorstore(session, collection_id)
        await vectorstore.adelete(where=filter_query)
    
    async def add_documents(self, session: AsyncSession, collection_id:UUID, documents:List[str], metadatas:List[Dict]):
        vectorstore = await self._get_vectorstore(session, collection_id)
        await vectorstore.aadd_texts(texts=documents, metadatas=metadatas)

    async def collection_exists(self, session: AsyncSession, collection_id:UUID, filter_query:dict=None):
        embeddings = await self._initialize_embeddings(session)
        vectorstore = await self._get_vectorstore(session, collection_id, embeddings=embeddings)

        if filter_query:
            query_embedding = await embeddings.aembed_query("test")
            results = await vectorstore.asimilarity_search_by_vector(query_embedding, k=1, filter=filter_query)
            return len(results) > 0
        else:
            results = await vectorstore.aget(limit=1)
            return len(results) > 0


