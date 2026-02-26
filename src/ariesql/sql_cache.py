from typing import Optional

from langchain_redis import RedisVectorStore

from ariesql.logger import Logger

logger = Logger(__name__).get_logger()


class RedisSQLBank:
    TOP_K = 10

    def __init__(self, vector_store: RedisVectorStore, threshold: float = 0.9) -> None:
        self.vector_store = vector_store
        self.threshold = threshold

    async def retrieve_sql(self, query: str) -> Optional[list[str]]:
        logger.debug(f"Retrieving similar SQL query for user query: {query}")
        similar_query_docs = await self.vector_store.asimilarity_search_with_score(
            query, k=self.TOP_K
        )
        if not similar_query_docs:
            return None

        sql_queries: list[str] = []

        for doc_tuple in similar_query_docs:
            doc, distance = doc_tuple
            similarity_score = 1 - abs(distance)
            logger.debug(f"Similarity score of a similar query: {similarity_score}")
            if similarity_score >= self.threshold:
                sql_query = doc.metadata.get("sql")
                if sql_query:
                    sql_queries.append(str(sql_query))

        return sql_queries if sql_queries else None

    async def set_sql(self, query: str, sql: str) -> None:
        await self.vector_store.aadd_texts(
            [query], metadatas=[{"query": query, "sql": sql}]
        )
