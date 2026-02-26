import spacy
from daytona import Daytona
from dependency_injector import containers, providers
from langchain_openai import OpenAIEmbeddings
from langchain_redis import RedisConfig, RedisVectorStore
from langgraph.checkpoint.memory import InMemorySaver

from ariesql.config import settings
from ariesql.context_loader import (
    DatabaseContextLoader,
    MSSQLDialect,
    PostgresDialect,
)
from ariesql.logger import Logger
from ariesql.sql_cache import RedisSQLBank
from ariesql.validator import SQLValidator

logger = Logger(__name__).get_logger()


def _load_spacy_model():
    logger.debug("Loading spaCy model...")
    model = spacy.load("en_core_web_sm")
    logger.debug("spaCy model loaded.")
    return model


def _create_redis_vector_store():
    logger.debug("Connecting to Redis at redis://localhost:6379 ...")
    config = RedisConfig(
        redis_url="redis://localhost:6379",
        index_name="sql_bank",
        distance_metric="COSINE",
    )
    store = RedisVectorStore(embeddings=OpenAIEmbeddings(), config=config)
    logger.debug("Redis vector store connected.")
    return store


def _create_daytona():
    logger.debug("Initializing Daytona...")
    instance = Daytona()
    logger.debug("Daytona initialized.")
    return instance


class Container(containers.DeclarativeContainer):
    """Dependency injection container for ArieSQL services."""

    # -- NLP ---------------------------------------------------------------
    nlp = providers.Singleton(_load_spacy_model)

    if settings.DATABASE_MANIFEST.database == "postgres":
        # -- PostgreSQL dialect & context loader --------------------------------
        CONN_STRING = (
            f"postgresql://{settings.DATABASE_MANIFEST.connection_params.username}:"
            f"{settings.DATABASE_MANIFEST.connection_params.password}@"
            f"{settings.DATABASE_MANIFEST.connection_params.host}:"
            f"{settings.DATABASE_MANIFEST.connection_params.port}/"
            f"{settings.DATABASE_MANIFEST.connection_params.database}"
        )
        dialect = providers.Singleton(PostgresDialect, conn_string=CONN_STRING)
    elif settings.DATABASE_MANIFEST.database == "mssql":
        dialect = providers.Singleton(
            MSSQLDialect,
            server=settings.DATABASE_MANIFEST.connection_params.host,
            port=int(settings.DATABASE_MANIFEST.connection_params.port),
            user=settings.DATABASE_MANIFEST.connection_params.username,
            password=settings.DATABASE_MANIFEST.connection_params.password,
            database=settings.DATABASE_MANIFEST.connection_params.database,
            schema=settings.DATABASE_MANIFEST.default_schema,
        )
    else:
        raise ValueError(f"Unsupported database: {settings.DATABASE_MANIFEST.database}")

    context_loader = providers.Singleton(
        DatabaseContextLoader,
        dialect=dialect,
    )

    # -- Redis vector store & SQL bank -------------------------------------
    vector_store = providers.Singleton(_create_redis_vector_store)

    sql_bank = providers.Singleton(
        RedisSQLBank,
        vector_store=vector_store,
    )

    # -- SQL validators ----------------------------------------------------
    validator = providers.Singleton(
        SQLValidator,
        table_policies=settings.DATABASE_MANIFEST.policy,
        blocked_functions=settings.DATABASE_MANIFEST.blocked_functions,
        dialect=settings.DATABASE_MANIFEST.dialect,
        default_schema=settings.DATABASE_MANIFEST.default_schema,
    )

    # -- Daytona -----------------------------------------------------------
    daytona = providers.Singleton(_create_daytona)

    # -- Checkpoint memory -------------------------------------------------
    memory_saver = providers.Singleton(InMemorySaver)


_container: Container | None = None

_WIRING_MODULES = [
    "ariesql.api.chat",
    "ariesql.sql_masker",
    "ariesql.tools.sql_query_tools",
]


def get_container() -> Container:
    """Return the singleton container instance (must call ``init_container`` first)."""
    if _container is None:
        raise RuntimeError(
            "DI container has not been initialised. Call init_container() first."
        )
    return _container


def init_container() -> Container:
    """Create, wire, and eagerly initialize all singletons to avoid cold starts."""
    global _container

    if _container is not None:
        return _container

    logger.debug("Creating DI container...")
    _container = Container()

    logger.debug("Wiring DI container...")
    _container.wire(modules=_WIRING_MODULES)
    logger.debug("DI container wired.")

    logger.debug("Initializing all singletons eagerly...")
    _container.nlp()
    _container.dialect()
    _container.context_loader()
    _container.vector_store()
    _container.sql_bank()
    _container.validator()
    _container.daytona()
    _container.memory_saver()
    logger.debug("All singletons initialized.")

    return _container
