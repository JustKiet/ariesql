import asyncio
import json
import uuid
from typing import Any, TypedDict

import psycopg2
import pymssql
from langchain.tools import ToolRuntime, tool
from psycopg2 import sql

from ariesql._types import Context
from ariesql.config import settings
from ariesql.container import get_container
from ariesql.logger import Logger
from ariesql.sql_masker import mask_ner_and_numbers

logger = Logger(__name__).get_logger()


async def safe_set_sql(masked_query: str, validated_query: str):
    try:
        await get_container().sql_bank().set_sql(masked_query, validated_query)
    except Exception as e:
        logger.error(f"Background set_sql failed: {e}")


class QueryResult(TypedDict, total=False):
    file_path: str
    results: list[dict[str, str]]
    error: str | None


@tool
async def execute_query_mssql(
    runtime: ToolRuntime[Context],
    query: str,
    enforce_user_permissions: bool = False,
) -> QueryResult:
    """
    Executes a SQL query on the SQL Server (MSSQL) database and returns the results.
    Only SELECT queries are allowed (SELECT, WITH).
    User credentials are automatically injected into the query on runtime,
    so you can write the query as if you are the user with no need to worry about the user credentials.
    The query will be validated and modified to include necessary filters based on the user's permissions before execution.

    Args:
        query (str): The SQL query to execute.
        enforce_user_permissions (bool): Whether to enforce user permissions.
        If True, the query will be validated and modified to include necessary filters based on the user's permissions.
        This is useful for user filtering on GLOBAL tables as they don't enforce user filtering by default.
        For USER-scoped tables, the system automatically applies user filtering at runtime, so this flag has no effect on USER-scoped tables.
        Default is False.
    Returns:
        QueryResult: The results of the query as a QueryResult object. Contains the file path to the JSON file with results (for referencing if need visualization), the results as the EXACT JSON-format list of dictionaries (for direct use in analysis), and an error message if an error occurred.
    Raises:
        PermissionError: If the query is not a read method.
    """
    conn = None
    cursor = None
    try:
        user_id = runtime.context.user_id
        masked_query = runtime.context.masked_query
        is_matched_sql = runtime.context.is_matched_sql

        validated_query = (
            get_container()
            .validator()
            .validate_query(
                query,
                current_user_id=user_id,
                enforce_user_filter_on_global_tables=enforce_user_permissions,
            )
        )

        logger.debug(
            f"Executing MSSQL query: {validated_query} with user_id: {user_id} and enforce_user_permissions: {enforce_user_permissions}"
        )

        # Connect to the SQL Server database
        conn = pymssql.connect(
            server=settings.DATABASE_MANIFEST.connection_params.host,
            port=str(settings.DATABASE_MANIFEST.connection_params.port),
            user=settings.DATABASE_MANIFEST.connection_params.username,
            password=settings.DATABASE_MANIFEST.connection_params.password,
            database=settings.DATABASE_MANIFEST.connection_params.database,
        )
        cursor = conn.cursor()

        # Fetch results if it's a SELECT query
        if validated_query.strip().lower().startswith(("select", "with")):
            cursor.execute(validated_query)
            results = cursor.fetchall()

            if masked_query and not is_matched_sql:
                asyncio.create_task(
                    safe_set_sql(masked_query, mask_ner_and_numbers(validated_query))
                )

            # Return an explicit sentinel so the LLM never hallucinates when
            # the query legitimately returns no rows.
            if not results:
                return QueryResult(
                    file_path="",
                    results=[],
                    error="NO_RESULTS: The query returned no rows.",
                )

            column_names = (
                [col[0] for col in cursor.description] if cursor.description else []
            )

            results_with_columns = [
                dict(zip(column_names, map(str, row))) for row in results
            ]

            file_path = f"temp/{uuid.uuid4()}.json"

            with open(file_path, "w") as f:
                json.dump(results_with_columns, f)

            return QueryResult(file_path=file_path, results=results_with_columns)

        else:
            raise PermissionError("Only read methods are allowed.")

    except PermissionError as pe:
        logger.error(f"Permission error: {pe}")
        return QueryResult(file_path="", results=[], error=str(pe))

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return QueryResult(file_path="", results=[], error=str(e))
    finally:
        # Close the database connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


@tool
async def execute_query(
    runtime: ToolRuntime[Context],
    query: str,
    enforce_user_permissions: bool = False,
) -> list[tuple[Any, ...]] | str:
    """
    Executes a SQL query on the PostgreSQL database and returns the results.
    Only SELECT queries are allowed (SELECT, WITH).
    User credentials are automatically injected into the query on runtime,
    so you can write the query as if you are the user with no need to worry about the user credentials.
    The query will be validated and modified to include necessary filters based on the user's permissions before execution.

    Args:
        query (str): The SQL query to execute.
        enforce_user_permissions (bool): Whether to enforce user permissions.
        If True, the query will be validated and modified to include necessary filters based on the user's permissions.
        This is useful for user filtering on GLOBAL tables as they don't enforce user filtering by default.
        For USER-scoped tables, the system automatically applies user filtering at runtime, so this flag has no effect on USER-scoped tables.
        Default is False.
        list[tuple[Any, ...]] | str: The results of the query as a list of tuples, or an error message.
    Raises:
        PermissionError: If the query is not a read method.
    """
    conn = None
    cursor = None
    try:
        user_id = runtime.context.user_id
        masked_query = runtime.context.masked_query
        is_matched_sql = runtime.context.is_matched_sql

        validated_query = (
            get_container()
            .validator()
            .validate_query(
                query,
                current_user_id=user_id,
                enforce_user_filter_on_global_tables=enforce_user_permissions,
            )
        )

        logger.debug(
            f"Executing query: {validated_query} with user_id: {user_id} and enforce_user_permissions: {enforce_user_permissions}"
        )

        conn_string = (
            f"postgresql://{settings.DATABASE_MANIFEST.connection_params.username}:"
            f"{settings.DATABASE_MANIFEST.connection_params.password}@"
            f"{settings.DATABASE_MANIFEST.connection_params.host}:"
            f"{settings.DATABASE_MANIFEST.connection_params.port}/"
            f"{settings.DATABASE_MANIFEST.connection_params.database}"
        )

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # Fetch results if it's a SELECT query
        if (
            validated_query.strip()
            .lower()
            .startswith(("select", "with", "show", "describe", "explain"))
        ):
            # Execute the VALIDATED query, never the raw LLM-generated query.
            cursor.execute(sql.SQL(validated_query))
            results = cursor.fetchall()

            if masked_query and not is_matched_sql:
                asyncio.create_task(
                    safe_set_sql(masked_query, mask_ner_and_numbers(validated_query))
                )

            # Return an explicit sentinel so the LLM never hallucinates when
            # the query legitimately returns no rows.
            if not results:
                return "NO_RESULTS: The query returned no rows."

            return results
        else:
            raise PermissionError("Only read methods are allowed.")

    except PermissionError as pe:
        logger.error(f"Permission error: {pe}")
        return str(pe)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return str(e)
    finally:
        # Close the database connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
