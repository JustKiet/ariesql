from openai import OpenAI
from ariesql.context_loader import DatabaseContextLoader
import textwrap
from pydantic import BaseModel
import psycopg2
from psycopg2 import sql
from typing import Any
import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

SQL_AGENT_PROMPT = textwrap.dedent("""
# System Instruction                                   

## Task Description
You are an agent that can execute SQL queries on a database. You will be given a question and you need to generate a SQL query to answer that question. You will then execute the SQL query and return the results.
You should only generate SQL queries that are valid and can be executed on the database. You should not generate any SQL queries that are invalid or cannot be executed on the database.
Your output must be a SQL query that can be executed on the database. You should not include any explanations or comments in your output. You should only output the SQL query.

## Database Schema
{database_schema}
""")


class SQLQueryRequest(BaseModel):
    sql_query: str


CONN_STRING = "postgresql://postgres:postgres@localhost:5432/employees"


def execute_query(query: str) -> list[tuple[Any, ...]]:
    conn = None
    cursor = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(CONN_STRING)
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(sql.SQL(query))

        # Fetch results if it's a SELECT query
        if query.strip().lower().startswith("select"):
            results = cursor.fetchall()
            return results
        else:
            raise PermissionError("Only read methods are allowed.")

    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    finally:
        # Close the database connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def main():
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    context_loader = DatabaseContextLoader(CONN_STRING)
    response = client.chat.completions.parse(
        model="gpt-5",
        messages=[
            {
                "role": "system",
                "content": SQL_AGENT_PROMPT.format(
                    database_schema=context_loader.format_for_llm_prompt()
                ),
            },
            {
                "role": "user",
                "content": "What's the name of the employee with the highest salary?",
            },
        ],
        response_format=SQLQueryRequest,
    )
    sql_query = (
        response.choices[0].message.parsed.sql_query
        if response.choices[0].message.parsed
        else None
    )
    print(response.choices[0].message.parsed)

    if not sql_query:
        print("No SQL query generated.")
        return

    output = execute_query(sql_query)
    print("Query Output:", output)


if __name__ == "__main__":
    main()
