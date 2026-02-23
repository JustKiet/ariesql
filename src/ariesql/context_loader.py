"""
SQL Agent Context Loader

This module provides utilities to load database context for LLM-powered SQL agents.
It extracts schema information, relationships, sample data, and statistics that
help LLMs understand the database structure and generate accurate SQL queries.
"""

import psycopg2
from typing import Dict, List, Any, Optional
import json
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class ColumnInfo:
    """Information about a database column"""

    name: str
    type: str
    nullable: bool
    max_length: Optional[int] = None
    default: Optional[str] = None
    position: int = 0


@dataclass
class TableInfo:
    """Information about a database table"""

    name: str
    type: str
    columns: List[ColumnInfo]
    primary_keys: List[str]
    indexes: List[Dict[str, str]]
    row_count: int = 0


@dataclass
class Relationship:
    """Foreign key relationship between tables"""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    constraint_name: str


class DatabaseContextLoader:
    """
    Loads comprehensive database context for LLM SQL Agents.

    Provides methods to extract and format database metadata including:
    - Schema information (tables, columns, types)
    - Relationships (foreign keys, primary keys)
    - Column insights (unique values, data patterns, statistics)
    - NO raw data (privacy-preserving)

    Auto-loads context on initialization.
    """

    def __init__(self, conn_string: str, auto_load: bool = True):
        """
        Initialize the context loader with a database connection string.
        Auto-loads database context on initialization.

        Args:
            conn_string: PostgreSQL connection string
                        (e.g., "postgresql://user:pass@host:port/dbname")
            auto_load: Whether to automatically load context on initialization (default: True)
        """
        self.conn_string = conn_string
        self.context: Optional[Dict[str, Any]] = None
        self._cache_timestamp: Optional[datetime] = None

        if auto_load:
            self.context = self.fetch_full_context()

    def fetch_full_context(
        self,
        use_cache: bool = True,
        cache_ttl_seconds: int = 300,
    ) -> Dict[str, Any]:
        """
        Fetch comprehensive database context.

        NO raw data is included - only statistical insights about columns.
        Privacy-preserving: shows patterns, not actual values.

        Args:
            use_cache: Whether to use cached context if available
            cache_ttl_seconds: Cache time-to-live in seconds

        Returns:
            Dictionary containing complete database context
        """
        # Check cache
        if use_cache and self._is_cache_valid(cache_ttl_seconds) and self.context:
            return self.context

        context = {
            "database_info": {},
            "tables": {},
            "relationships": [],
            "column_insights": {},
            "statistics": {},
            "metadata": {
                "fetched_at": datetime.now().isoformat(),
                "privacy_mode": True,
            },
        }

        try:
            with psycopg2.connect(self.conn_string) as conn:
                with conn.cursor() as cursor:
                    # 1. Database information
                    context["database_info"] = self._get_database_info(cursor)
                    schema_name = context["database_info"]["schema"]

                    # 2. Tables and columns
                    tables = self._get_tables(cursor, schema_name)

                    for table_name in tables:
                        context["tables"][table_name] = {
                            "type": "BASE TABLE",
                            "columns": self._get_columns(
                                cursor, schema_name, table_name
                            ),
                            "primary_keys": self._get_primary_keys(
                                cursor, schema_name, table_name
                            ),
                            "indexes": self._get_indexes(
                                cursor, schema_name, table_name
                            ),
                        }

                        # Statistics
                        context["statistics"][table_name] = self._get_table_statistics(
                            cursor, schema_name, table_name
                        )

                        # Column insights (NO raw data)
                        context["column_insights"][table_name] = (
                            self._get_column_insights(
                                cursor,
                                schema_name,
                                table_name,
                                context["tables"][table_name]["columns"],
                            )
                        )

                    # 3. Relationships
                    context["relationships"] = self._get_relationships(
                        cursor, schema_name
                    )

            # Cache the context
            self.context = context
            self._cache_timestamp = datetime.now()

            return context

        except psycopg2.Error as e:
            raise RuntimeError(f"Database error while fetching context: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while fetching context: {e}")

    def _is_cache_valid(self, ttl_seconds: int) -> bool:
        """Check if cached context is still valid"""
        if not self.context or not self._cache_timestamp:
            return False

        age = (datetime.now() - self._cache_timestamp).total_seconds()
        return age < ttl_seconds

    def _get_database_info(self, cursor) -> Dict[str, str]:
        """Get basic database information"""
        cursor.execute("""
            SELECT current_database(), current_schema(), version();
        """)
        db_name, schema_name, version = cursor.fetchone()
        return {"database": db_name, "schema": schema_name, "version": version}

    def _get_tables(self, cursor, schema_name: str) -> List[str]:
        """Get list of all tables in the schema"""
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE' 
            ORDER BY table_name;
        """,
            (schema_name,),
        )
        return [row[0] for row in cursor.fetchall()]

    def _get_columns(
        self, cursor, schema_name: str, table_name: str
    ) -> List[Dict[str, Any]]:
        """Get column information for a table"""
        cursor.execute(
            """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """,
            (schema_name, table_name),
        )

        columns = []
        for col in cursor.fetchall():
            col_name, data_type, max_length, is_nullable, col_default, position = col
            columns.append(
                {
                    "name": col_name,
                    "type": data_type,
                    "max_length": max_length,
                    "nullable": is_nullable == "YES",
                    "default": str(col_default) if col_default else None,
                    "position": position,
                }
            )
        return columns

    def _get_primary_keys(self, cursor, schema_name: str, table_name: str) -> List[str]:
        """Get primary key columns for a table"""
        cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = %s
                AND tc.table_name = %s
            ORDER BY kcu.ordinal_position;
        """,
            (schema_name, table_name),
        )
        return [row[0] for row in cursor.fetchall()]

    def _get_indexes(
        self, cursor, schema_name: str, table_name: str
    ) -> List[Dict[str, str]]:
        """Get indexes for a table"""
        cursor.execute(
            """
            SELECT 
                i.indexname,
                i.indexdef
            FROM pg_indexes i
            WHERE i.schemaname = %s AND i.tablename = %s;
        """,
            (schema_name, table_name),
        )

        return [
            {"name": idx_name, "definition": idx_def}
            for idx_name, idx_def in cursor.fetchall()
        ]

    def _get_table_statistics(
        self, cursor, schema_name: str, table_name: str
    ) -> Dict[str, Any]:
        """Get statistics for a table"""
        cursor.execute(f"""
            SELECT COUNT(*) FROM {schema_name}.{table_name};
        """)
        row_count = cursor.fetchone()[0]

        # Get table size
        cursor.execute(
            """
            SELECT pg_size_pretty(pg_total_relation_size(%s));
        """,
            (f"{schema_name}.{table_name}",),
        )
        table_size = cursor.fetchone()[0]

        return {"row_count": row_count, "size": table_size}

    def _get_column_insights(
        self, cursor, schema_name: str, table_name: str, columns: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get statistical insights about each column WITHOUT revealing actual data.
        Privacy-preserving: no sample rows, only patterns and statistics.
        """
        insights = {}

        for col in columns:
            col_name = col["name"]
            col_type = col["type"].lower()

            insight = {
                "data_type": col["type"],
                "nullable": col["nullable"],
            }

            # Count null values
            cursor.execute(f"""
                SELECT 
                    COUNT(*) FILTER (WHERE {col_name} IS NULL) as null_count,
                    COUNT(*) FILTER (WHERE {col_name} IS NOT NULL) as non_null_count
                FROM {schema_name}.{table_name};
            """)
            null_count, non_null_count = cursor.fetchone()
            total = null_count + non_null_count

            insight["null_count"] = null_count
            insight["null_percentage"] = round(
                (null_count / total * 100) if total > 0 else 0, 2
            )

            if non_null_count == 0:
                insight["status"] = "all_null"
                insights[col_name] = insight
                continue

            # Get unique count (skip for datetime types for performance)
            is_datetime = any(dt in col_type for dt in ["timestamp", "date", "time"])

            if not is_datetime:
                try:
                    cursor.execute(f"""
                        SELECT COUNT(DISTINCT {col_name}) as unique_count
                        FROM {schema_name}.{table_name}
                        WHERE {col_name} IS NOT NULL;
                    """)
                    unique_count = cursor.fetchone()[0]
                    insight["unique_count"] = unique_count
                    insight["cardinality"] = (
                        "unique"
                        if unique_count == non_null_count
                        else "high"
                        if unique_count > non_null_count * 0.9
                        else "medium"
                        if unique_count > non_null_count * 0.5
                        else "low"
                    )
                except Exception:
                    pass  # Skip if query fails

            # Numeric types: get range info (no actual values)
            if any(
                t in col_type
                for t in [
                    "integer",
                    "numeric",
                    "decimal",
                    "real",
                    "double",
                    "smallint",
                    "bigint",
                    "money",
                ]
            ):
                try:
                    cursor.execute(f"""
                        SELECT 
                            MIN({col_name})::text,
                            MAX({col_name})::text,
                            AVG({col_name})::numeric,
                            STDDEV({col_name})::numeric
                        FROM {schema_name}.{table_name}
                        WHERE {col_name} IS NOT NULL;
                    """)
                    min_val, max_val, avg_val, stddev_val = cursor.fetchone()

                    # Only show range breadth, not actual values for privacy
                    insight["numeric_stats"] = {
                        "has_range": True,
                        "average": float(avg_val) if avg_val else None,
                        "std_dev": float(stddev_val) if stddev_val else None,
                    }
                except Exception:
                    pass

            # Text types: get length statistics
            elif any(t in col_type for t in ["char", "text", "varchar"]):
                try:
                    cursor.execute(f"""
                        SELECT 
                            MIN(LENGTH({col_name})) as min_length,
                            MAX(LENGTH({col_name})) as max_length,
                            AVG(LENGTH({col_name}))::numeric as avg_length
                        FROM {schema_name}.{table_name}
                        WHERE {col_name} IS NOT NULL AND {col_name} != '';
                    """)
                    min_len, max_len, avg_len = cursor.fetchone()

                    insight["text_stats"] = {
                        "min_length": min_len,
                        "max_length": max_len,
                        "avg_length": round(float(avg_len), 2) if avg_len else None,
                    }
                except Exception:
                    pass

            # Boolean types: get distribution
            elif "boolean" in col_type or "bool" in col_type:
                try:
                    cursor.execute(f"""
                        SELECT 
                            COUNT(*) FILTER (WHERE {col_name} = true) as true_count,
                            COUNT(*) FILTER (WHERE {col_name} = false) as false_count
                        FROM {schema_name}.{table_name}
                        WHERE {col_name} IS NOT NULL;
                    """)
                    true_count, false_count = cursor.fetchone()
                    total_bool = true_count + false_count

                    insight["boolean_stats"] = {
                        "true_percentage": round(
                            (true_count / total_bool * 100) if total_bool > 0 else 0, 2
                        ),
                        "false_percentage": round(
                            (false_count / total_bool * 100) if total_bool > 0 else 0, 2
                        ),
                    }
                except Exception:
                    pass

            # Date/datetime types: get range info
            elif is_datetime:
                try:
                    cursor.execute(f"""
                        SELECT 
                            MIN({col_name})::text,
                            MAX({col_name})::text
                        FROM {schema_name}.{table_name}
                        WHERE {col_name} IS NOT NULL;
                    """)
                    min_date, max_date = cursor.fetchone()

                    # Only indicate there's a date range, not the actual dates
                    insight["datetime_stats"] = {
                        "has_range": True,
                        "range_description": "temporal data present",
                    }
                except Exception:
                    pass

            insights[col_name] = insight

        return insights

    def _get_relationships(self, cursor, schema_name: str) -> List[Dict[str, str]]:
        """Get foreign key relationships"""
        cursor.execute(
            """
            SELECT
                tc.table_name AS from_table,
                kcu.column_name AS from_column,
                ccu.table_name AS to_table,
                ccu.column_name AS to_column,
                tc.constraint_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = %s
            ORDER BY tc.table_name, kcu.column_name;
        """,
            (schema_name,),
        )

        return [
            {
                "from_table": from_table,
                "from_column": from_column,
                "to_table": to_table,
                "to_column": to_column,
                "constraint_name": constraint_name,
            }
            for from_table, from_column, to_table, to_column, constraint_name in cursor.fetchall()
        ]

    def format_for_llm_prompt(self, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Format the database context into a text prompt suitable for LLMs.

        Args:
            context: Database context dictionary (uses cached if not provided)

        Returns:
            Formatted text description of the database
        """
        if context is None:
            context = self.context if self.context else self.fetch_full_context()

        lines = []
        lines.append("# Database Schema Context")
        lines.append("")

        # Database info
        db_info = context["database_info"]
        lines.append(f"Database: {db_info['database']}")
        lines.append(f"Schema: {db_info['schema']}")
        lines.append("")

        # Tables
        lines.append("## Tables")
        lines.append("")

        for table_name, table_info in context["tables"].items():
            stats = context["statistics"].get(table_name, {})
            lines.append(f"### {table_name}")
            lines.append(f"- Row count: {stats.get('row_count', 'N/A')}")
            lines.append(f"- Table size: {stats.get('size', 'N/A')}")
            lines.append(
                f"- Primary keys: {', '.join(table_info['primary_keys']) or 'None'}"
            )
            lines.append("")

            lines.append("Columns:")
            column_insights = context["column_insights"].get(table_name, {})

            for col in table_info["columns"]:
                col_name = col["name"]
                nullable = "NULL" if col["nullable"] else "NOT NULL"
                type_info = col["type"]
                if col["max_length"]:
                    type_info += f"({col['max_length']})"

                col_line = f"  - {col_name}: {type_info} {nullable}"

                # Add insights
                if col_name in column_insights:
                    insights = column_insights[col_name]
                    insight_parts = []

                    if insights.get("null_percentage", 0) > 0:
                        insight_parts.append(f"{insights['null_percentage']}% null")

                    if "unique_count" in insights:
                        insight_parts.append(
                            f"{insights['unique_count']} unique ({insights.get('cardinality', 'unknown')} cardinality)"
                        )

                    if "numeric_stats" in insights:
                        ns = insights["numeric_stats"]
                        if ns.get("average") is not None:
                            insight_parts.append(f"avg={ns['average']:.2f}")

                    if "text_stats" in insights:
                        ts = insights["text_stats"]
                        insight_parts.append(
                            f"length: {ts['min_length']}-{ts['max_length']}"
                        )

                    if "boolean_stats" in insights:
                        bs = insights["boolean_stats"]
                        insight_parts.append(f"true: {bs['true_percentage']}%")

                    if insight_parts:
                        col_line += f" [{', '.join(insight_parts)}]"

                lines.append(col_line)
            lines.append("")

        # Relationships
        if context["relationships"]:
            lines.append("## Relationships")
            lines.append("")
            for rel in context["relationships"]:
                lines.append(
                    f"- {rel['from_table']}.{rel['from_column']} -> "
                    f"{rel['to_table']}.{rel['to_column']}"
                )
            lines.append("")

        return "\n".join(lines)

    def get_table_context(self, table_name: str) -> Dict[str, Any]:
        """
        Get context for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary containing table-specific context
        """
        if not self.context:
            self.context = self.fetch_full_context()

        if table_name not in self.context["tables"]:
            raise ValueError(f"Table '{table_name}' not found in database")

        return {
            "table": table_name,
            "info": self.context["tables"][table_name],
            "statistics": self.context["statistics"].get(table_name, {}),
            "column_insights": self.context["column_insights"].get(table_name, {}),
            "relationships": [
                rel
                for rel in self.context["relationships"]
                if rel["from_table"] == table_name or rel["to_table"] == table_name
            ],
        }

    def clear_cache(self):
        """Clear the cached context"""
        self.context = None
        self._cache_timestamp = None


# def main():
#     """Example usage of the DatabaseContextLoader"""
#     # Example connection string
#     conn_string = "postgresql://postgres:postgres@localhost:5432/employees"

#     # Initialize loader (auto-loads context)
#     print("Initializing DatabaseContextLoader (auto-loading context)...")
#     loader = DatabaseContextLoader(conn_string)

#     # Access loaded context
#     context = loader.context

#     # Display summary
#     print(f"\nDatabase: {context['database_info']['database']}")
#     print(f"Schema: {context['database_info']['schema']}")
#     print(f"\nTables found: {len(context['tables'])}")
#     for table_name in context["tables"]:
#         stats = context["statistics"][table_name]
#         print(
#             f"  - {table_name}: {stats['row_count']} rows ({stats.get('size', 'N/A')})"
#         )

#     print(f"\nRelationships found: {len(context['relationships'])}")

#     # Format for LLM
#     print("\n" + "=" * 80)
#     print("LLM-Formatted Context (Privacy-Preserving):")
#     print("=" * 80)
#     llm_prompt = loader.format_for_llm_prompt()
#     print(llm_prompt)

#     # Save to file
#     with open("database_context.json", "w") as f:
#         json.dump(context, f, indent=2)

#     with open("database_context.md", "w") as f:
#         f.write(llm_prompt)
#     print("\nContext saved to database_context.json")
#     print("\nNote: No actual row data is included - only statistical insights!")


# if __name__ == "__main__":
#     main()
