from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import psycopg2
import pymssql

# -- Data classes ----------------------------------------------------------


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


# -- Dialect interface -----------------------------------------------------


class DatabaseDialect(ABC):
    """
    Abstract interface that encapsulates every database-specific operation
    needed by :class:`DatabaseContextLoader`.

    Implementations provide a connection context-manager and a set of
    metadata queries expressed in the native SQL dialect.
    """

    @abstractmethod
    @contextmanager
    def connect(self) -> Iterator[Any]:
        """Yield a DB-API 2.0 *cursor* and clean up when done."""
        ...

    @abstractmethod
    def get_database_info(self, cursor: Any) -> Dict[str, str]:
        """Return ``{"database": ..., "schema": ..., "version": ...}``."""
        ...

    @abstractmethod
    def get_tables(self, cursor: Any, schema_name: str) -> List[str]:
        """Return a sorted list of base-table names in *schema_name*."""
        ...

    @abstractmethod
    def get_columns(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> List[Dict[str, Any]]:
        """Return column metadata dicts for *table_name*."""
        ...

    @abstractmethod
    def get_primary_keys(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> List[str]:
        """Return an ordered list of primary-key column names."""
        ...

    @abstractmethod
    def get_indexes(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> List[Dict[str, str]]:
        """Return index metadata dicts for *table_name*."""
        ...

    @abstractmethod
    def get_table_statistics(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> Dict[str, Any]:
        """Return ``{"row_count": int, "size": str}``."""
        ...

    @abstractmethod
    def get_column_insights(
        self,
        cursor: Any,
        schema_name: str,
        table_name: str,
        columns: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """Return privacy-preserving statistical insights per column."""
        ...

    @abstractmethod
    def get_relationships(self, cursor: Any, schema_name: str) -> List[Dict[str, str]]:
        """Return foreign-key relationship dicts for the whole schema."""
        ...


# -- PostgreSQL dialect ----------------------------------------------------


class PostgresDialect(DatabaseDialect):
    """PostgreSQL implementation using *psycopg2*."""

    def __init__(self, conn_string: str) -> None:
        self._conn_string = conn_string

    @contextmanager
    def connect(self) -> Iterator[Any]:
        conn = psycopg2.connect(self._conn_string)
        try:
            with conn.cursor() as cursor:
                yield cursor
        finally:
            conn.close()

    def get_database_info(self, cursor: Any) -> Dict[str, str]:
        cursor.execute("SELECT current_database(), current_schema(), version();")
        db_name, schema_name, version = cursor.fetchone()
        return {"database": db_name, "schema": schema_name, "version": version}

    def get_tables(self, cursor: Any, schema_name: str) -> List[str]:
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

    def get_columns(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> List[Dict[str, Any]]:
        cursor.execute(
            """
            SELECT column_name, data_type, character_maximum_length,
                   is_nullable, column_default, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (schema_name, table_name),
        )
        columns: List[Dict[str, Any]] = []
        for (
            col_name,
            data_type,
            max_length,
            is_nullable,
            col_default,
            position,
        ) in cursor.fetchall():
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

    def get_primary_keys(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> List[str]:
        cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema  = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name   = %s
            ORDER BY kcu.ordinal_position;
            """,
            (schema_name, table_name),
        )
        return [row[0] for row in cursor.fetchall()]

    def get_indexes(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> List[Dict[str, str]]:
        cursor.execute(
            """
            SELECT i.indexname, i.indexdef
            FROM pg_indexes i
            WHERE i.schemaname = %s AND i.tablename = %s;
            """,
            (schema_name, table_name),
        )
        return [
            {"name": idx_name, "definition": idx_def}
            for idx_name, idx_def in cursor.fetchall()
        ]

    def get_table_statistics(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> Dict[str, Any]:
        cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
        row_count = cursor.fetchone()[0]

        cursor.execute(
            "SELECT pg_size_pretty(pg_total_relation_size(%s));",
            (f"{schema_name}.{table_name}",),
        )
        table_size = cursor.fetchone()[0]
        return {"row_count": row_count, "size": table_size}

    def get_column_insights(
        self,
        cursor: Any,
        schema_name: str,
        table_name: str,
        columns: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        insights: Dict[str, Dict[str, Any]] = {}

        for col in columns:
            col_name = col["name"]
            col_type = col["type"].lower()
            insight: Dict[str, Any] = {
                "data_type": col["type"],
                "nullable": col["nullable"],
            }

            # Null counts
            cursor.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE {col_name} IS NULL)     AS null_count,
                    COUNT(*) FILTER (WHERE {col_name} IS NOT NULL) AS non_null_count
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

            is_datetime = any(dt in col_type for dt in ["timestamp", "date", "time"])

            # Unique count (skip datetime for performance)
            if not is_datetime:
                try:
                    cursor.execute(f"""
                        SELECT COUNT(DISTINCT {col_name})
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
                    pass

            # Numeric stats
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
                        SELECT MIN({col_name})::text, MAX({col_name})::text,
                               AVG({col_name})::numeric, STDDEV({col_name})::numeric
                        FROM {schema_name}.{table_name}
                        WHERE {col_name} IS NOT NULL;
                    """)
                    _, _, avg_val, stddev_val = cursor.fetchone()
                    insight["numeric_stats"] = {
                        "has_range": True,
                        "average": float(avg_val) if avg_val else None,
                        "std_dev": float(stddev_val) if stddev_val else None,
                    }
                except Exception:
                    pass

            # Text stats
            elif any(t in col_type for t in ["char", "text", "varchar"]):
                try:
                    cursor.execute(f"""
                        SELECT MIN(LENGTH({col_name})), MAX(LENGTH({col_name})),
                               AVG(LENGTH({col_name}))::numeric
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

            # Boolean stats
            elif "boolean" in col_type or "bool" in col_type:
                try:
                    cursor.execute(f"""
                        SELECT
                            COUNT(*) FILTER (WHERE {col_name} = true)  AS true_count,
                            COUNT(*) FILTER (WHERE {col_name} = false) AS false_count
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

            # Datetime stats
            elif is_datetime:
                try:
                    cursor.execute(f"""
                        SELECT MIN({col_name})::text, MAX({col_name})::text
                        FROM {schema_name}.{table_name}
                        WHERE {col_name} IS NOT NULL;
                    """)
                    insight["datetime_stats"] = {
                        "has_range": True,
                        "range_description": "temporal data present",
                    }
                except Exception:
                    pass

            insights[col_name] = insight

        return insights

    def get_relationships(self, cursor: Any, schema_name: str) -> List[Dict[str, str]]:
        cursor.execute(
            """
            SELECT tc.table_name   AS from_table,
                   kcu.column_name AS from_column,
                   ccu.table_name  AS to_table,
                   ccu.column_name AS to_column,
                   tc.constraint_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema   = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
               AND ccu.table_schema    = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema    = %s
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


# -- MSSQL dialect ---------------------------------------------------------


class MSSQLDialect(DatabaseDialect):
    """SQL Server implementation using *pymssql*."""

    def __init__(
        self,
        *,
        server: str,
        port: int,
        user: str,
        password: str,
        database: str,
        schema: str = "dbo",
    ) -> None:
        self._server = server
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._schema = schema

    @contextmanager
    def connect(self) -> Iterator[Any]:
        conn = pymssql.connect(
            server=self._server,
            port=str(self._port),
            user=self._user,
            password=self._password,
            database=self._database,
        )
        try:
            cursor = conn.cursor()
            yield cursor
        finally:
            conn.close()

    def get_database_info(self, cursor: Any) -> Dict[str, str]:
        cursor.execute("""
            SELECT DB_NAME(),
                   @@VERSION;
        """)
        db_name, version = cursor.fetchone()
        return {"database": db_name, "schema": self._schema, "version": version}

    def get_tables(self, cursor: Any, schema_name: str) -> List[str]:
        cursor.execute(
            """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_TYPE   = 'BASE TABLE'
            ORDER BY TABLE_NAME;
            """,
            (schema_name,),
        )
        return [row[0] for row in cursor.fetchall()]

    def get_columns(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> List[Dict[str, Any]]:
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION;
            """,
            (schema_name, table_name),
        )
        columns: list[dict[str, Any]] = []
        for (
            col_name,
            data_type,
            max_length,
            is_nullable,
            col_default,
            position,
        ) in cursor.fetchall():
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

    def get_primary_keys(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> List[str]:
        cursor.execute(
            """
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
               AND tc.TABLE_SCHEMA   = kcu.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA    = %s
              AND tc.TABLE_NAME      = %s
            ORDER BY kcu.ORDINAL_POSITION;
            """,
            (schema_name, table_name),
        )
        return [row[0] for row in cursor.fetchall()]

    def get_indexes(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> List[Dict[str, str]]:
        cursor.execute(
            """
            SELECT i.name                        AS index_name,
                   STRING_AGG(c.name, ', ')
                       WITHIN GROUP (ORDER BY ic.key_ordinal)
                                                 AS columns,
                   CASE WHEN i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END
                       + i.type_desc             AS index_type
            FROM sys.indexes i
            JOIN sys.index_columns ic
                ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c
                ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = OBJECT_ID(%s)
              AND i.name IS NOT NULL
            GROUP BY i.name, i.is_unique, i.type_desc;
            """,
            (f"{schema_name}.{table_name}",),
        )
        return [
            {"name": name, "definition": f"{idx_type} ON ({cols})"}
            for name, cols, idx_type in cursor.fetchall()
        ]

    def get_table_statistics(
        self, cursor: Any, schema_name: str, table_name: str
    ) -> Dict[str, Any]:
        cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
        row_count = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT SUM(a.total_pages) * 8 AS total_kb
            FROM sys.tables t
            JOIN sys.indexes i      ON t.object_id = i.object_id
            JOIN sys.partitions p   ON i.object_id = p.object_id
                                   AND i.index_id  = p.index_id
            JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE t.object_id = OBJECT_ID(%s);
            """,
            (f"{schema_name}.{table_name}",),
        )
        total_kb = cursor.fetchone()[0] or 0
        if total_kb >= 1024:
            table_size = f"{total_kb // 1024} MB"
        else:
            table_size = f"{total_kb} kB"

        return {"row_count": row_count, "size": table_size}

    def get_column_insights(
        self,
        cursor: Any,
        schema_name: str,
        table_name: str,
        columns: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        fqn = f"{schema_name}.{table_name}"
        insights: Dict[str, Dict[str, Any]] = {}

        for col in columns:
            col_name = col["name"]
            col_type = col["type"].lower()
            insight: Dict[str, Any] = {
                "data_type": col["type"],
                "nullable": col["nullable"],
            }

            # Null counts (T-SQL: SUM(CASE ...) instead of FILTER)
            cursor.execute(f"""
                SELECT
                    SUM(CASE WHEN [{col_name}] IS NULL     THEN 1 ELSE 0 END) AS null_count,
                    SUM(CASE WHEN [{col_name}] IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count
                FROM {fqn};
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

            is_datetime = any(
                dt in col_type for dt in ["datetime", "date", "time", "smalldatetime"]
            )

            # Unique count
            if not is_datetime:
                try:
                    cursor.execute(f"""
                        SELECT COUNT(DISTINCT [{col_name}])
                        FROM {fqn}
                        WHERE [{col_name}] IS NOT NULL;
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
                    pass

            # Numeric stats
            if any(
                t in col_type
                for t in [
                    "int",
                    "numeric",
                    "decimal",
                    "real",
                    "float",
                    "smallint",
                    "bigint",
                    "money",
                    "tinyint",
                ]
            ):
                try:
                    cursor.execute(f"""
                        SELECT
                            CAST(AVG(CAST([{col_name}] AS FLOAT)) AS FLOAT),
                            CAST(STDEV(CAST([{col_name}] AS FLOAT)) AS FLOAT)
                        FROM {fqn}
                        WHERE [{col_name}] IS NOT NULL;
                    """)
                    avg_val, stddev_val = cursor.fetchone()
                    insight["numeric_stats"] = {
                        "has_range": True,
                        "average": round(float(avg_val), 2) if avg_val else None,
                        "std_dev": round(float(stddev_val), 2) if stddev_val else None,
                    }
                except Exception:
                    pass

            # Text stats
            elif any(
                t in col_type
                for t in ["char", "text", "varchar", "nchar", "nvarchar", "ntext"]
            ):
                try:
                    cursor.execute(f"""
                        SELECT MIN(LEN([{col_name}])), MAX(LEN([{col_name}])),
                               AVG(CAST(LEN([{col_name}]) AS FLOAT))
                        FROM {fqn}
                        WHERE [{col_name}] IS NOT NULL AND [{col_name}] != '';
                    """)
                    min_len, max_len, avg_len = cursor.fetchone()
                    insight["text_stats"] = {
                        "min_length": min_len,
                        "max_length": max_len,
                        "avg_length": round(float(avg_len), 2) if avg_len else None,
                    }
                except Exception:
                    pass

            # Boolean (BIT) stats
            elif "bit" in col_type:
                try:
                    cursor.execute(f"""
                        SELECT
                            SUM(CASE WHEN [{col_name}] = 1 THEN 1 ELSE 0 END),
                            SUM(CASE WHEN [{col_name}] = 0 THEN 1 ELSE 0 END)
                        FROM {fqn}
                        WHERE [{col_name}] IS NOT NULL;
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

            # Datetime stats
            elif is_datetime:
                try:
                    cursor.execute(f"""
                        SELECT MIN([{col_name}]), MAX([{col_name}])
                        FROM {fqn}
                        WHERE [{col_name}] IS NOT NULL;
                    """)
                    insight["datetime_stats"] = {
                        "has_range": True,
                        "range_description": "temporal data present",
                    }
                except Exception:
                    pass

            insights[col_name] = insight

        return insights

    def get_relationships(self, cursor: Any, schema_name: str) -> List[Dict[str, str]]:
        cursor.execute(
            """
            SELECT
                tp.name  AS from_table,
                cp.name  AS from_column,
                tr.name  AS to_table,
                cr.name  AS to_column,
                fk.name  AS constraint_name
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc
                ON fk.object_id = fkc.constraint_object_id
            JOIN sys.tables tp  ON fkc.parent_object_id     = tp.object_id
            JOIN sys.columns cp ON fkc.parent_object_id     = cp.object_id
                               AND fkc.parent_column_id     = cp.column_id
            JOIN sys.tables tr  ON fkc.referenced_object_id = tr.object_id
            JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id
                               AND fkc.referenced_column_id = cr.column_id
            WHERE SCHEMA_NAME(tp.schema_id) = %s
            ORDER BY tp.name, cp.name;
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


# -- Context loader (dialect-agnostic) ------------------------------------


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

    def __init__(self, dialect: DatabaseDialect, auto_load: bool = True):
        """
        Initialize the context loader with a database dialect backend.

        Args:
            dialect: A :class:`DatabaseDialect` implementation that provides
                     the database connection and metadata queries.
            auto_load: Whether to automatically load context on initialization
                       (default: True)
        """
        self._dialect = dialect
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
        if use_cache and self._is_cache_valid(cache_ttl_seconds) and self.context:
            return self.context

        context: Dict[str, Any] = {
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
            with self._dialect.connect() as cursor:
                context["database_info"] = self._dialect.get_database_info(cursor)
                schema_name = context["database_info"]["schema"]

                tables = self._dialect.get_tables(cursor, schema_name)

                for table_name in tables:
                    context["tables"][table_name] = {
                        "type": "BASE TABLE",
                        "columns": self._dialect.get_columns(
                            cursor, schema_name, table_name
                        ),
                        "primary_keys": self._dialect.get_primary_keys(
                            cursor, schema_name, table_name
                        ),
                        "indexes": self._dialect.get_indexes(
                            cursor, schema_name, table_name
                        ),
                    }

                    context["statistics"][table_name] = (
                        self._dialect.get_table_statistics(
                            cursor, schema_name, table_name
                        )
                    )

                    context["column_insights"][table_name] = (
                        self._dialect.get_column_insights(
                            cursor,
                            schema_name,
                            table_name,
                            context["tables"][table_name]["columns"],
                        )
                    )

                context["relationships"] = self._dialect.get_relationships(
                    cursor, schema_name
                )

            self.context = context
            self._cache_timestamp = datetime.now()
            return context

        except Exception as e:
            raise RuntimeError(f"Database error while fetching context: {e}")

    def _is_cache_valid(self, ttl_seconds: int) -> bool:
        if not self.context or not self._cache_timestamp:
            return False
        age = (datetime.now() - self._cache_timestamp).total_seconds()
        return age < ttl_seconds

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

        lines: list[str] = []
        lines.append("# Database Schema Context")
        lines.append("")

        db_info = context["database_info"]
        lines.append(f"Database: {db_info['database']}")
        lines.append(f"Schema: {db_info['schema']}")
        lines.append("")

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

                if col_name in column_insights:
                    ci = column_insights[col_name]
                    parts: list[str] = []

                    if ci.get("null_percentage", 0) > 0:
                        parts.append(f"{ci['null_percentage']}% null")

                    if "unique_count" in ci:
                        parts.append(
                            f"{ci['unique_count']} unique "
                            f"({ci.get('cardinality', 'unknown')} cardinality)"
                        )

                    if "numeric_stats" in ci:
                        ns = ci["numeric_stats"]
                        if ns.get("average") is not None:
                            parts.append(f"avg={ns['average']:.2f}")

                    if "text_stats" in ci:
                        ts = ci["text_stats"]
                        parts.append(f"length: {ts['min_length']}-{ts['max_length']}")

                    if "boolean_stats" in ci:
                        bs = ci["boolean_stats"]
                        parts.append(f"true: {bs['true_percentage']}%")

                    if parts:
                        col_line += f" [{', '.join(parts)}]"

                lines.append(col_line)
            lines.append("")

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
