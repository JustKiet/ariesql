from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel


class Scope(Enum):
    GLOBAL = "global"
    USER = "user"


class TablePolicy(BaseModel):
    scope: Scope
    allowed_columns: set[str]
    user_key: str | None = None


class ConnectionParams(BaseModel):
    host: str
    port: int
    username: str
    password: str
    database: str


class DatabaseManifest(BaseModel):
    database: str
    dialect: str
    connection_params: ConnectionParams
    default_schema: str | None = None
    policy: dict[str, TablePolicy]
    blocked_functions: set[str]


@dataclass
class Context:
    user_id: int
    masked_query: str | None = None
    is_matched_sql: bool = False
