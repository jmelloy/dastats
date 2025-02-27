from dataclasses import dataclass, field, fields
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    get_type_hints,
    get_args,
    get_origin,
    Literal,
)
import uuid
import logging
import sqlite3

from datetime import datetime
import json


logger = logging.getLogger(__name__)


class Select:
    def __init__(self, model: Union["BaseModel", str], columns="*"):
        if isinstance(model, str):
            self.table = model
        else:
            self.table = model.table_name

        self.columns = "*"
        self.joins = []
        self.where_clauses = []
        self.group_by_columns = []
        self.having_clauses = []
        self.order_by_columns = []
        self.from_clauses = []

        if columns != "*":
            if isinstance(columns, str):
                self.columns = columns
            else:
                self.columns = ", ".join(columns)

    def select(self, *columns):
        """Specify columns to select."""
        if columns:
            self.columns = ", ".join(columns)
        return self

    def from_clause(self, model: Union["BaseModel", str]):
        if type(model) is str:
            self.from_clauses.append(model)
        else:
            self.from_clauses.append(model.table_name)
        return self

    def join(
        self, model: Union["BaseModel", str], *, how="inner", on=None, condition=None
    ):
        """Add a JOIN clause."""
        if type(model) is str:
            table = model
        else:
            table = model.table_name

        if on:
            self.joins.append(f"{how} JOIN {table} USING ({on})")
        elif condition:
            self.joins.append(f"{how} JOIN {table} ON {condition}")
        else:
            self.joins.append(f", {table}")
        return self

    def where(self, condition):
        """Add a WHERE condition."""
        self.where_clauses.append(condition)
        return self

    def group_by(self, *columns):
        """Add GROUP BY columns."""
        self.group_by_columns.extend(columns)
        return self

    def having(self, condition):
        """Add a HAVING condition."""
        self.having_clauses.append(condition)
        return self

    def order_by(self, *columns):
        """Add ORDER BY columns."""
        self.order_by_columns.extend(columns)
        return self

    def sql(self, offset=None, limit=None):
        """Generate the final SQL query."""
        query = f"SELECT {self.columns} FROM {self.table}"

        if self.joins:
            query += " " + " ".join(self.joins)

        if self.from_clauses:
            if self.joins:
                query += ","
            query += ",".join(self.from_clauses)

        if self.where_clauses:
            query += " WHERE " + " AND ".join(self.where_clauses)

        if self.group_by_columns:
            query += " GROUP BY " + ", ".join(self.group_by_columns)

        if self.having_clauses:
            query += " HAVING " + " AND ".join(self.having_clauses)

        if self.order_by_columns:
            query += " ORDER BY " + ", ".join(self.order_by_columns)

        if offset:
            query += f" OFFSET {offset}"

        if limit:
            query += f" LIMIT {limit}"

        logger.debug(query)

        return query + ";"

    def __str__(self):
        return self.sql()


def get_sql_type(field_type: Any) -> str:
    # Mapping Python types to SQL types for DuckDB
    type_mapping = {
        int: "BIGINT",
        str: "VARCHAR",
        float: "DOUBLE",
        bool: "BOOLEAN",
        dict: "JSONB",
        list: "JSONB",  # Arrays and lists default to JSON
        BaseModel: "JSONB",  # Nested dataclasses map to STRUCT
        uuid.UUID: "UUID",
        datetime: "TIMESTAMP",
    }

    origin = get_origin(field_type)
    args = get_args(field_type)

    # Handle Optional types by extracting the inner type
    if origin is Union and type(None) in args:
        field_type = next(t for t in args if t != type(None))

    is_list = get_origin(field_type) is list
    # Handle list types
    if is_list:
        field_type = get_args(field_type)[0]

    # Check for nested dataclass
    if hasattr(field_type, "__dataclass_fields__"):
        return "JSON"
        nested_columns = []
        for nested_field_name, nested_field_type in get_type_hints(field_type).items():
            if nested_field_name == "table_name":
                continue
            nested_sql_type = get_sql_type(nested_field_type)
            nested_columns.append(f"{nested_field_name} {nested_sql_type}")
        return f"STRUCT({', '.join(nested_columns)})" + ("[]" if is_list else "")

    # Return the DuckDB equivalent type or 'TEXT' if unknown
    return type_mapping.get(field_type, "TEXT")


def convert_type(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return json.dumps(value.to_dict(), cls=BaseModelEncoder)
    if isinstance(value, dict):
        return json.dumps(value, cls=BaseModelEncoder)
    if isinstance(value, list):
        return json.dumps([convert_type(v) for v in value], cls=BaseModelEncoder)
    return value


class BaseModelEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, BaseModel):
            return obj.to_dict()
        return super().default(obj)


@dataclass
class BaseModel:

    table_name: str = field(init=False, default="")

    def __post_init__(self):
        if not self.table_name:
            self.table_name = self.__class__.__name__.lower()

    def to_dict(self) -> Dict[str, Any]:
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if f.name != "table_name"
        }

    @property
    def columns(self) -> List[str]:
        return [f.name for f in fields(self) if f.name != "table_name"]

    @classmethod
    def pk(cls) -> str:
        return [f.name for f in fields(cls) if f.metadata.get("primary_key")]

    @classmethod
    def foreign_keys(cls) -> List[Union[str, "BaseModel"]]:
        return [
            (f.name, f.metadata["foreign_key"])
            for f in fields(cls)
            if f.metadata.get("foreign_key")
        ]

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "BaseModel":
        type_hints = get_type_hints(cls)
        init_args = {}
        for field_name, field_type in type_hints.items():
            if (
                field_name == "table_name"
                or field_name == "created_at"
                or field_name == "updated_at"
            ):
                continue
            value = data.get(field_name)

            origin = get_origin(field_type)
            args = get_args(field_type)

            if origin is Union and type(None) in args:
                # Get the actual type (first type arg that's not None)
                field_type = next(t for t in get_args(field_type) if t != type(None))

            if hasattr(field_type, "__dataclass_fields__") and isinstance(value, dict):
                init_args[field_name] = field_type.from_json(value)  # Nested dataclass

            elif get_origin(field_type) is list and isinstance(value, list):
                item_type = get_args(field_type)[0]
                if hasattr(item_type, "__dataclass_fields__"):
                    init_args[field_name] = [
                        item_type.from_json(item) if isinstance(item, dict) else item
                        for item in value
                    ]
                else:
                    init_args[field_name] = value
            else:
                init_args[field_name] = value

        return cls(**init_args)

    @classmethod
    def select(
        cls, conn, where: str = "", offset=None, limit=None
    ) -> List["BaseModel"]:

        return conn.execute(Select(cls, "*").where(where).sql(offset, limit)).fetchall()

    def insert(
        self,
        conn: sqlite3.Connection,
        *,
        conflict_mode: Literal["ignore", "replace"] = None,
    ) -> str:

        # Convert any BaseModel values to their JSON representation
        non_null_cols = {
            col: convert_type(value)
            for col, value in self.to_dict().items()
            if value is not None
        }

        non_null_cols["created_at"] = datetime.now().isoformat()
        non_null_cols["updated_at"] = datetime.now().isoformat()

        cols = ", ".join(non_null_cols.keys())
        values = ", ".join(f":{f}" for f in non_null_cols.keys())

        if not conflict_mode:
            sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({values})"
        if conflict_mode == "ignore":
            sql = f"INSERT OR IGNORE INTO {self.table_name} ({cols}) VALUES ({values})"
        if conflict_mode == "replace":
            sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({values}) ON CONFLICT DO UPDATE SET {', '.join(f'{col} = excluded.{col}' for col in non_null_cols.keys() if col != 'created_at' and col not in self.pk())}"

        logger.debug(sql)

        return conn.execute(f"{sql};", non_null_cols)

    def update(self, conn, cols=None) -> str:
        if not cols:
            cols = self.columns

        columns = {
            col: convert_type(value)
            for col, value in self.to_dict().items()
            if col not in self.pk() and col in cols
        }

        pk = {col: getattr(self, col) for col in self.pk()}

        set_clause = ", ".join(f"{col} = ?" for col in columns)

        where_clause = " AND ".join(f"{col} = ?" for col in pk)

        sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause};"

        logger.debug(sql)
        logger.debug(list(columns.values()) + list(pk.values()))

        return conn.execute(sql, list(columns.values()) + list(pk.values()))

    def upsert(self, conn, cols=None) -> str:
        upd = self.update(conn, cols)
        if upd.rowcount == 0:
            return self.insert(conn, conflict_mode="replace")

        return upd

    def delete(self, conn) -> str:
        return conn.execute(
            f"DELETE FROM {self.table_name} WHERE {self.pk} = '{getattr(self, self.pk)}';"
        )

    @classmethod
    def create_table_sql(cls, table_name: str = None) -> str:
        if table_name:
            cls.table_name = table_name

        columns = []
        primary_keys = []
        foreign_keys = []
        for f in fields(cls):
            if f.name == "table_name":
                continue

            sql_type = get_sql_type(f.type)
            column_def = f"{f.name} {sql_type}"
            if f.default is field(default=None).default:
                column_def += " DEFAULT NULL"

            columns.append(column_def)

            if f.metadata.get("primary_key"):
                primary_keys.append(f.name)

            if fk := f.metadata.get("foreign_key"):
                foreign_keys.append((f.name, fk))

        columns.append(f"PRIMARY KEY ({','.join(primary_keys)})")
        if foreign_keys := cls.foreign_keys():
            columns.extend(
                f"FOREIGN KEY ({fk}) REFERENCES {tbl.table_name} ({','.join(tbl.pk())})"
                for fk, tbl in foreign_keys
            )

        columns_clause = ",\n    ".join(columns)
        return (
            f"CREATE TABLE IF NOT EXISTS {cls.table_name} (\n    {columns_clause}\n);"
        )

    def __str__(self) -> str:
        field_values = ", ".join(
            f"{f.name}={repr(getattr(self, f.name))}"
            for f in fields(self)[0:5]
            if f.name != "table_name"
        )
        return f"{self.__class__.__name__}<{field_values}>  "


@dataclass
class DeviationStats(BaseModel):
    comments: int
    favourites: int


@dataclass
class Preview(BaseModel):
    src: str
    height: int
    width: int
    transparency: bool


@dataclass
class Content(BaseModel):
    src: str
    height: int
    width: int
    transparency: bool
    filesize: int


@dataclass
class Thumbnail(BaseModel):
    src: str
    height: int
    width: int
    transparency: bool


@dataclass
class Video(BaseModel):
    src: str
    quality: str
    filesize: int
    duration: int


@dataclass
class DailyDeviation(BaseModel):
    body: str
    time: str
    giver: Dict[str, Any]
    suggester: Optional[Dict[str, Any]]


@dataclass
class MotionBook(BaseModel):
    embed_url: str


@dataclass
class User(BaseModel):
    table_name = "users"

    userid: uuid.UUID = field(metadata={"primary_key": True})
    username: str
    usericon: str
    type: str
    is_watching: Optional[bool]
    is_subscribed: Optional[bool]
    details: Optional[Dict[str, Any]]
    geo: Optional[Dict[str, Any]]
    profile: Optional[Dict[str, Any]]
    stats: Optional[Dict[str, Any]]
    sidebar: Optional[Dict[str, Any]]
    session: Optional[Dict[str, Any]]

    created_at: datetime = field(init=False, default_factory=datetime.now)
    updated_at: datetime = field(init=False, default_factory=datetime.now)

    @classmethod
    def pk(self) -> str:
        return ["userid"]


@dataclass
class Deviation(BaseModel):
    table_name = "deviations"

    deviationid: uuid.UUID = field(metadata={"primary_key": True})
    printid: Optional[str]
    url: Optional[str]
    title: Optional[str]
    is_favourited: Optional[bool]
    is_deleted: bool
    is_published: Optional[bool]
    is_blocked: Optional[bool]
    author: Optional[User]
    user_id: Optional[uuid.UUID] = field(metadata={"foreign_key": User})
    stats: Optional[DeviationStats]
    published_time: Optional[str]
    allows_comments: Optional[bool]
    tier: Optional[Dict[str, Any]]
    preview: Optional[Preview]
    content: Optional[Content]
    thumbs: Optional[List[Thumbnail]]
    videos: Optional[List[Video]]
    flash: Optional[Dict[str, Any]]
    daily_deviation: Optional[DailyDeviation]
    premium_folder_data: Optional[Dict[str, Any]]
    text_content: Optional[Dict[str, Any]]
    is_pinned: Optional[bool]
    cover_image: Optional[Dict[str, Any]]
    tier_access: Optional[str]
    primary_tier: Optional[Dict[str, Any]]
    excerpt: Optional[str]
    is_mature: Optional[bool]
    is_downloadable: Optional[bool]
    download_filesize: Optional[int]
    motion_book: Optional[MotionBook]

    created_at: datetime = field(init=False, default_factory=datetime.now)
    updated_at: datetime = field(init=False, default_factory=datetime.now)


@dataclass
class DeviationActivity(BaseModel):
    table_name = "deviation_activity"

    deviationid: uuid.UUID = field(
        metadata={"primary_key": True, "foreign_key": Deviation}
    )
    userid: uuid.UUID = field(metadata={"primary_key": True})
    action: str = field(metadata={"primary_key": True})
    time: int = field(metadata={"primary_key": True})

    timestamp: datetime

    created_at: datetime = field(init=False, default_factory=datetime.now)
    updated_at: datetime = field(init=False, default_factory=datetime.now)


@dataclass
class Tag(BaseModel):
    tag_name: str
    sponsored: bool
    sponsor: str


@dataclass
class Submission(BaseModel):
    creation_time: str
    category: str
    file_size: Optional[str]
    resolution: Optional[str]


@dataclass
class Stats(BaseModel):
    views: int
    views_today: Optional[int]
    favourites: int
    comments: int
    downloads: int


@dataclass
class Camera:
    pass


@dataclass
class Collection(BaseModel):
    table_name = "collections"

    folderid: uuid.UUID = field(metadata={"primary_key": True})
    name: str

    created_at: datetime = field(init=False, default_factory=datetime.now)
    updated_at: datetime = field(init=False, default_factory=datetime.now)


@dataclass
class Gallery(BaseModel):
    table_name = "galleries"

    folderid: uuid.UUID = field(metadata={"primary_key": True})
    name: str

    created_at: datetime = field(init=False, default_factory=datetime.now)
    updated_at: datetime = field(init=False, default_factory=datetime.now)


@dataclass
class DeviationMetadata(BaseModel):
    table_name = "deviation_metadata"

    deviationid: uuid.UUID = field(metadata={"primary_key": True})
    printid: Optional[uuid.UUID]
    author: User
    user_id: Optional[uuid.UUID] = field(metadata={"foreign_key": User})
    is_watching: bool
    title: str
    description: str
    license: str
    allows_comments: bool
    tags: List[Tag]
    is_favourited: bool
    is_mature: bool
    mature_level: Optional[str]
    mature_classification: Optional[List[str]]
    submission: Optional[Submission]
    stats: Optional[Stats]
    camera: Optional[Dict[str, Any]]
    collections: Optional[List[Collection]]
    galleries: Optional[List[Gallery]]
    can_post_comment: bool

    created_at: datetime = field(init=False, default_factory=datetime.now)
    updated_at: datetime = field(init=False, default_factory=datetime.now)
