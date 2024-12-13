from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional, Union, get_type_hints, NewType
import uuid


def get_sql_type(field_type: Any) -> str:
    # Mapping Python types to SQL types for DuckDB
    type_mapping = {
        int: "BIGINT",
        str: "VARCHAR",
        float: "DOUBLE",
        bool: "BOOLEAN",
        dict: "JSON",
        list: "JSON",  # Arrays and lists default to JSON
        BaseModel: "STRUCT",  # Nested dataclasses map to STRUCT
        uuid.UUID: "UUID",
    }

    # Handle Optional types by extracting the inner type
    if hasattr(field_type, "__origin__") and field_type.__origin__ is Optional:
        field_type = field_type.__args__[0]

    # Handle List types by extracting the inner type
    if hasattr(field_type, "__origin__") and field_type.__origin__ is list:
        field_type = field_type.__args__[0]

    # Check for nested dataclass
    if hasattr(field_type, "__dataclass_fields__"):
        nested_columns = []
        for nested_field_name, nested_field_type in get_type_hints(field_type).items():
            if nested_field_name == "table_name":
                continue
            nested_sql_type = get_sql_type(nested_field_type)
            nested_columns.append(f"{nested_field_name} {nested_sql_type}")
        return f"STRUCT({', '.join(nested_columns)})"

    # Return the DuckDB equivalent type or 'TEXT' if unknown
    return type_mapping.get(field_type, "TEXT")


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
            if field_name == "table_name":
                continue
            value = data.get(field_name)
            if hasattr(field_type, "__dataclass_fields__") and isinstance(value, dict):
                init_args[field_name] = field_type.from_json(value)  # Nested dataclass
            elif field_type == Optional[List[Any]] and isinstance(value, list):
                init_args[field_name] = [
                    field_type.__args__[0](**item) if isinstance(item, dict) else item
                    for item in value
                ]  # List of nested dataclasses
            else:
                init_args[field_name] = value
        return cls(**init_args)

    def insert_sql(self, ignore_conflicts: bool = False, duplicate: str = None) -> str:
        cols = ", ".join(f for f in self.columns)
        values = ", ".join(f"?" for f in self.columns)

        sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({values})"
        if ignore_conflicts:
            sql += " ON CONFLICT DO NOTHING"
        elif duplicate:
            sql += f" ON CONFLICT ({duplicate}) DO UPDATE SET {', '.join(f'{f.name}=EXCLUDED.{f.name}' for f in fields(self) if f.name != "table_name" and f.name not in self.pk())}"

        return (f"{sql};", [getattr(self, f) for f in self.columns])

    def select_sql(self, where_clause: str = "") -> str:
        cols = ", ".join(f for f in self.columns)
        sql = f"SELECT {cols} FROM {self.table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        return sql + ";"

    def update_sql(self, where_clause: str) -> str:
        set_clause = ", ".join(f"{f.name} = ?" for f in self.columns)

        return (
            f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause};",
            [getattr(self, f.name) for f in self.columns],
        )

    def delete_sql(self, where_clause: str) -> str:
        return f"DELETE FROM {self.table_name} WHERE {where_clause};"

    @classmethod
    def create_table_sql(cls) -> str:

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
class Stats:
    comments: int
    favourites: int


@dataclass
class Preview:
    src: str
    height: int
    width: int
    transparency: bool


@dataclass
class Content:
    src: str
    height: int
    width: int
    transparency: bool
    filesize: int


@dataclass
class Thumbnail:
    src: str
    height: int
    width: int
    transparency: bool


@dataclass
class Video:
    src: str
    quality: str
    filesize: int
    duration: int


@dataclass
class DailyDeviation:
    body: str
    time: str
    giver: Dict[str, Any]
    suggester: Optional[Dict[str, Any]]


@dataclass
class MotionBook:
    embed_url: str


@dataclass
class User(BaseModel):
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

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "User":
        return BaseModel.from_json(cls, data)

    @classmethod
    def pk(self) -> str:
        return "userid"


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
    stats: Optional[Stats]
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


@dataclass
class Activity(BaseModel):
    table_name = "deviation_activity"

    deviationid: uuid.UUID = field(
        metadata={"primary_key": True, "foreign_key": Deviation}
    )
    userId: uuid.UUID = field(metadata={"primary_key": True})
    action: str = field(metadata={"primary_key": True})
    time: str = field(metadata={"primary_key": True})
    user: User
