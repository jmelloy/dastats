from typing import Optional, Dict, Any, List
from datetime import datetime
import uuid
import json
from sqlmodel import SQLModel, Field, Column, Relationship
from sqlalchemy import JSON, String, DateTime, Boolean, text, ForeignKey
from sqlalchemy.types import TypeDecorator, CHAR
from sqlalchemy.dialects.postgresql import UUID as PGUUID
import logging

logger = logging.getLogger(__name__)


# Custom UUID type for SQLite compatibility
class GUID(TypeDecorator):
    """Platform-independent GUID type.
    Uses PostgreSQL's UUID type, otherwise uses CHAR(32), storing as stringified hex values.
    """
    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(PGUUID())
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if isinstance(value, uuid.UUID):
                return str(value)
            else:
                return str(uuid.UUID(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value


# Custom JSON encoder for datetime and UUID
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)


# Helper function to convert UUID to string for SQLite
def uuid_to_str(val):
    if isinstance(val, uuid.UUID):
        return str(val)
    return val


# Nested models that will be stored as JSON
class DeviationStats(SQLModel):
    comments: int
    favourites: int


class Preview(SQLModel):
    src: str
    height: int
    width: int
    transparency: bool


class Content(SQLModel):
    src: str
    height: int
    width: int
    transparency: bool
    filesize: int


class Thumbnail(SQLModel):
    src: str
    height: int
    width: int
    transparency: bool


class Video(SQLModel):
    src: str
    quality: str
    filesize: int
    duration: int


class DailyDeviation(SQLModel):
    body: str
    time: str
    giver: Dict[str, Any]
    suggester: Optional[Dict[str, Any]] = None


class MotionBook(SQLModel):
    embed_url: str


class Tag(SQLModel):
    tag_name: str
    sponsored: bool
    sponsor: str


class Submission(SQLModel):
    creation_time: str
    category: str
    file_size: Optional[str] = None
    resolution: Optional[str] = None


class Stats(SQLModel):
    views: int
    views_today: Optional[int] = None
    favourites: int
    comments: int
    downloads: int


# Database models
class User(SQLModel, table=True):
    __tablename__ = "users"
    
    userid: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        sa_column=Column(GUID, primary_key=True)
    )
    username: str
    usericon: str
    type: str
    is_watching: Optional[bool] = None
    is_subscribed: Optional[bool] = None
    details: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    geo: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    profile: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    stats: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    sidebar: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    session: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'), onupdate=datetime.now)
    )

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "User":
        """Create User instance from JSON data"""
        # Remove fields that don't exist in the model
        filtered_data = {k: v for k, v in data.items() if k in cls.model_fields and k not in ['created_at', 'updated_at']}
        if 'userid' in filtered_data and isinstance(filtered_data['userid'], str):
            filtered_data['userid'] = uuid.UUID(filtered_data['userid'])
        return cls(**filtered_data)


class Deviation(SQLModel, table=True):
    __tablename__ = "deviations"
    
    deviationid: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        sa_column=Column(GUID, primary_key=True)
    )
    printid: Optional[str] = None
    url: Optional[str] = None
    title: Optional[str] = None
    is_favourited: Optional[bool] = None
    is_deleted: bool = False
    is_published: Optional[bool] = None
    is_blocked: Optional[bool] = None
    
    user_id: Optional[uuid.UUID] = Field(default=None, sa_column=Column(GUID, ForeignKey("users.userid")))
    
    # JSON fields for complex nested data
    author: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    stats: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    published_time: Optional[str] = None
    allows_comments: Optional[bool] = None
    tier: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    preview: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    content: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    thumbs: Optional[List[Dict[str, Any]]] = Field(default=None, sa_column=Column(JSON))
    videos: Optional[List[Dict[str, Any]]] = Field(default=None, sa_column=Column(JSON))
    flash: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    daily_deviation: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    premium_folder_data: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    text_content: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    is_pinned: Optional[bool] = None
    cover_image: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    tier_access: Optional[str] = None
    primary_tier: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    excerpt: Optional[str] = None
    is_mature: Optional[bool] = None
    is_downloadable: Optional[bool] = None
    download_filesize: Optional[int] = None
    motion_book: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'), onupdate=datetime.now)
    )

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Deviation":
        """Create Deviation instance from JSON data"""
        filtered_data = {}
        for k, v in data.items():
            if k in ['created_at', 'updated_at']:
                continue
            if k in cls.model_fields:
                if k == 'deviationid' and isinstance(v, str):
                    filtered_data[k] = uuid.UUID(v)
                elif k == 'user_id' and v and isinstance(v, str):
                    filtered_data[k] = uuid.UUID(v)
                elif k == 'author' and isinstance(v, dict):
                    filtered_data[k] = v
                    # Extract user_id from author if present
                    if 'userid' in v:
                        filtered_data['user_id'] = uuid.UUID(v['userid']) if isinstance(v['userid'], str) else v['userid']
                else:
                    filtered_data[k] = v
        
        return cls(**filtered_data)


class DeviationActivity(SQLModel, table=True):
    __tablename__ = "deviation_activity"
    
    deviationid: uuid.UUID = Field(
        sa_column=Column(GUID, ForeignKey("deviations.deviationid"), primary_key=True)
    )
    userid: uuid.UUID = Field(
        sa_column=Column(GUID, primary_key=True)
    )
    action: str = Field(primary_key=True)
    time: int = Field(primary_key=True)
    
    timestamp: datetime
    
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'), onupdate=datetime.now)
    )


class Collection(SQLModel, table=True):
    __tablename__ = "collections"
    
    folderid: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        sa_column=Column(GUID, primary_key=True)
    )
    name: str
    
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'), onupdate=datetime.now)
    )


class Gallery(SQLModel, table=True):
    __tablename__ = "galleries"
    
    folderid: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        sa_column=Column(GUID, primary_key=True)
    )
    name: str
    
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'), onupdate=datetime.now)
    )


class DeviationMetadata(SQLModel, table=True):
    __tablename__ = "deviation_metadata"
    
    deviationid: uuid.UUID = Field(
        sa_column=Column(GUID, primary_key=True)
    )
    printid: Optional[uuid.UUID] = Field(default=None, sa_column=Column(GUID))
    user_id: Optional[uuid.UUID] = Field(default=None, sa_column=Column(GUID, ForeignKey("users.userid")))
    
    # JSON fields
    author: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    is_watching: bool = False
    title: str
    description: str
    license: str
    allows_comments: bool
    tags: List[Dict[str, Any]] = Field(default=[], sa_column=Column(JSON))
    is_favourited: bool
    is_mature: bool
    mature_level: Optional[str] = None
    mature_classification: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    submission: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    stats: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    camera: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    collections: Optional[List[Dict[str, Any]]] = Field(default=None, sa_column=Column(JSON))
    galleries: Optional[List[Dict[str, Any]]] = Field(default=None, sa_column=Column(JSON))
    can_post_comment: bool = False
    
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'), onupdate=datetime.now)
    )

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "DeviationMetadata":
        """Create DeviationMetadata instance from JSON data"""
        filtered_data = {}
        for k, v in data.items():
            if k in ['created_at', 'updated_at']:
                continue
            if k in cls.model_fields:
                if k == 'deviationid' and isinstance(v, str):
                    filtered_data[k] = uuid.UUID(v)
                elif k == 'printid' and v and isinstance(v, str):
                    filtered_data[k] = uuid.UUID(v)
                elif k == 'user_id' and v and isinstance(v, str):
                    filtered_data[k] = uuid.UUID(v)
                elif k == 'author' and isinstance(v, dict):
                    filtered_data[k] = v
                    # Extract user_id from author if present
                    if 'userid' in v:
                        filtered_data['user_id'] = uuid.UUID(v['userid']) if isinstance(v['userid'], str) else v['userid']
                else:
                    filtered_data[k] = v
        
        return cls(**filtered_data)


class Message(SQLModel, table=True):
    __tablename__ = "messages"
    
    messageid: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        sa_column=Column(GUID, primary_key=True)
    )
    type: str
    orphaned: bool
    ts: Optional[datetime] = None
    stackid: Optional[str] = None
    stack_count: Optional[int] = None
    is_new: bool
    
    # JSON fields for complex nested data
    originator: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    subject: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    profile: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    deviation: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    status: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    comment: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    collection: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    gallery: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    html: Optional[str] = None
    
    deviationid: Optional[uuid.UUID] = Field(default=None, sa_column=Column(GUID, ForeignKey("deviations.deviationid")))
    
    created_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column=Column(DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'), onupdate=datetime.now)
    )

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Message":
        """Create Message instance from JSON data"""
        filtered_data = {}
        for k, v in data.items():
            if k in ['created_at', 'updated_at']:
                continue
            if k in cls.model_fields:
                if k == 'messageid' and isinstance(v, str):
                    filtered_data[k] = uuid.UUID(v)
                elif k == 'deviationid' and v and isinstance(v, str):
                    filtered_data[k] = uuid.UUID(v)
                else:
                    filtered_data[k] = v
        
        # Set deviationid from subject or deviation if not directly provided
        if 'deviationid' not in filtered_data or filtered_data['deviationid'] is None:
            if 'subject' in filtered_data and filtered_data['subject']:
                dev_id = filtered_data['subject'].get('deviation', {}).get('deviationid')
                if dev_id:
                    filtered_data['deviationid'] = uuid.UUID(dev_id) if isinstance(dev_id, str) else dev_id
            elif 'deviation' in filtered_data and filtered_data['deviation']:
                dev_id = filtered_data['deviation'].get('deviationid')
                if dev_id:
                    filtered_data['deviationid'] = uuid.UUID(dev_id) if isinstance(dev_id, str) else dev_id
        
        return cls(**filtered_data)


# For backwards compatibility - keep the Select class from old models
class Select:
    """Legacy Select class for backwards compatibility"""
    def __init__(self, model, columns="*"):
        if isinstance(model, str):
            self.table = model
        else:
            self.table = model.__tablename__ if hasattr(model, '__tablename__') else model.__name__.lower()
        
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
    
    def from_clause(self, model):
        if type(model) is str:
            self.from_clauses.append(model)
        else:
            self.from_clauses.append(model.__tablename__ if hasattr(model, '__tablename__') else model.__name__.lower())
        return self
    
    def join(self, model, *, how="inner", on=None, condition=None):
        """Add a JOIN clause."""
        if type(model) is str:
            table = model
        else:
            table = model.__tablename__ if hasattr(model, '__tablename__') else model.__name__.lower()
        
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
