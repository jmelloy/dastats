"""
Helper functions for migrating from direct sqlite3 operations to SQLAlchemy
"""
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlmodel import SQLModel
import uuid
import logging

logger = logging.getLogger(__name__)


def upsert_model(session: Session, model_instance: SQLModel):
    """
    Insert or update a model instance using SQLAlchemy session.
    This replaces the old insert(db, conflict_mode="replace") pattern.
    """
    try:
        # Try to merge (upsert) the instance
        merged = session.merge(model_instance)
        session.flush()
        return merged
    except Exception as e:
        logger.error(f"Error upserting {type(model_instance).__name__}: {e}")
        raise


def bulk_upsert(session: Session, model_instances: list):
    """
    Bulk insert/update multiple model instances.
    """
    for instance in model_instances:
        try:
            session.merge(instance)
        except Exception as e:
            logger.error(f"Error upserting {type(instance).__name__}: {e}")
            # Continue with other instances
    session.flush()


def convert_uuid(value):
    """Convert UUID to string for SQLite storage"""
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def prepare_model_for_db(model_instance: SQLModel):
    """
    Prepare a model instance for database insertion by converting UUIDs to strings.
    This is only needed for SQLite.
    """
    # For SQLite, we store UUIDs as strings
    for field_name, field_value in model_instance.__dict__.items():
        if isinstance(field_value, uuid.UUID):
            setattr(model_instance, field_name, str(field_value))
    return model_instance


def execute_raw_sql(session: Session, query: str, params: dict = None):
    """
    Execute raw SQL query (for backwards compatibility with old query patterns)
    """
    try:
        if params:
            result = session.execute(text(query), params)
        else:
            result = session.execute(text(query))
        return result
    except Exception as e:
        logger.error(f"Error executing raw SQL: {e}")
        logger.error(f"Query: {query}")
        raise
