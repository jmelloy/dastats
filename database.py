"""Database configuration and session management for SQLAlchemy"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlmodel import SQLModel
import os
import logging

logger = logging.getLogger(__name__)

# Global engine and session factory
_engine = None
_SessionLocal = None


def get_database_url(sqlite_db=None):
    """Get the database URL based on configuration"""
    if sqlite_db:
        db_path = sqlite_db
    else:
        file_path = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(file_path, "deviantart_data.sqlite")
    
    # Use SQLite with proper connection string
    return f"sqlite:///{db_path}"


def init_db(sqlite_db=None):
    """Initialize the database engine and session factory"""
    global _engine, _SessionLocal
    
    database_url = get_database_url(sqlite_db)
    logger.info(f"Initializing database with URL: {database_url}")
    
    # Create engine with proper SQLite configuration
    _engine = create_engine(
        database_url,
        echo=False,
        connect_args={"check_same_thread": False}  # Needed for SQLite
    )
    
    # Create session factory
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    
    # Import all models to ensure they're registered
    from models import (
        User, Deviation, DeviationActivity, Collection, 
        Gallery, DeviationMetadata, Message
    )
    
    # Create all tables
    SQLModel.metadata.create_all(_engine)
    
    return _engine


def get_engine():
    """Get the SQLAlchemy engine"""
    global _engine
    if _engine is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _engine


def get_session() -> Session:
    """Get a new database session"""
    global _SessionLocal
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _SessionLocal()


def get_db():
    """Dependency for getting DB session (FastAPI style)"""
    db = get_session()
    try:
        yield db
    finally:
        db.close()
