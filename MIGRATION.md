# SQLAlchemy and SQLModel Migration

## Overview

This project has been successfully migrated from a custom SQLite ORM to SQLAlchemy and SQLModel. This migration provides better maintainability, type safety, and compatibility with modern Python database tooling.

## Changes Made

### 1. Dependencies
Added to `requirements.txt`:
- `sqlalchemy` - Core ORM functionality
- `sqlmodel` - Pydantic-based ORM built on SQLAlchemy

### 2. New Files

#### `database.py`
- Database engine initialization
- Session management
- Connection pooling configuration

#### `db_helpers.py`
- Helper functions for database operations
- `upsert_model()` - Insert or update model instances
- `execute_raw_sql()` - Execute raw SQL queries with parameters

#### `test_migration.py`
- Comprehensive test suite for the migration
- Tests CRUD operations for all models
- Verifies UUID handling and foreign key relationships

### 3. Updated Files

#### `models.py`
- Converted all dataclasses to SQLModel classes
- Added custom `GUID` TypeDecorator for UUID handling in SQLite
- Maintained backwards compatibility with the `Select` query builder
- All models now use SQLModel's declarative syntax

Key models:
- `User` - DeviantArt user accounts
- `Deviation` - Art submissions
- `DeviationMetadata` - Extended metadata for deviations
- `DeviationActivity` - Activity tracking (favorites, etc.)
- `Message` - Notification messages
- `Gallery` - Gallery folders
- `Collection` - Art collections

#### `da.py`
- Updated to use SQLAlchemy sessions instead of sqlite3 connections
- All database operations now use `session.merge()` for upserts
- Maintained all existing API functionality

#### `sql.py`
- Updated all query functions to use SQLAlchemy sessions
- Raw SQL queries now use parameterized statements for safety
- All functions return the same data structures as before

#### `app.py`
- No changes needed - works with updated backend automatically

#### `celery_tasks.py`
- Updated to use SQLAlchemy sessions
- All tasks now properly manage database connections

#### Other Files
- `duplicates.py` - Updated database queries
- `rename.py` - Updated database access
- `image_comparison.py` - Updated to use SQLAlchemy

### 4. UUID Handling

Created a custom `GUID` TypeDecorator that:
- Stores UUIDs as strings (CHAR(36)) in SQLite
- Automatically converts between UUID objects and strings
- Would use native UUID types in PostgreSQL if migrated
- Ensures type safety while maintaining SQLite compatibility

## Benefits

1. **Type Safety**: SQLModel provides Pydantic-based validation
2. **Better ORM**: Industry-standard SQLAlchemy ORM with extensive features
3. **Migrations**: Can use Alembic for schema migrations if needed
4. **Database Agnostic**: Easy to migrate to PostgreSQL or MySQL in the future
5. **Better Testing**: Easier to write and maintain tests
6. **IDE Support**: Better autocomplete and type checking

## Backwards Compatibility

- The `Select` query builder class has been preserved for backwards compatibility
- All existing API endpoints work unchanged
- Database schema remains the same
- No changes required for users of the application

## Testing

Run the test suite:
```bash
python3 test_migration.py
```

All tests should pass, verifying:
- Database initialization
- CRUD operations for all models
- Foreign key relationships
- UUID handling
- Query operations

## Migration from Old Databases

Existing SQLite databases will work with the new code without modification. The schema is compatible, and SQLAlchemy will automatically create tables if they don't exist.

## Future Improvements

Possible enhancements now that we're using SQLAlchemy:

1. **Alembic Migrations**: Add proper schema version control
2. **Connection Pooling**: Optimize for high-traffic scenarios
3. **Query Optimization**: Use SQLAlchemy's query optimization features
4. **Database Switching**: Easy migration to PostgreSQL for better concurrency
5. **Async Support**: Could add async SQLAlchemy support if needed
6. **Better Relationships**: Define explicit ORM relationships between models

## Notes

- The old custom ORM code has been removed
- Backup files (`models_old.py`, `models.py.backup`) are in .gitignore
- All UUID fields are properly handled for SQLite compatibility
- Raw SQL queries are still used where appropriate for complex operations
