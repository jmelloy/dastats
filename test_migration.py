#!/usr/bin/env python3
"""
Simple test to verify the SQLAlchemy/SQLModel migration works correctly.
"""
import tempfile
import os
import uuid
from datetime import datetime

from database import init_db, get_session
from models import User, Deviation, Gallery, Collection, Message
from db_helpers import upsert_model


def test_basic_operations():
    """Test basic CRUD operations with SQLModel"""
    print("Testing basic SQLAlchemy/SQLModel operations...")
    
    # Create a temporary database using secure method
    import tempfile
    temp_file = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
    test_db = temp_file.name
    temp_file.close()
    print(f"Using test database: {test_db}")
    
    try:
        # Initialize database
        engine = init_db(test_db)
        print("✓ Database initialized")
        
        # Get a session
        session = get_session()
        
        try:
            # Test User creation
            user_id = uuid.uuid4()
            user = User(
                userid=user_id,
                username="testuser",
                usericon="https://example.com/icon.jpg",
                type="regular"
            )
            upsert_model(session, user)
            session.commit()
            print("✓ User created")
            
            # Test Deviation creation
            deviation_id = uuid.uuid4()
            deviation = Deviation(
                deviationid=deviation_id,
                title="Test Deviation",
                url="https://deviantart.com/test",
                is_deleted=False,
                user_id=user_id
            )
            upsert_model(session, deviation)
            session.commit()
            print("✓ Deviation created")
            
            # Test Gallery creation
            gallery_id = uuid.uuid4()
            gallery = Gallery(
                folderid=gallery_id,
                name="Test Gallery"
            )
            upsert_model(session, gallery)
            session.commit()
            print("✓ Gallery created")
            
            # Test Collection creation
            collection_id = uuid.uuid4()
            collection = Collection(
                folderid=collection_id,
                name="Test Collection"
            )
            upsert_model(session, collection)
            session.commit()
            print("✓ Collection created")
            
            # Test Message creation
            message = Message(
                messageid=uuid.uuid4(),
                type="feedback.favourite",
                orphaned=False,
                is_new=True,
                deviationid=deviation_id,
                ts=datetime.now()
            )
            upsert_model(session, message)
            session.commit()
            print("✓ Message created")
            
            # Query back the user
            from sqlalchemy import select
            stmt = select(User).where(User.userid == user_id)
            result = session.execute(stmt)
            queried_user = result.scalar_one_or_none()
            
            if queried_user and queried_user.username == "testuser":
                print("✓ User query successful")
            else:
                print("✗ User query failed")
                return False
            
            # Count deviations
            stmt = select(Deviation)
            result = session.execute(stmt)
            deviations = result.scalars().all()
            print(f"✓ Found {len(deviations)} deviation(s)")
            
            print("\n✅ All tests passed!")
            return True
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Clean up
        if os.path.exists(test_db):
            os.remove(test_db)
            print(f"Cleaned up test database")


if __name__ == "__main__":
    success = test_basic_operations()
    exit(0 if success else 1)
