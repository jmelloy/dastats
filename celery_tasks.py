from celery import Celery
from da import DeviantArt, populate_gallery, populate_metadata, populate_favorites
import sqlite3
import os
import logging

# Configure Celery
app = Celery('da_tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

# Configure logging
logger = logging.getLogger(__name__)

@app.task
def populate_gallery_task(username=None, gallery="all", full=False, offset=0):
    """Task to populate gallery data"""
    try:
        da = DeviantArt(sqlitedb=os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{username}.sqlite" if username else "deviantart_data.sqlite"))
        da.check_token()
        
        with sqlite3.connect(da.sqlite_db) as db:
            populate_gallery(da, db, gallery=gallery, username=username, full=full, offset=offset)
        return {"status": "success", "message": f"Gallery data populated for {username or 'all users'}"}
    except Exception as e:
        logger.error(f"Error in populate_gallery_task: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.task
def populate_metadata_task(username=None):
    """Task to populate metadata"""
    try:
        da = DeviantArt(sqlitedb=os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{username}.sqlite" if username else "deviantart_data.sqlite"))
        da.check_token()
        
        with sqlite3.connect(da.sqlite_db) as db:
            populate_metadata(da, db)
        return {"status": "success", "message": f"Metadata populated for {username or 'all users'}"}
    except Exception as e:
        logger.error(f"Error in populate_metadata_task: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.task
def populate_favorites_task(username=None):
    """Task to populate favorites"""
    try:
        da = DeviantArt(sqlitedb=os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{username}.sqlite" if username else "deviantart_data.sqlite"))
        da.check_token()
        
        with sqlite3.connect(da.sqlite_db) as db:
            populate_favorites(da, db)
        return {"status": "success", "message": f"Favorites populated for {username or 'all users'}"}
    except Exception as e:
        logger.error(f"Error in populate_favorites_task: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.task
def full_populate_task(username=None, full=False, offset=0):
    """Task to run the full populate process"""
    try:
        da = DeviantArt(sqlitedb=os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{username}.sqlite" if username else "deviantart_data.sqlite"))
        da.check_token()
        
        # Chain the tasks in order
        gallery_result = populate_gallery_task.delay(username, "all", full, offset)
        metadata_result = populate_metadata_task.delay(username)
        favorites_result = populate_favorites_task.delay(username)
        
        return {
            "status": "success",
            "message": "Full populate process started",
            "tasks": {
                "gallery": gallery_result.id,
                "metadata": metadata_result.id,
                "favorites": favorites_result.id
            }
        }
    except Exception as e:
        logger.error(f"Error in full_populate_task: {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == '__main__':
    app.start() 