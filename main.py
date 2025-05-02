from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

# Database connection details (from environment variables / secrets)
DB_HOST = os.environ.get("DATABASE_HOST")
DB_USER = os.environ.get("DATABASE_USER")
DB_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DB_NAME = os.environ.get("DATABASE_NAME")

# Database Connection Function (Dependency)
def get_db_connection():
    conn = None  # Initialize conn as None
    try:
        print(f"Attempting DB connection to host={DB_HOST} db={DB_NAME} user={DB_USER}") # Added log
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursor_factory=RealDictCursor,  # Return results as dictionaries
        )
        print("DB connection successful.") # Added log
        yield conn # Yield connection only if successful
    except psycopg2.OperationalError as e:
        # Log the error AND potentially the connection details used (be careful with passwords!)
        print(f"Database connection error (OperationalError): {e}")
        print(f"Failed connection details: host={DB_HOST}, user={DB_USER}, db={DB_NAME}") # Log details used
        raise HTTPException(status_code=503, detail=f"Database connection error: {e}") from e
    except Exception as e:
        print(f"Unexpected error during DB connection attempt: {type(e).__name__} - {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error during DB connection: {type(e).__name__} - {e}") from e
    finally:
        # This block will now always execute, and 'conn' will always be defined
        if conn is not None: # Check if conn was successfully assigned
            print("Closing database connection.") # Added log
            conn.close()
        else:
            # This branch is reached if connect() failed
            print("No active database connection to close.") # Added log

####################################################### top 50 projects trend #######################################################

# Pydantic Models (Data Validation) - top 50 projects trend view
class top_50_projects_trend(BaseModel):
    project_title: str
    report_date: str
    weighted_score_index: float
    fork_count: float
    stargaze_count: float
    commit_count: float
    contributor_count: float
    watcher_count: float
    is_not_fork_ratio: float
    commit_count_pct_change_over_4_weeks: float
    contributor_count_pct_change_over_4_weeks: float
    fork_count_pct_change_over_4_weeks: float
    stargaze_count_pct_change_over_4_weeks: float
    watcher_count_pct_change_over_4_weeks: float
    is_not_fork_ratio_pct_change_over_4_weeks: float
    project_rank_category: str

@app.get("/projects/top50-trend", response_model=List[top_50_projects_trend])
async def get_top_50_trend(db: psycopg2.extensions.connection = Depends(get_db_connection)):
    """
    Retrieves the top 50 projects by weighted score index from the api schema in postgres database.
    """
    if db is None: # Good check, though get_db_connection should raise exceptions
         raise HTTPException(status_code=503, detail="Database not connected")
    try:
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
            cur.execute("""
                SELECT 
                    project_title, 
                    report_date, 
                    weighted_score_index,
                    fork_count, 
                    stargaze_count, 
                    commit_count, 
                    contributor_count, 
                    watcher_count,
                    is_not_fork_ratio, 
                    commit_count_pct_change_over_4_weeks,
                    contributor_count_pct_change_over_4_weeks, 
                    fork_count_pct_change_over_4_weeks,
                    stargaze_count_pct_change_over_4_weeks, 
                    watcher_count_pct_change_over_4_weeks,
                    is_not_fork_ratio_pct_change_over_4_weeks, 
                    project_rank_category

                FROM api.top_50_projects_trend;
            """) 
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/projects/top50-trend/{project_title}", response_model=top_50_projects_trend)
async def get_top_50_trend_project(project_title: str, db: psycopg2.extensions.connection = Depends(get_db_connection)):
     """Retrieves a single project by project_title."""
     try:
        if db is None: # Good check, though get_db_connection should raise exceptions
            raise HTTPException(status_code=503, detail="Database not connected")

        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
             cur.execute("""
                SELECT
                    project_title,
                    report_date,
                    weighted_score_index,
                    stargaze_count, 
                    commit_count, 
                    contributor_count, 
                    watcher_count,
                    is_not_fork_ratio, 
                    commit_count_pct_change_over_4_weeks,
                    contributor_count_pct_change_over_4_weeks, 
                    fork_count_pct_change_over_4_weeks,
                    stargaze_count_pct_change_over_4_weeks, 
                    watcher_count_pct_change_over_4_weeks,
                    is_not_fork_ratio_pct_change_over_4_weeks, 
                    project_rank_category

                FROM api.top_50_projects_trend 
                WHERE project_title = %s;
             """, (project_title,))
             result = cur.fetchone()
             if result is None:
                  raise HTTPException(status_code=404, detail="Project not found")
             return result
     except HTTPException:  # Re-raise HTTPException
         raise
     except Exception as e:
          raise HTTPException(status_code=500, detail=str(e))

####################################################### end top 50 projects trend #######################################################


####################################################### forks #######################################################

# Pydantic Models (Data Validation) - top 100 forks view
class top_100_projects_forks(BaseModel):  # Replace with your actual data structure
    project_title: str
    latest_data_timestamp: str
    fork_count: int

@app.get("/projects/top100-forks-count", response_model=List[top_100_projects_forks])
async def get_top_100_forks(db: psycopg2.extensions.connection = Depends(get_db_connection)):
    """
    Retrieves the top 100 projects by forks from the api schema in postgresdatabase.
    """
    if db is None: # Good check, though get_db_connection should raise exceptions
         raise HTTPException(status_code=503, detail="Database not connected")
    try:
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
            cur.execute("SELECT * FROM api.top_100_projects_fork_count;")  # Use your view
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/projects/top100-forks-count/{project_title}", response_model=top_100_projects_forks)
async def get_top_100_forks_project(project_title: str, db: psycopg2.extensions.connection = Depends(get_db_connection)):
     """Retrieves a single project by project_title."""
     try:
        if db is None: # Good check, though get_db_connection should raise exceptions
            raise HTTPException(status_code=503, detail="Database not connected")

        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
             cur.execute("SELECT * FROM api.top_100_projects_fork_count WHERE project_title = %s;", (project_title,))
             result = cur.fetchone()
             if result is None:
                  raise HTTPException(status_code=404, detail="Project not found")
             return result
     except HTTPException:  # Re-raise HTTPException
         raise
     except Exception as e:
          raise HTTPException(status_code=500, detail=str(e))

####################################################### forks #######################################################

####################################################### stars #######################################################  

# Pydantic Models (Data Validation) - top 100 stars view
class top_100_projects_stars(BaseModel):  # Replace with your actual data structure
    project_title: str
    latest_data_timestamp: str
    stargaze_count: int

@app.get("/projects/top100-star-count", response_model=List[top_100_projects_stars])
async def get_top_100_stars(db: psycopg2.extensions.connection = Depends(get_db_connection)):
    """
    Retrieves the top 100 projects by stars from the api schema in postgresdatabase.
    """
    try:
        if db is None: # Good check, though get_db_connection should raise exceptions
            raise HTTPException(status_code=503, detail="Database not connected")
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
            cur.execute("SELECT * FROM api.top_100_projects_stargaze_count;")  # Use your view
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/projects/top100-star-count/{project_title}", response_model=top_100_projects_stars)
async def get_top_100_stars_project(project_title: str, db: psycopg2.extensions.connection = Depends(get_db_connection)):
     """Retrieves a single project by project_title."""
     try:
        if db is None: # Good check, though get_db_connection should raise exceptions
            raise HTTPException(status_code=503, detail="Database not connected")
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
             cur.execute("SELECT * FROM api.top_100_projects_stargaze_count WHERE project_title = %s;", (project_title,))
             result = cur.fetchone()
             if result is None:
                  raise HTTPException(status_code=404, detail="Project not found")
             return result
     except HTTPException:  # Re-raise HTTPException
         raise
     except Exception as e:
          raise HTTPException(status_code=500, detail=str(e))

####################################################### stars #######################################################   

####################################################### health #######################################################

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    # This endpoint requires no authentication and simply returns 200 OK
    # You could add logic here later to check DB connections, etc., if needed
    # but for a basic uptime check, just returning 200 is often enough.
    return {"status": "ok"}

####################################################### health #######################################################