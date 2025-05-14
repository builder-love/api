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
    repo_count: Optional[float]
    fork_count: Optional[float]
    stargaze_count: Optional[float]
    commit_count: Optional[float]
    contributor_count: Optional[float]
    watcher_count: Optional[float]
    is_not_fork_ratio: Optional[float]
    commit_count_pct_change_over_4_weeks: Optional[float]
    contributor_count_pct_change_over_4_weeks: Optional[float]
    fork_count_pct_change_over_4_weeks: Optional[float]
    stargaze_count_pct_change_over_4_weeks: Optional[float]
    watcher_count_pct_change_over_4_weeks: Optional[float]
    is_not_fork_ratio_pct_change_over_4_weeks: Optional[float]
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
                    repo_count,
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
                    repo_count,
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

####################################################### top 100 contributors #######################################################

# Pydantic Models (Data Validation) - top 100 contributors view
class top_100_contributors(BaseModel):
    contributor_login: str
    is_anon: Optional[bool]
    dominant_language: Optional[str]
    location: Optional[str]
    contributor_html_url: Optional[str]
    total_repos_contributed_to: Optional[int]
    total_contributions: Optional[int]
    contributions_to_og_repos: Optional[int]
    normalized_total_repo_quality_weighted_contribution_score_rank: Optional[int]
    followers_total_count: Optional[int]
    weighted_score_index: float
    quartile_bucket: Optional[int]
    contributor_rank: Optional[int]
    latest_data_timestamp: str

@app.get("/contributors/top100", response_model=List[top_100_contributors])
async def get_top_100_contributors(db: psycopg2.extensions.connection = Depends(get_db_connection)):
    """
    Retrieves the top 100 contributors from the api schema in postgres database.
    """
    if db is None: # Good check, though get_db_connection should raise exceptions
         raise HTTPException(status_code=503, detail="Database not connected")
    try:
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
            cur.execute("""
                SELECT 
                    contributor_login, 
                    is_anon, 
                    dominant_language,
                    location,
                    contributor_html_url,
                    total_repos_contributed_to,
                    total_contributions,
                    contributions_to_og_repos,
                    normalized_total_repo_quality_weighted_contribution_score_rank,
                    followers_total_count,
                    weighted_score_index,
                    quartile_bucket,
                    contributor_rank, 
                    latest_data_timestamp

                FROM api.top_100_contributors;
            """) 
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

####################################################### end top 100 contributors #######################################################

####################################################### health #######################################################

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    # This endpoint requires no authentication and simply returns 200 OK
    # You could add logic here later to check DB connections, etc., if needed
    # but for a basic uptime check, just returning 200 is often enough.
    return {"status": "ok"}

####################################################### health #######################################################