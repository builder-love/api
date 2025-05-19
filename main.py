from fastapi import FastAPI, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import urllib.parse

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

####################################################### get project metrics #######################################################

# Pydantic Models (Data Validation) - all projects view
class project_metrics(BaseModel):
    project_title: str
    latest_data_timestamp: str
    contributor_count: Optional[int]
    contributor_count_pct_change_over_4_weeks: Optional[float]
    repo_count: Optional[int]
    fork_count: Optional[int]
    fork_count_pct_change_over_4_weeks: Optional[float]
    stargaze_count: Optional[int]
    stargaze_count_pct_change_over_4_weeks: Optional[float]
    commit_count: Optional[int]
    commit_count_pct_change_over_4_weeks: Optional[float]
    watcher_count: Optional[int]
    watcher_count_pct_change_over_4_weeks: Optional[float]
    is_not_fork_ratio: Optional[float]
    is_not_fork_ratio_pct_change_over_4_weeks: Optional[float]
    project_rank: Optional[int]
    prior_4_weeks_project_rank: Optional[int]
    absolute_project_rank_change_over_4_weeks: Optional[int]
    rank_of_project_rank_change_over_4_weeks: Optional[int]
    quartile_bucket: Optional[int]
    project_rank_category: Optional[str]
    weighted_score_index: Optional[float]
    weighted_score_sma: Optional[float]
    prior_4_weeks_weighted_score: Optional[float]

# Pydantic Model for Organization Data
class ProjectOrganization(BaseModel):
    project_title: Optional[str] 
    project_organization_url: Optional[str]
    latest_data_timestamp: Optional[str]
    org_rank: Optional[int]
    org_rank_category: Optional[str]
    weighted_score_index: Optional[float]

# Pydantic Model for Repository Details
class RepoDetail(BaseModel):
    project_title: Optional[str]
    latest_data_timestamp: Optional[str]
    repo: Optional[str] 
    fork_count: Optional[int]
    stargaze_count: Optional[int]
    watcher_count: Optional[int]
    weighted_score_index: Optional[float] 
    repo_rank: Optional[int] 
    quartile_bucket: Optional[int]
    repo_rank_category: Optional[str]

class PaginatedRepoResponse(BaseModel):
    items: List[RepoDetail]
    total_items: int
    page: int
    limit: int
    total_pages: int

#######################################################
# New Endpoints for Project Search & Details from 'top_projects'
#######################################################

@app.get("/api/projects/search_top_projects", response_model=List[project_metrics]) # Using your existing model
async def search_top_projects_by_title(
    q: Optional[str] = Query(None, min_length=1, description="Search term for project title (case-insensitive)"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of results to return"),
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Searches for projects in the 'top_projects' view by project_title using a case-insensitive partial match.
    Returns a list of matching projects.
    Used for both autocomplete suggestions and displaying ambiguous search results.
    """
    if not q:
        return [] # Return empty list if query is empty or None

    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    try:
        with db.cursor() as cur:
            # The SQL query will search for project_title containing the query string (q)
            # ILIKE for case-insensitive search.
            # Orders by how early the match appears, then by title length, then by a popularity metric (e.g., stargaze_count)
            sql_query = """
                SELECT * FROM api.top_projects
                WHERE project_title ILIKE %s
                ORDER BY
                    CASE
                        WHEN project_title ILIKE %s THEN 1 -- Exact match (case-insensitive)
                        WHEN project_title ILIKE %s THEN 2 -- Starts with (case-insensitive)
                        ELSE 3
                    END,
                    weighted_score_index DESC NULLS LAST,
                    LENGTH(project_title)
                LIMIT %s;
            """
            # Parameters for ILIKE
            search_term_contains = f"%{q}%"
            search_term_exact = q # For exact ILIKE match
            search_term_starts_with = f"{q}%"

            cur.execute(sql_query, (search_term_contains, search_term_exact, search_term_starts_with, limit))
            results = cur.fetchall()
        return results
    except psycopg2.Error as e:
        print(f"Database error during project search: {e}")
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
    except Exception as e:
        print(f"Unexpected error during project search: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@app.get("/api/projects/details_from_top_projects/{project_title_url_encoded}", response_model=project_metrics) # Using your existing model
async def get_single_project_details_from_top_projects(
    project_title_url_encoded: str,
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves the full details for a single project from the 'top_projects' view
    by its URL-encoded project_title.
    Uses a case-insensitive match for project_title.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        # URL decode the project title
        project_title = urllib.parse.unquote(project_title_url_encoded)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid project title encoding: {e}")

    try:
        with db.cursor() as cur:
            # It's safer to use ILIKE if the exact casing isn't guaranteed or if names
            # fetched from search (which is ILIKE) are used directly for lookup.
            # However, if project_title is a strict unique key, '=' might be preferred.
            # Given search is ILIKE, using ILIKE here for consistency can be safer.
            sql_query = """
                SELECT * FROM api.top_projects
                WHERE project_title ILIKE %s;
            """
            cur.execute(sql_query, (project_title,))
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail=f"Project '{project_title}' not found in top_projects.")
        return result
    except HTTPException: # Re-raise 404 if already raised
        raise
    except psycopg2.Error as e:
        print(f"Database error fetching project '{project_title}': {e}")
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
    except Exception as e:
        print(f"Unexpected error fetching project '{project_title}': {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")


#######################################################
# Project-specific Organization Data
#######################################################

@app.get("/api/projects/{project_title_url_encoded}/top_organizations", response_model=List[ProjectOrganization])
async def get_top_5_organizations_for_project(
    project_title_url_encoded: str,
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves the top 5 organizations associated with a specific project
    from the 'top_5_organizations_by_project' view, based on its URL-encoded project_title.
    Matches project_title case-insensitively.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        project_title = urllib.parse.unquote(project_title_url_encoded)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid project title encoding: {e}")

    try:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur: # Use RealDictCursor for easy Pydantic mapping
            # 'project_title' in 'top_5_organizations_by_project' needs to match
            # the one from 'top_projects'.
            # the view already provides the "top 5" and is ordered,
            sql_query = """
                SELECT
                    project_title,
                    project_organization_url,
                    latest_data_timestamp,
                    org_rank,
                    org_rank_category,
                    weighted_score_index
                FROM api.top_5_organizations_by_project
                WHERE project_title ILIKE %s
                ORDER BY org_rank ASC; -- Ensure they are ordered by rank
            """
            cur.execute(sql_query, (project_title,))
            organizations = cur.fetchall()
            if not organizations:
                # It's okay if a project has no organizations, return empty list instead of 404
                return []
        return organizations
    except psycopg2.Error as e:
        print(f"Database error fetching organizations for project '{project_title}': {e}")
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
    except Exception as e:
        print(f"Unexpected error fetching organizations for project '{project_title}': {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

#######################################################
# Project-specific Repo Details
#######################################################
# Whitelist of columns that can be sorted to prevent SQL injection
VALID_SORT_COLUMNS_REPOS = {
    "repo": "repo",
    "fork_count": "fork_count",
    "stargaze_count": "stargaze_count",
    "watcher_count": "watcher_count",
    "weighted_score_index": "weighted_score_index",
    "repo_rank": "repo_rank",
    "quartile_bucket": "quartile_bucket",
    "repo_rank_category": "repo_rank_category",
    "latest_data_timestamp": "latest_data_timestamp" 
}

@app.get("/api/projects/{project_title_url_encoded}/repos", response_model=PaginatedRepoResponse)
async def get_project_repositories(
    project_title_url_encoded: str,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, min_length=1, description="Search term for repository name (case-insensitive)"),
    sort_by: Optional[str] = Query("repo_rank", description=f"Column to sort by. Allowed: {', '.join(VALID_SORT_COLUMNS_REPOS.keys())}"),
    sort_order: Optional[str] = Query("asc", pattern="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves paginated, searchable, and sortable repository details for a specific project
    from api.top_project_repos view.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        project_title = urllib.parse.unquote(project_title_url_encoded)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid project title encoding: {e}")

    # Validate sort_by column
    if sort_by and sort_by not in VALID_SORT_COLUMNS_REPOS:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by column. Allowed values: {', '.join(VALID_SORT_COLUMNS_REPOS.keys())}")
    
    db_sort_column = VALID_SORT_COLUMNS_REPOS.get(sort_by, "repo_rank") # Default sort
    db_sort_order = "ASC" if sort_order == "asc" else "DESC"

    base_query = "FROM api.top_project_repos WHERE project_title ILIKE %(project_title)s"
    count_query_sql = f"SELECT COUNT(*) {base_query}"
    data_query_sql_select = f"SELECT project_title, latest_data_timestamp, repo, fork_count, stargaze_count, watcher_count, weighted_score_index, repo_rank, quartile_bucket, repo_rank_category {base_query}"

    params = {"project_title": project_title}

    if search:
        # Ensure search is applied to the correct field, e.g., 'repo' (the repository name/identifier)
        data_query_sql_select += " AND repo ILIKE %(search_term)s"
        count_query_sql += " AND repo ILIKE %(search_term)s"
        params["search_term"] = f"%{search}%"
    
    # Add ORDER BY, LIMIT, OFFSET to the data query
    data_query_sql_select += f" ORDER BY {db_sort_column} {db_sort_order} NULLS LAST" # NULLS LAST can be useful
    data_query_sql_select += " LIMIT %(limit)s OFFSET %(offset)s"
    
    offset = (page - 1) * limit
    params["limit"] = limit
    params["offset"] = offset

    try:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get total count
            cur.execute(count_query_sql, {"project_title": project_title, "search_term": f"%{search}%" if search else "%"}) # Adjust params for count
            total_items = cur.fetchone()['count']

            # Get paginated data
            cur.execute(data_query_sql_select, params)
            items = cur.fetchall()
            
        total_pages = (total_items + limit - 1) // limit  # Ceiling division

        return {
            "items": items,
            "total_items": total_items,
            "page": page,
            "limit": limit,
            "total_pages": total_pages
        }
    except psycopg2.Error as e:
        print(f"Database error fetching repositories for project '{project_title}': {e}")
        raise HTTPException(status_code=500, detail="Database query error while fetching repositories.")
    except Exception as e:
        print(f"Unexpected error fetching repositories for project '{project_title}': {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred while fetching repositories.")



####################################################### end all projects #######################################################



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