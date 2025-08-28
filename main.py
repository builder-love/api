from fastapi import FastAPI, Depends, HTTPException, status, Query, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional, Union, Dict
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import urllib.parse
from config import get_settings
from pgvector.psycopg2 import register_vector
import numpy as np
import httpx
import google.auth
import google.auth.transport.requests
from google.oauth2 import id_token

# This URL is only accessible from within Google Cloud
# we use this to get the internal IP of the GCE instance
METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience="

app = FastAPI()

# Get settings
settings = get_settings()

# API Key Authentication
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key"
        )
    return api_key

# This function is used to get the GCP ID token for the GCE instance
async def get_gcp_id_token(audience_url: str) -> str:
    """Fetches an OIDC ID token from the GCP metadata server."""
    headers = {"Metadata-Flavor": "Google"}
    async with httpx.AsyncClient() as client:
        # The request to the metadata server gets the token
        response = await client.get(METADATA_URL + audience_url, headers=headers)
        response.raise_for_status() # Ensure the request was successful
        return response.text

# Database Connection Function (Dependency)
def get_db_connection():
    conn = None
    try:
        print(f"Attempting DB connection to host={settings.DATABASE_HOST} db={settings.DATABASE_NAME} user={settings.DATABASE_USER}")
        conn = psycopg2.connect(
            host=settings.DATABASE_HOST,
            user=settings.DATABASE_USER,
            password=settings.DATABASE_PASSWORD,
            database=settings.DATABASE_NAME,
            cursor_factory=RealDictCursor,
        )
        print("DB connection successful.")
        register_vector(conn)
        yield conn
    except psycopg2.OperationalError as e:
        print(f"Database connection error (OperationalError): {e}")
        print(f"Failed connection details: host={settings.DATABASE_HOST}, user={settings.DATABASE_USER}, db={settings.DATABASE_NAME}")
        raise HTTPException(status_code=503, detail=f"Database connection error: {e}") from e
    except Exception as e:
        print(f"Unexpected error during DB connection attempt: {type(e).__name__} - {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error during DB connection: {type(e).__name__} - {e}") from e
    finally:
        if conn is not None:
            print("Closing database connection.")
            conn.close()
        else:
            print("No active database connection to close.")

# Helper function to get schema name
def get_schema_name(base_schema: str) -> str:
    return f"{base_schema}{settings.SCHEMA_SUFFIX}"

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

# Pydantic Models (Data Validation) - all projects view
class project_trend(BaseModel):
    project_title: str
    report_date: str
    latest_data_timestamp: str
    contributor_count: Optional[int]
    contributor_count_rank: Optional[int]
    contributor_count_pct_change_over_4_weeks: Optional[float]
    contributor_count_pct_change_over_4_weeks_rank: Optional[int]
    repo_count: Optional[int]
    fork_count: Optional[int]
    fork_count_pct_change_over_4_weeks: Optional[float]
    fork_count_rank: Optional[int]
    fork_count_pct_change_over_4_weeks_rank: Optional[int]
    stargaze_count: Optional[int]
    stargaze_count_pct_change_over_4_weeks: Optional[float]
    stargaze_count_rank: Optional[int]
    stargaze_count_pct_change_over_4_weeks_rank: Optional[int]
    commit_count: Optional[int]
    commit_count_pct_change_over_4_weeks: Optional[float]
    commit_count_rank: Optional[int]
    commit_count_pct_change_over_4_weeks_rank: Optional[int]
    watcher_count: Optional[int]
    watcher_count_pct_change_over_4_weeks: Optional[float]
    watcher_count_rank: Optional[int]
    watcher_count_pct_change_over_4_weeks_rank: Optional[int]
    is_not_fork_ratio: Optional[float]
    is_not_fork_ratio_pct_change_over_4_weeks: Optional[float]
    is_not_fork_ratio_rank: Optional[int]
    is_not_fork_ratio_pct_change_over_4_weeks_rank: Optional[int]
    quartile_project_rank: Optional[int]
    overall_project_rank: Optional[int]

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
    first_seen_timestamp: Optional[str]
    latest_data_timestamp: Optional[str]
    repo: Optional[str] 
    fork_count: Optional[int]
    stargaze_count: Optional[int]
    watcher_count: Optional[int]
    weighted_score_index: Optional[float] 
    repo_rank: Optional[int] 
    quartile_bucket: Optional[int]
    repo_rank_category: Optional[str]
    predicted_is_dev_tooling: Optional[bool]
    predicted_is_educational: Optional[bool]
    predicted_is_scaffold: Optional[bool]
    predicted_is_app: Optional[bool]
    predicted_is_infrastructure: Optional[bool]
    distance: Optional[float]

class PaginatedRepoResponse(BaseModel):
    items: List[RepoDetail]
    total_items: int
    page: int
    limit: int
    total_pages: int

# project specific repo symantic search - Pydantic model for the POST request body
class RepoRequestPayload(BaseModel):
    page: int = 1
    limit: int = 10
    sort_by: str = "repo_rank"
    sort_order: str = "asc"
    search: Optional[str] = Field(None, max_length=40) # The raw search query is now sent here

class ProjectOutliers(BaseModel):
    project_title: str
    report_date: str
    pct_change: Optional[float]
    current_value: Optional[Union[int, float]]
    previous_value: Optional[Union[int, float]]

#######################################################
# Endpoint for Project outliers
#######################################################

# Map the public API parameter to the actual, safe database column names
# This is a critical security step to prevent SQL injection
METRIC_MAP = {
    "fork_count": {
        "pct_change_col": "fork_count_pct_change_over_4_weeks",
        "current_col": "current_fork_count",
        "previous_col": "previous_fork_count"
    },
    "commit_count": {
        "pct_change_col": "commit_count_pct_change_over_4_weeks",
        "current_col": "current_commit_count",
        "previous_col": "previous_commit_count"
    },
    "stargaze_count": {
        "pct_change_col": "stargaze_count_pct_change_over_4_weeks",
        "current_col": "current_stargaze_count",
        "previous_col": "previous_stargaze_count"
    },
    "watcher_count": {
        "pct_change_col": "watcher_count_pct_change_over_4_weeks",
        "current_col": "current_watcher_count",
        "previous_col": "previous_watcher_count"
    },
    "is_not_fork_ratio": {
        "pct_change_col": "is_not_fork_ratio_pct_change_over_4_weeks",
        "current_col": "current_is_not_fork_ratio",
        "previous_col": "previous_is_not_fork_ratio"
    },
    "contributor_count": {
        "pct_change_col": "contributor_count_pct_change_over_4_weeks",
        "current_col": "current_contributor_count",
        "previous_col": "previous_contributor_count"
    },
    "repo_count": {
        "pct_change_col": "repo_count_pct_change_over_4_weeks",
        "current_col": "current_repo_count",
        "previous_col": "previous_repo_count"
    },
}

@app.get("/projects/outliers", response_model=List[ProjectOutliers], dependencies=[Depends(get_api_key)])
async def get_project_outliers(
    limit: int = Query(10, ge=1, le=100, description="Maximum number of results to return"),
    metric: str = Query(..., description="The metric to rank by (e.g., 'fork_count')"),
    include_forks: bool = Query(False, description="Set to true to include history from forked repos for commits and contributors"),
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Dynamically builds a query to return outlier metrics for the respecitve outlier leaderboards for projects in the 'project_outliers' view.
    Returns a list of matching projects.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    # Validate the incoming metric parameter
    if metric not in METRIC_MAP:
        raise HTTPException(status_code=400, detail="Invalid metric specified")
    
    # Get the safe column names from the map
    selected_metric = METRIC_MAP[metric]
    pct_change_col = selected_metric["pct_change_col"]
    current_col = selected_metric["current_col"]
    previous_col = selected_metric["previous_col"]

    # Dynamically select the table based on the include_forks parameter
    table_name = f"{get_schema_name('api')}.project_outliers"
    if metric in ["commit_count", "contributor_count"] and not include_forks:
        table_name = f"{get_schema_name('api')}.project_outliers_no_forks"
    
    try:
        with db.cursor() as cur:
            # Build the query dynamically based on the limit parameter
            query = f"""
                        SELECT
                            project_title,
                            report_date,
                            {pct_change_col} AS pct_change,
                            {current_col} AS current_value,
                            {previous_col} AS previous_value

                        FROM
                            {table_name}
                        WHERE
                            {pct_change_col} IS NOT NULL
                        ORDER BY
                            {pct_change_col} DESC
                        LIMIT {limit};
                    """
            # print(f"Executing query: {query}")
            # execute the query and return the results
            cur.execute(query)
            results = cur.fetchall()

            if not results:
                raise HTTPException(status_code=404, detail=f"No results found when querying for {metric} project outliers.")
            
        return results
    except HTTPException as http_exc:
        # This will catch the 404 if not results, and let it pass through directly
        raise http_exc
    except psycopg2.Error as e:
        print(f"Database error during project search: {e}")
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
    except Exception as e:
        print(f"Unexpected error during project search: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

#######################################################
# Endpoint for Project Search from 'top_projects'
#######################################################

@app.get("/api/projects/search_top_projects", response_model=List[project_metrics], dependencies=[Depends(get_api_key)])
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
            sql_query = f"""
                SELECT * FROM {get_schema_name('api')}.top_projects
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

#######################################################
# Endpoint for Project details from 'top_projects'
#######################################################

@app.get("/api/projects/details_from_top_projects/{project_title_url_encoded}", response_model=project_metrics, dependencies=[Depends(get_api_key)])
async def get_single_project_details_from_top_projects(
    project_title_url_encoded: str,
    include_forks: bool = Query(False, description="Set to true to include history from forked repos"),
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves the full details for a single project from the 'top_projects' view
    by its URL-encoded project_title.
    Uses a case-insensitive match for project_title.
    Retrieves the full details for a single project from either 'top_projects' or
    'top_projects_no_forks' view depending on the 'include_forks' parameter.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    # confirm include_forks exists and is a boolean
    if include_forks is not None and not isinstance(include_forks, bool):
        raise HTTPException(status_code=400, detail="include_forks must be a boolean")
    
    # Dynamically select the view name based on the parameter
    # Default (include_forks=False) uses no forks view
    table_name = (
        f"{get_schema_name('api')}.top_projects"
        if include_forks
        else f"{get_schema_name('api')}.top_projects_no_forks"
    )

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
            sql_query = f"""
                SELECT * FROM {table_name}
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
# Endpoint for Project trends from 'top_projects_trend'
#######################################################

@app.get("/api/projects/trends_from_top_projects/{project_title_url_encoded}", response_model=List[project_trend], dependencies=[Depends(get_api_key)])
async def get_project_trends_from_top_projects(
    project_title_url_encoded: str,
    include_forks: bool = Query(False, description="Set to true to include history from forked repos"),
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves the full trends for a single project from the 'top_projects_trend' view
    by its URL-encoded project_title.
    Uses a case-insensitive match for project_title.
    The view queried depends on 'include_forks'.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        # URL decode the project title
        project_title = urllib.parse.unquote(project_title_url_encoded)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid project title encoding: {e}")

    # confirm include_forks exists and is a boolean
    if include_forks is not None and not isinstance(include_forks, bool):
        raise HTTPException(status_code=400, detail="include_forks must be a boolean")
    
    # Dynamically select the view name based on the parameter
    # Default (include_forks=False) uses no forks view
    table_name = (
        f"{get_schema_name('api')}.top_projects_trend"
        if include_forks
        else f"{get_schema_name('api')}.top_projects_trend_no_forks"
    )

    try:
        with db.cursor() as cur:
            # It's safer to use ILIKE if the exact casing isn't guaranteed or if names
            # fetched from search (which is ILIKE) are used directly for lookup.
            # However, if project_title is a strict unique key, '=' might be preferred.
            # Given search is ILIKE, using ILIKE here for consistency can be safer.
            sql_query = f"""
                SELECT * FROM {table_name}
                WHERE project_title ILIKE %s
                ORDER BY report_date DESC;
            """
            cur.execute(sql_query, (project_title,))
            results = cur.fetchall()
            if not results:
                return []
                # raise HTTPException(status_code=404, detail=f"Project '{project_title}' not found in top_projects_trend.")
        return results
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

@app.get("/api/projects/{project_title_url_encoded}/top_organizations", response_model=List[ProjectOrganization], dependencies=[Depends(get_api_key)])
async def get_top_5_organizations_for_project(
    project_title_url_encoded: str,
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves the top 5 organizations associated with a specific project
    from the 'top_5_organizations_by_project' view, based on its URL-encoded project_title.
    Matches project_title case-insensitively.
    """
    print(f"Getting top 5 organizations for project: {project_title_url_encoded}\n")

    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        project_title = urllib.parse.unquote(project_title_url_encoded)
        print(f"URL encoded project title value resolved to: {project_title}\n")

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid project title encoding: {e}")

    try:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur: # Use RealDictCursor for easy Pydantic mapping
            # 'project_title' in 'top_5_organizations_by_project' needs to match
            # the one from 'top_projects'.
            # the view already provides the "top 5" and is ordered,
            sql_query = f"""
                SELECT
                    project_title,
                    project_organization_url,
                    latest_data_timestamp,
                    org_rank,
                    org_rank_category,
                    weighted_score_index
                FROM {get_schema_name('api')}.top_5_organizations_by_project
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
    "first_seen_timestamp": "first_seen_timestamp",
    "fork_count": "fork_count",
    "stargaze_count": "stargaze_count",
    "watcher_count": "watcher_count",
    "repo_rank": "repo_rank",
    "repo_rank_category": "repo_rank_category",
    "predicted_is_dev_tooling": "predicted_is_dev_tooling",
    "predicted_is_educational": "predicted_is_educational",
    "predicted_is_scaffold": "predicted_is_scaffold"
}

# --- In-Memory Cache ---
# Use a global dictionary as an in-memory cache.
# This cache will store generated embeddings to avoid re-calling the embedding service
# for the same search query. If we introduce a multiple server processes,
# we would use a shared cache like Redis or Memcached for this.
embedding_cache: Dict[str, List[float]] = {}

# embedding generation and semantic search orchestration logic
async def generate_embedding(search_text: str, gce_audience_url: str) -> List[float]:
    """
    Calls the dedicated Qwen embedding service on Cloud Run.
    """

    # Check the cache first...
    if search_text in embedding_cache:
        print(f"Cache HIT for Qwen: '{search_text}'")
        return embedding_cache[search_text]

    # Get a GCP identity token to securely call the other Cloud Run service
    creds, project = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    identity_token = id_token.fetch_id_token(auth_req, gce_audience_url)

    # Set the headers for the request
    headers = {
        "Authorization": f"Bearer {identity_token}",
        "Content-Type": "application/json"
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            gce_audience_url,
            headers=headers,
            json={"text": search_text},
            timeout=15.0
        )
        response.raise_for_status()
        embedding = response.json()["embedding"]

    # Store in cache and return
    embedding_cache[search_text] = embedding
    return embedding

@app.post("/api/projects/{project_title_url_encoded}/repos", response_model=PaginatedRepoResponse, dependencies=[Depends(get_api_key)])
async def get_project_repositories_with_semantic_filter(
    project_title_url_encoded: str,
    payload: RepoRequestPayload,
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves paginated, searchable, and sortable repository details for a specific project
    from api.top_projects_repos view.
    
    This endpoint uses a GCE host to generate embeddeings for semantic search.
    Embeddings for semantic search are cached to improve performance.
    """

    # Define a threshold for semantic similarity.
    # A lower value means higher similarity. 0.5 is a reasonable starting point.
    SIMILARITY_THRESHOLD = 0.45

    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        project_title = urllib.parse.unquote(project_title_url_encoded)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid project title encoding: {e}")

    # Validate sort_by column
    if payload.sort_by not in VALID_SORT_COLUMNS_REPOS:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by column. Allowed values: {', '.join(VALID_SORT_COLUMNS_REPOS.keys())}")
    
    # define variables we will use to construct the SQL query
    db_sort_column = VALID_SORT_COLUMNS_REPOS[payload.sort_by] # Default sort
    db_sort_order = "ASC" if payload.sort_order == "asc" else "DESC"
    params = {"project_title": project_title} # universal params 

    # The fields to select from the table.
    select_fields = """
        tpr.project_title, tpr.first_seen_timestamp, tpr.latest_data_timestamp, 
        tpr.repo, tpr.fork_count, tpr.stargaze_count, tpr.watcher_count, 
        tpr.weighted_score_index, tpr.repo_rank, tpr.quartile_bucket, 
        tpr.repo_rank_category, tpr.predicted_is_dev_tooling, 
        tpr.predicted_is_educational, tpr.predicted_is_scaffold, 
        tpr.predicted_is_app, tpr.predicted_is_infrastructure
    """

    # ---- embedding generation and semantic search orchestration logic ----
    # if the search query is not empty, we need to generate an embedding and get repo URLs from the database
    if payload.search:

        # The audience is the URL of the GCE service
        gce_audience_url = settings.GCE_EMBED_URL # set to the cloud run service URL

        # call the get_semantically_similar_repos function to generate an embedding
        print("Starting embedding generation...")
        embedding = await generate_embedding(payload.search, gce_audience_url)
        print("Embeddings generated...")
        params["embedding"] = np.array(embedding)
        # Similarity threshold
        params["similarity_threshold"] = SIMILARITY_THRESHOLD

        # The JOIN is only needed for semantic search
        from_clause = f"""
            FROM {get_schema_name('api')}.top_projects_repos AS tpr
            JOIN {get_schema_name('api')}.project_repo_embeddings AS pre
                 ON tpr.repo = pre.repo
        """
        # Filters by the similarity score.
        where_sql = "tpr.project_title ILIKE %(project_title)s AND (pre.corpus_embedding <=> %(embedding)s) < %(similarity_threshold)s"
        
        # Add the distance calculation to the SELECT statement.
        select_fields += ", (pre.corpus_embedding <=> %(embedding)s) as distance"
        
        # Default sort for semantic search is by distance (relevance).
        # The user can override this by selecting another column.
        order_by_sql = f"ORDER BY {db_sort_column} {db_sort_order} NULLS LAST"

    else:
        # --- This path is for STANDARD, non-semantic requests ---
        from_clause = f"FROM {get_schema_name('api')}.top_projects_repos tpr"
        where_sql = "tpr.project_title ILIKE %(project_title)s"
        select_fields += ", NULL as distance"
        # The ORDER BY is based on the user's sort selection
        order_by_sql = f"ORDER BY {db_sort_column} {db_sort_order} NULLS LAST"
    
    # Construct the full SQL queries for counting and fetching data.
    count_query_sql = f"SELECT COUNT(*) {from_clause} WHERE {where_sql}"
    data_query_sql = f"""
        SELECT {select_fields}
        {from_clause}
        WHERE {where_sql}
        {order_by_sql}
        LIMIT %(limit)s OFFSET %(offset)s
    """

    # execute the queries
    try:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Set a timeout for this transaction that is less than the Vercel timeout (10 seconds)
            cur.execute("SET LOCAL statement_timeout = '8000';") # 8000ms = 8 seconds
            # Execute the count query using the 'params' dictionary we've been building.
            cur.execute(count_query_sql, params)
            total_items_result = cur.fetchone()
            total_items = total_items_result['count'] if total_items_result else 0

            items = []
            if total_items > 0:
                # Set pagination parameters using the 'payload' object
                offset = (payload.page - 1) * payload.limit
                params["limit"] = payload.limit
                params["offset"] = offset
                # print(f"Total items: {total_items}")
                # full_sql_query = cur.mogrify(data_query_sql, params).decode('utf-8')
                # print(f"Executing Data Query:\n{full_sql_query.replace('\n', ' ')}\n")
                # Execute the data query only if there are items to fetch.
                cur.execute(data_query_sql, params)
                items = cur.fetchall()

        # Rebuild the items list to ensure all data types are JSON serializable
        # This handles potential issues like Decimal types from the database.
        cleaned_items = [RepoDetail(**item) for item in items]

        total_pages = (total_items + payload.limit - 1) // payload.limit

        # # ---- START: DIAGNOSTIC LOGGING ----
        # # Add this block to inspect the data before it's returned.
        # print("--- Starting Diagnostic Logging ---")
        # if cleaned_items:
        #     # Inspect the first item in the list
        #     first_item = cleaned_items[0]
        #     print("Inspecting data types of the first item:")
        #     for key, value in first_item.items():
        #         print(f"  - Key: '{key}', Value: '{value}', Type: {type(value)}")
        # else:
        #     print("No items were fetched from the database.")
        # print("--- End of Diagnostic Logging ---")

        # Return the final paginated response using values from the payload.
        return PaginatedRepoResponse(
            items=cleaned_items,
            total_items=total_items,
            page=payload.page,
            limit=payload.limit,
            total_pages=total_pages
        )
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

@app.get("/projects/top50-trend", response_model=List[top_50_projects_trend], dependencies=[Depends(get_api_key)])
async def get_top_50_trend(
        include_forks: bool = Query(False, description="Set to true to include history from forked repos"),
        db: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves the top 50 projects by weighted score index from the api schema in postgres database.
    """
    if db is None: # Good check, though get_db_connection should raise exceptions
         raise HTTPException(status_code=503, detail="Database not connected")
    
    # confirm include_forks exists and is a boolean
    if include_forks is not None and not isinstance(include_forks, bool):
        raise HTTPException(status_code=400, detail="include_forks must be a boolean")
    
    # Dynamically select the view name based on the parameter
    # Default (include_forks=False) uses no forks view
    table_name = (
        f"{get_schema_name('api')}.top_50_projects_trend"
        if include_forks
        else f"{get_schema_name('api')}.top_50_projects_trend_no_forks"
    )

    try:
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
            cur.execute(f"""
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

                FROM {table_name};
            """) 
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/projects/top50-trend/{project_title}", response_model=top_50_projects_trend, dependencies=[Depends(get_api_key)])
async def get_top_50_trend_project(
    project_title: str, 
    include_forks: bool = Query(False, description="Set to true to include history from forked repos"),
    db: psycopg2.extensions.connection = Depends(get_db_connection)
):
     """Retrieves a single project by project_title."""
     try:
        if db is None: # Good check, though get_db_connection should raise exceptions
            raise HTTPException(status_code=503, detail="Database not connected")

        # confirm include_forks exists and is a boolean
        if include_forks is not None and not isinstance(include_forks, bool):
            raise HTTPException(status_code=400, detail="include_forks must be a boolean")
        
        # Dynamically select the view name based on the parameter
        # Default (include_forks=False) uses no forks view
        table_name = (
            f"{get_schema_name('api')}.top_50_projects_trend"
            if include_forks
            else f"{get_schema_name('api')}.top_50_projects_trend_no_forks"
        )

        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
             cur.execute(f"""
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

                FROM {table_name}
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

####################################################### contributors #######################################################


###############################
## top 100 contributors
###############################
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

@app.get("/contributors/top100", response_model=List[top_100_contributors], dependencies=[Depends(get_api_key)])
async def get_top_100_contributors(db: psycopg2.extensions.connection = Depends(get_db_connection)):
    """
    Retrieves the top 100 contributors from the api schema in postgres database.
    """
    if db is None: # Good check, though get_db_connection should raise exceptions
         raise HTTPException(status_code=503, detail="Database not connected")
    try:
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
            cur.execute(f"""
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

                FROM {get_schema_name('api')}.top_100_contributors;
            """) 
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
###########################################
## count of contributors by language trend
###########################################
# Pydantic Models (Data Validation) - count of contributors by language trend view
class count_of_contributors_by_language_trend(BaseModel):
    dominant_language: str
    developer_count: int
    latest_data_timestamp: str

@app.get("/contributors/language_trend", response_model=List[count_of_contributors_by_language_trend], dependencies=[Depends(get_api_key)])
async def get_count_of_contributors_by_language_trend(db: psycopg2.extensions.connection = Depends(get_db_connection)):
    """
    Retrieves the count of contributors by language trend from the api schema in postgres database.
    """
    if db is None: # Good check, though get_db_connection should raise exceptions
         raise HTTPException(status_code=503, detail="Database not connected")
    try:
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    dominant_language,
                    developer_count,
                    latest_data_timestamp

                FROM {get_schema_name('api')}.contributor_count_by_language;
            """) 
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/contributors/language_trend/top1000", response_model=List[count_of_contributors_by_language_trend], dependencies=[Depends(get_api_key)])
async def get_count_of_contributors_by_language_trend_top1000(db: psycopg2.extensions.connection = Depends(get_db_connection)):
    """
    Retrieves the count of contributors by language trend from the api schema in postgres database. Filtered to top 1000 contributors.
    """
    if db is None: # Good check, though get_db_connection should raise exceptions
         raise HTTPException(status_code=503, detail="Database not connected")
    try:
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    dominant_language,
                    developer_count,
                    latest_data_timestamp

                FROM {get_schema_name('api')}.top_1000_contributor_count_by_language;
            """) 
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

####################################################### end contributors #######################################################

####################################################### health #######################################################

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    # This endpoint requires no authentication and simply returns 200 OK
    # You could add logic here later to check DB connections, etc., if needed
    # but for a basic uptime check, just returning 200 is often enough.
    return {"status": "ok"}

####################################################### end health #######################################################


####################################################### testing #######################################################

# Pydantic Models (Data Validation) - test rec count from staging schema
class repo_count_test(BaseModel):
    rec_count: int

@app.get("/repos/count_test", response_model=List[repo_count_test], dependencies=[Depends(get_api_key)])
async def get_repo_count_test(db: psycopg2.extensions.connection = Depends(get_db_connection)):
    """
    Retrieves the count of repos from the api schema in postgres database.
    """
    if db is None: # Good check, though get_db_connection should raise exceptions
         raise HTTPException(status_code=503, detail="Database not connected")
    try:
        # Use 'db' (the connection object) directly to get a cursor
        with db.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    rec_count

                FROM {get_schema_name('api')}.active_distinct_repo_count;
            """) 
            results = cur.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

####################################################### end testing #######################################################