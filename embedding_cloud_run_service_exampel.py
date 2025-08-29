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


This endpoint uses a cloud run service to generate embeddeings for semantic search.

Embeddings for semantic search are cached to improve performance.

"""



# Define a threshold for semantic similarity.

# A lower value means higher similarity. 0.5 is a reasonable starting point.

SIMILARITY_THRESHOLD = 0.55



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

print("******************************************************************************************")

print(f"Payload search field: {payload.search}")

print("******************************************************************************************")

if payload.search:



# The audience is the URL of the GCE service

gce_audience_url = settings.GCE_EMBED_URL # set to the cloud run service URL



# call the get_semantically_similar_repos function to generate an embedding

print("Starting embedding generation...")

try:

embedding = await generate_embedding(payload.search, gce_audience_url)

except Exception as e:

print(f"Error generating embedding: {e}")

raise HTTPException(status_code=500, detail="Error generating embedding. Check qwen-embedding-api cloud run service")

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

print("******************************************************************************************")

print("No search query, using standard non-semantic request")

print("******************************************************************************************")

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

print(f"Total items: {total_items}")

full_sql_query = cur.mogrify(data_query_sql, params).decode('utf-8')

print(f"Executing Data Query:\n{full_sql_query.replace('\n', ' ')}\n")

# Execute the data query only if there are items to fetch.

cur.execute(data_query_sql, params)

items = cur.fetchall()

else:

print("No items were fetched from the database.")

print("******************************************************************************************")

full_sql_query = cur.mogrify(count_query_sql, params).decode('utf-8')

print(f"Executed Count Query:\n{full_sql_query.replace('\n', ' ')}\n")

print("******************************************************************************************")



# Rebuild the items list to ensure all data types are JSON serializable

# This handles potential issues like Decimal types from the database.

cleaned_items = [RepoDetail(**item) for item in items]



total_pages = (total_items + payload.limit - 1) // payload.limit



# # ---- START: DIAGNOSTIC LOGGING ----

# # Add this block to inspect the data before it's returned.

# print("--- Starting Diagnostic Logging ---")

# if cleaned_items:

# # Inspect the first item in the list

# first_item = cleaned_items[0]

# print("Inspecting data types of the first item:")

# for key, value in first_item.items():

# print(f" - Key: '{key}', Value: '{value}', Type: {type(value)}")

# else:

# print("No items were fetched from the database.")

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