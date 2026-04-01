from fastapi import FastAPI, HTTPException
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime
import json
import hashlib
import os
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from mangum import Mangum

load_dotenv()

# ── DynamoDB setup ─────────────────────────────────────────────────────────────

dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.getenv("DYNAMODB_REGION", "us-east-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
)

hash_keys_table = dynamodb.Table("hash_keys")  # PK: business_key (String)
scraped_data_table = dynamodb.Table("scraped_data")  # PK: hash_key (String)

_root_path = "/Prod" if os.environ.get("AWS_LAMBDA_FUNCTION_NAME") else ""

app = FastAPI(
    title="Ghostie Data Retrieval API",
    description=(
        "Retrieves collected data for a business and uses hashing to "
        "detect if data has changed since last retrieval."
    ),
    version="2.0.0",
    root_path=_root_path,
    servers=[{"url": _root_path}] if _root_path else [{"url": "/"}],
    docs_url=None,       # disable default /docs — we serve a custom one below
    redoc_url=None,      # disable default /redoc
    openapi_url=None,    # disable default /openapi.json
)


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui():
    # Use a relative URL so the browser resolves it correctly regardless of
    # the API Gateway stage prefix (e.g. /Prod/docs → /Prod/openapi.json).
    # Mangum doesn't reliably populate root_path, so we avoid relying on it.
    return get_swagger_ui_html(
        openapi_url="openapi.json",
        title=app.title,
    )


@app.get("/openapi.json", include_in_schema=False)
async def custom_openapi():
    return JSONResponse(get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        servers=[{"url": _root_path}] if _root_path else [{"url": "/"}]
    ))


class StoreRequest(BaseModel):
    business_name: str
    location: str
    category: str
    collected_at: str
    news_count: int = 0
    review_count: int = 0
    data: list


# ── Helpers ────────────────────────────────────────────────────────────────────

def floats_to_decimals(obj):
    """Recursively convert floats to Decimal before writing to DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: floats_to_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [floats_to_decimals(i) for i in obj]
    return obj


def decimals_to_floats(obj):
    """Recursively convert Decimals back to float/int when reading from DynamoDB."""
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: decimals_to_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decimals_to_floats(i) for i in obj]
    return obj


def make_business_key(business_name: str, location: str, category: str) -> str:
    """Create a consistent lookup key for a business."""
    return f"{business_name.lower().strip()}_{location.lower().strip()}_{category.lower().strip()}"


def compute_hash(data: list) -> str:
    """Generate a SHA-256 hash fingerprint of the data."""
    data_string = json.dumps(data, sort_keys=True)
    return hashlib.sha256(data_string.encode()).hexdigest()


# ── DynamoDB: hash_keys table ──────────────────────────────────────────────────

def get_stored_hash_entry(business_key: str) -> dict | None:
    """Fetch the stored hash entry for a business from DynamoDB."""
    try:
        response = hash_keys_table.get_item(Key={"business_key": business_key})
        return response.get("Item")
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"DynamoDB error (hash_keys): {e.response['Error']['Message']}")


def save_hash_entry(business_key: str, hash_key: str, business_name: str, location: str, category: str):
    """Write or update the hash entry for a business in DynamoDB."""
    try:
        hash_keys_table.put_item(Item={
            "business_key":  business_key,
            "hash_key":      hash_key,
            "updated_at":    datetime.utcnow().isoformat(),
            "business_name": business_name,
            "location":      location,
            "category":      category,
        })
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"DynamoDB error (hash_keys put): {e.response['Error']['Message']}")


# ── DynamoDB: scraped_data table ───────────────────────────────────────────────

def get_scraped_data_by_hash(hash_key: str) -> dict | None:
    """Fetch a specific dataset by its hash key."""
    try:
        response = scraped_data_table.get_item(Key={"hash_key": hash_key})
        item = response.get("Item")
        return decimals_to_floats(item) if item else None
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"DynamoDB error (scraped_data): {e.response['Error']['Message']}")


def get_latest_scraped_data(business_key: str) -> dict | None:
    """
    Fetch the most recent dataset for a business.
    Scans the scraped_data table filtering by business_key, then picks the latest
    by collected_at. (A GSI on business_key + collected_at would be more efficient
    at scale, but a Scan is fine for this lab project.)
    """
    try:
        response = scraped_data_table.scan(
            FilterExpression=Attr("business_key").eq(business_key)
        )
        items = response.get("Items", [])
        if not items:
            return None
        # Sort descending by collected_at and return the newest
        latest = max(items, key=lambda x: x.get("collected_at", ""))
        return decimals_to_floats(latest)
    except ClientError as e:
        raise HTTPException(
            status_code=500,
            detail=f"DynamoDB error (scraped_data scan): {e.response['Error']['Message']}"
        )


def save_scraped_data(hash_key: str, business_key: str, payload: StoreRequest):
    """Write a new dataset into the scraped_data table."""
    try:
        # DynamoDB rejects Python floats — convert the entire data list first
        clean_data = floats_to_decimals(payload.data)
        scraped_data_table.put_item(Item={
            "hash_key":      hash_key,
            "business_key":  business_key,
            "business_name": payload.business_name,
            "location":      payload.location,
            "category":      payload.category,
            "collected_at":  payload.collected_at,
            "news_count":    payload.news_count,
            "review_count":  payload.review_count,
            "data":          clean_data,
        })
    except ClientError as e:
        raise HTTPException(
            status_code=500,
            detail=f"DynamoDB error (scraped_data put): {e.response['Error']['Message']}"
        )


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "service": "Ghostie Data Retrieval API",
        "version": "2.0.0",
        "status":  "running",
        "endpoints": {
            "GET  /retrieve":             "Retrieve latest data for a business (with hash comparison)",
            "GET  /retrieve/{hash_key}":  "Retrieve a specific dataset by hash key",
            "POST /store":                "Store new collected data (called by Data Collection service)",
            "GET  /health":               "Health check",
        }
    }


@app.get("/health")
def health():
    """Health check — also verifies DynamoDB connectivity."""
    try:
        # Light-weight check: just describe the table status
        status = hash_keys_table.table_status
        return {
            "status":    "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "dynamodb":  status,   # should be "ACTIVE"
        }
    except ClientError as e:
        return {
            "status":    "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "error":     e.response["Error"]["Message"],
        }


@app.post("/store")
def store(payload: StoreRequest):
    """
    Store newly collected data into DynamoDB.
    Called by the Data Collection service after it finishes scraping.

    Body (JSON):
        business_name, location, category, collected_at,
        news_count, review_count, data (list)
    """
    if not payload.data:
        raise HTTPException(status_code=400, detail="'data' field must be a non-empty list.")

    hash_key = compute_hash(payload.data)
    business_key = make_business_key(payload.business_name, payload.location, payload.category)

    save_scraped_data(hash_key, business_key, payload)

    return {
        "status":        "STORED",
        "hash_key":      hash_key,
        "business_key":  business_key,
        "total_results": len(payload.data),
    }


@app.get("/companies")
def list_companies():
    """List all unique companies that have ever been collected."""
    try:
        response = hash_keys_table.scan()
        items = response.get("Items", [])
        return {
            "count":     len(items),
            "companies": items,
        }
    except ClientError as e:
        raise HTTPException(
            status_code=500,
            detail=f"DynamoDB error (hash_keys scan): {e.response['Error']['Message']}"
        )


@app.get("/retrieve")
def retrieve(business_name: str, location: str, category: str):
    """
    Retrieve the latest collected data for a business.

    Compares the hash of the current data against the previously stored hash:
    - If identical  → returns NO NEW DATA (use cached analytical outputs)
    - If different  → returns NEW DATA with full payload (run fresh analysis)

    Query params:
        business_name : e.g. "Subway"
        location      : e.g. "Sydney"
        category      : e.g. "restaurant"
    """
    if not business_name or not location or not category:
        raise HTTPException(status_code=400, detail="business_name, location, and category are all required.")

    business_key = make_business_key(business_name, location, category)

    # Get the most recent dataset for this business from scraped_data table
    collected = get_latest_scraped_data(business_key)
    if collected is None:
        raise HTTPException(
            status_code=404,
            detail=f"No collected data found for '{business_name}' in '{location}'. Run POST /store first."
        )

    current_data = collected.get("data", [])
    if not current_data:
        raise HTTPException(status_code=404, detail="Record exists in DynamoDB but contains no data.")

    # Use the hash_key already stored in the record (avoids float→Decimal→int
    # round-trip mismatch that would produce a different hash on recompute)
    current_hash = collected.get("hash_key") or compute_hash(current_data)

    # Compare against the stored hash in hash_keys table
    stored_entry = get_stored_hash_entry(business_key)
    stored_hash = stored_entry.get("hash_key") if stored_entry else None

    if stored_hash == current_hash:
        # ── NO NEW DATA ────────────────────────────────────────────────────────
        return {
            "status":        "NO NEW DATA",
            "hash_key":      current_hash,
            "business_name": business_name,
            "location":      location,
            "category":      category,
            "message":       "Data has not changed since last retrieval. Use cached analytical outputs.",
        }

    else:
        # ── NEW DATA ───────────────────────────────────────────────────────────
        # Update the hash_keys table with the new hash
        save_hash_entry(business_key, current_hash, business_name, location, category)

        return {
            "status":        "NEW DATA",
            "hash_key":      current_hash,
            "business_name": business_name,
            "location":      location,
            "category":      category,
            "total_results": len(current_data),
            "news_count":    collected.get("news_count", 0),
            "review_count":  collected.get("review_count", 0),
            "collected_at":  collected.get("collected_at", ""),
            "data":          current_data,
        }


@app.get("/retrieve/{hash_key}")
def retrieve_by_hash(hash_key: str):
    """
    Retrieve a specific version of data by its hash key.
    Used to fetch a previously seen dataset by its fingerprint.
    """
    item = get_scraped_data_by_hash(hash_key)
    if item is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for hash key '{hash_key}'."
        )

    data = item.get("data", [])
    return {
        "status":        "FOUND",
        "hash_key":      hash_key,
        "business_name": item.get("business_name", ""),
        "location":      item.get("location", ""),
        "category":      item.get("category", ""),
        "collected_at":  item.get("collected_at", ""),
        "total_results": len(data),
        "data":          data,
    }


# ── Lambda handler (Mangum) ─────────────────────────────────────────────────────
handler = Mangum(app)

# ── Run locally ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("DataRetrieval:app", host="0.0.0.0", port=8001, reload=True)
