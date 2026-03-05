from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import json
import os
import hashlib
import glob

# ── Local storage paths ───────────────────────────────────────────────────────
# These point to the same folder that Data Collection saves into.
# When DynamoDB is ready, swap these file reads for boto3 DynamoDB queries.
COLLECTED_DATA_DIR = "../Ghostie_data-collection/collected_data"  # path to data collection output
HASH_STORE_FILE    = "hash_store.json"   # tracks latest hash per business

app = FastAPI(
    title="Ghostie Data Retrieval API",
    description="Retrieves collected data for a business and uses hashing to detect if data has changed since last retrieval.",
    version="1.0.0"
)


# ── Hash store helpers ────────────────────────────────────────────────────────
# The hash store is a simple JSON file that maps a business key to its latest hash.
# Format: { "subway_sydney_restaurant": { "hash_key": "abc123...", "updated_at": "..." } }
# TODO: replace with DynamoDB hash_keys table when Do In Kim sets up the DB.

def load_hash_store() -> dict:
    """Load the hash store from disk."""
    if not os.path.exists(HASH_STORE_FILE):
        return {}
    with open(HASH_STORE_FILE, "r") as f:
        return json.load(f)


def save_hash_store(store: dict):
    """Save the hash store to disk."""
    with open(HASH_STORE_FILE, "w") as f:
        json.dump(store, f, indent=2)


def make_business_key(business_name: str, location: str, category: str) -> str:
    """Create a consistent lookup key for a business."""
    return f"{business_name.lower().strip()}_{location.lower().strip()}_{category.lower().strip()}"


def compute_hash(data: list) -> str:
    """
    Generate a SHA-256 hash fingerprint of the data.
    If the data hasn't changed since last time, the hash will be identical.
    """
    data_string = json.dumps(data, sort_keys=True)
    return hashlib.sha256(data_string.encode()).hexdigest()


# ── Data loader ───────────────────────────────────────────────────────────────

def load_latest_data(business_name: str, location: str, category: str) -> dict | None:
    """
    Find the most recently saved JSON file for this business from the
    Data Collection service output folder.
    TODO: replace with DynamoDB scraped_data table query when DB is ready.
    """
    safe_name     = business_name.lower().replace(" ", "_")
    safe_location = location.lower().replace(" ", "_")

    # Look for files matching this business + location
    pattern = os.path.join(COLLECTED_DATA_DIR, f"{safe_name}_{safe_location}_*.json")
    matches = glob.glob(pattern)

    if not matches:
        return None

    # Return the most recently created file
    latest_file = max(matches, key=os.path.getmtime)
    with open(latest_file, "r") as f:
        return json.load(f)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "service": "Ghostie Data Retrieval API",
        "version": "1.0.0",
        "status":  "running",
        "endpoints": {
            "GET /retrieve":             "Retrieve data for a business (with hash comparison)",
            "GET /retrieve/{hash_key}":  "Retrieve data by a specific hash key",
            "GET /health":               "Health check",
        }
    }


@app.get("/health")
def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/retrieve")
def retrieve(business_name: str, location: str, category: str):
    """
    Retrieve the latest collected data for a business.

    Compares the hash of the current data against the previously stored hash:
    - If identical  → returns NO NEW DATA (Stefan uses cached analytical outputs)
    - If different  → returns NEW DATA with full payload (Stefan runs fresh analysis)

    Query params:
        business_name : e.g. "Subway"
        location      : e.g. "Sydney"
        category      : e.g. "restaurant"
    """

    if not business_name or not location or not category:
        raise HTTPException(status_code=400, detail="business_name, location and category are all required")

    # Load the latest collected data for this business
    collected = load_latest_data(business_name, location, category)

    if collected is None:
        raise HTTPException(
            status_code=404,
            detail=f"No collected data found for '{business_name}' in '{location}'. Run POST /collect first."
        )

    current_data = collected.get("data", [])
    if not current_data:
        raise HTTPException(status_code=404, detail="Collected file exists but contains no data.")

    # Compute hash fingerprint of current data
    current_hash = compute_hash(current_data)
    business_key = make_business_key(business_name, location, category)

    # Compare against stored hash
    hash_store    = load_hash_store()
    stored_entry  = hash_store.get(business_key)
    stored_hash   = stored_entry.get("hash_key") if stored_entry else None

    if stored_hash == current_hash:
        # ── NO NEW DATA ──────────────────────────────────────────────────────
        return {
            "status":        "NO NEW DATA",
            "hash_key":      current_hash,
            "business_name": business_name,
            "location":      location,
            "category":      category,
            "message":       "Data has not changed since last retrieval. Use cached analytical outputs.",
        }

    else:
        # ── NEW DATA ─────────────────────────────────────────────────────────
        # Update the hash store with the new hash
        hash_store[business_key] = {
            "hash_key":   current_hash,
            "updated_at": datetime.utcnow().isoformat(),
            "business_name": business_name,
            "location":      location,
            "category":      category,
        }
        save_hash_store(hash_store)

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
    Stefan uses this to fetch a previously seen dataset by its fingerprint.
    TODO: when DynamoDB is ready, query the scraped_data table by hash_key field.
    """

    # Search all collected files for one matching this hash
    pattern = os.path.join(COLLECTED_DATA_DIR, "*.json")
    all_files = glob.glob(pattern)

    for filepath in all_files:
        with open(filepath, "r") as f:
            collected = json.load(f)

        data = collected.get("data", [])
        if compute_hash(data) == hash_key:
            return {
                "status":        "FOUND",
                "hash_key":      hash_key,
                "business_name": collected.get("business_name", ""),
                "location":      collected.get("location", ""),
                "category":      collected.get("category", ""),
                "collected_at":  collected.get("collected_at", ""),
                "total_results": len(data),
                "data":          data,
            }

    raise HTTPException(
        status_code=404,
        detail=f"No data found for hash key '{hash_key}'"
    )


# ── Run locally ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("DataRetrieval:app", host="0.0.0.0", port=8001, reload=True)  # note: port 8001 (not 8000)