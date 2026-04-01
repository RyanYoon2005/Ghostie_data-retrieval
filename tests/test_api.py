"""
Component tests for the Ghostie Data Retrieval API.

Level of abstraction: COMPONENT
Tests exercise the full HTTP request/response cycle through FastAPI, including
request parsing, validation, business logic, and response serialisation.
External AWS DynamoDB dependencies are replaced with MagicMock instances
(configured per-test via the mock_tables fixture in conftest.py), so no real
network calls are made and tests run deterministically offline.

Endpoints under test:
  GET  /             → root info
  GET  /health       → DynamoDB connectivity check
  POST /store        → ingest new scraped data
  GET  /retrieve     → latest data with hash comparison
  GET  /retrieve/{hash_key}  → data by specific hash
  GET  /companies    → list all tracked businesses
"""
from unittest.mock import PropertyMock

from botocore.exceptions import ClientError

from DataRetrieval import compute_hash

# ── Shared test data ─────────────────────────────────────────────────────────

SAMPLE_DATA = [
    {"type": "review", "body": "Great service!", "rating": 5},
    {"type": "news",   "title": "Subway expands in Sydney", "body": "Expansion plans announced."},
]
SAMPLE_HASH = compute_hash(SAMPLE_DATA)

SAMPLE_STORED_RECORD = {
    "hash_key":      SAMPLE_HASH,
    "business_key":  "subway_sydney_restaurant",
    "business_name": "Subway",
    "location":      "Sydney",
    "category":      "restaurant",
    "collected_at":  "2026-03-31T10:00:00",
    "news_count":    1,
    "review_count":  1,
    "data":          SAMPLE_DATA,
}

VALID_STORE_PAYLOAD = {
    "business_name": "Subway",
    "location":      "Sydney",
    "category":      "restaurant",
    "collected_at":  "2026-03-31T10:00:00",
    "news_count":    1,
    "review_count":  1,
    "data":          SAMPLE_DATA,
}

RETRIEVE_PARAMS = {
    "business_name": "Subway",
    "location":      "Sydney",
    "category":      "restaurant",
}


def _dynamo_error(message: str = "Test DynamoDB error") -> ClientError:
    """Build a ClientError that mimics a real AWS DynamoDB failure."""
    return ClientError(
        {"Error": {"Code": "InternalServerError", "Message": message}},
        "OperationName",
    )


# ── GET / ────────────────────────────────────────────────────────────────────


class TestRootEndpoint:
    def test_returns_200(self, client, mock_tables):
        assert client.get("/").status_code == 200

    def test_service_name_in_response(self, client, mock_tables):
        assert client.get("/").json()["service"] == "Ghostie Data Retrieval API"

    def test_response_contains_version(self, client, mock_tables):
        assert "version" in client.get("/").json()

    def test_response_status_is_running(self, client, mock_tables):
        assert client.get("/").json()["status"] == "running"

    def test_response_contains_endpoints_map(self, client, mock_tables):
        assert "endpoints" in client.get("/").json()


# ── GET /health ──────────────────────────────────────────────────────────────


class TestHealthEndpoint:
    def test_returns_200(self, client, mock_tables):
        assert client.get("/health").status_code == 200

    def test_healthy_when_dynamodb_reachable(self, client, mock_tables):
        mock_hash, _ = mock_tables
        mock_hash.table_status = "ACTIVE"
        assert client.get("/health").json()["status"] == "healthy"

    def test_response_contains_timestamp(self, client, mock_tables):
        assert "timestamp" in client.get("/health").json()

    def test_unhealthy_when_dynamodb_raises(self, client, mock_tables):
        mock_hash, _ = mock_tables
        type(mock_hash).table_status = PropertyMock(
            side_effect=_dynamo_error("Endpoint unreachable")
        )
        assert client.get("/health").json()["status"] == "unhealthy"

    def test_unhealthy_response_contains_error_message(self, client, mock_tables):
        mock_hash, _ = mock_tables
        type(mock_hash).table_status = PropertyMock(
            side_effect=_dynamo_error("Endpoint unreachable")
        )
        assert "error" in client.get("/health").json()


# ── POST /store ──────────────────────────────────────────────────────────────


class TestStoreEndpoint:
    def test_missing_body_returns_422(self, client, mock_tables):
        assert client.post("/store").status_code == 422

    def test_missing_business_name_returns_422(self, client, mock_tables):
        payload = {k: v for k, v in VALID_STORE_PAYLOAD.items() if k != "business_name"}
        assert client.post("/store", json=payload).status_code == 422

    def test_missing_location_returns_422(self, client, mock_tables):
        payload = {k: v for k, v in VALID_STORE_PAYLOAD.items() if k != "location"}
        assert client.post("/store", json=payload).status_code == 422

    def test_empty_data_field_returns_400(self, client, mock_tables):
        payload = {**VALID_STORE_PAYLOAD, "data": []}
        assert client.post("/store", json=payload).status_code == 400

    def test_valid_request_returns_200(self, client, mock_tables):
        assert client.post("/store", json=VALID_STORE_PAYLOAD).status_code == 200

    def test_response_status_is_stored(self, client, mock_tables):
        body = client.post("/store", json=VALID_STORE_PAYLOAD).json()
        assert body["status"] == "STORED"

    def test_response_contains_64_char_hash_key(self, client, mock_tables):
        body = client.post("/store", json=VALID_STORE_PAYLOAD).json()
        assert "hash_key" in body
        assert len(body["hash_key"]) == 64

    def test_hash_key_matches_data_content(self, client, mock_tables):
        body = client.post("/store", json=VALID_STORE_PAYLOAD).json()
        assert body["hash_key"] == SAMPLE_HASH

    def test_business_key_is_lowercase_normalised(self, client, mock_tables):
        body = client.post("/store", json=VALID_STORE_PAYLOAD).json()
        assert body["business_key"] == "subway_sydney_restaurant"

    def test_total_results_matches_data_length(self, client, mock_tables):
        body = client.post("/store", json=VALID_STORE_PAYLOAD).json()
        assert body["total_results"] == len(SAMPLE_DATA)

    def test_scraped_data_table_put_item_called(self, client, mock_tables):
        _, mock_scraped = mock_tables
        client.post("/store", json=VALID_STORE_PAYLOAD)
        mock_scraped.put_item.assert_called_once()

    def test_uppercase_business_name_normalised(self, client, mock_tables):
        payload = {**VALID_STORE_PAYLOAD, "business_name": "WOOLWORTHS", "location": "MELBOURNE"}
        body = client.post("/store", json=payload).json()
        assert body["business_key"] == "woolworths_melbourne_restaurant"


# ── GET /retrieve ────────────────────────────────────────────────────────────


class TestRetrieveEndpoint:
    def test_missing_all_params_returns_422(self, client, mock_tables):
        assert client.get("/retrieve").status_code == 422

    def test_missing_location_returns_422(self, client, mock_tables):
        response = client.get("/retrieve", params={"business_name": "Subway", "category": "restaurant"})
        assert response.status_code == 422

    def test_missing_category_returns_422(self, client, mock_tables):
        response = client.get("/retrieve", params={"business_name": "Subway", "location": "Sydney"})
        assert response.status_code == 422

    def test_returns_404_when_no_data_stored(self, client, mock_tables):
        _, mock_scraped = mock_tables
        mock_scraped.scan.return_value = {"Items": []}
        assert client.get("/retrieve", params=RETRIEVE_PARAMS).status_code == 404

    def test_new_data_returns_200(self, client, mock_tables):
        mock_hash, mock_scraped = mock_tables
        mock_scraped.scan.return_value = {"Items": [SAMPLE_STORED_RECORD]}
        mock_hash.get_item.return_value = {"Item": {"hash_key": "old_stale_hash"}}
        mock_hash.put_item.return_value = {}
        assert client.get("/retrieve", params=RETRIEVE_PARAMS).status_code == 200

    def test_new_data_status_field(self, client, mock_tables):
        mock_hash, mock_scraped = mock_tables
        mock_scraped.scan.return_value = {"Items": [SAMPLE_STORED_RECORD]}
        mock_hash.get_item.return_value = {"Item": {"hash_key": "old_stale_hash"}}
        mock_hash.put_item.return_value = {}
        body = client.get("/retrieve", params=RETRIEVE_PARAMS).json()
        assert body["status"] == "NEW DATA"

    def test_new_data_response_contains_required_fields(self, client, mock_tables):
        mock_hash, mock_scraped = mock_tables
        mock_scraped.scan.return_value = {"Items": [SAMPLE_STORED_RECORD]}
        mock_hash.get_item.return_value = {"Item": {"hash_key": "old_hash"}}
        mock_hash.put_item.return_value = {}
        body = client.get("/retrieve", params=RETRIEVE_PARAMS).json()
        for field in ("status", "hash_key", "business_name", "location", "category",
                      "total_results", "data"):
            assert field in body, f"Missing field: {field}"

    def test_no_new_data_when_hash_matches(self, client, mock_tables):
        mock_hash, mock_scraped = mock_tables
        mock_scraped.scan.return_value = {"Items": [SAMPLE_STORED_RECORD]}
        mock_hash.get_item.return_value = {"Item": {"hash_key": SAMPLE_HASH}}
        body = client.get("/retrieve", params=RETRIEVE_PARAMS).json()
        assert body["status"] == "NO NEW DATA"

    def test_no_new_data_response_contains_hash_key(self, client, mock_tables):
        mock_hash, mock_scraped = mock_tables
        mock_scraped.scan.return_value = {"Items": [SAMPLE_STORED_RECORD]}
        mock_hash.get_item.return_value = {"Item": {"hash_key": SAMPLE_HASH}}
        body = client.get("/retrieve", params=RETRIEVE_PARAMS).json()
        assert body["hash_key"] == SAMPLE_HASH

    def test_no_new_data_does_not_call_put_item(self, client, mock_tables):
        mock_hash, mock_scraped = mock_tables
        mock_scraped.scan.return_value = {"Items": [SAMPLE_STORED_RECORD]}
        mock_hash.get_item.return_value = {"Item": {"hash_key": SAMPLE_HASH}}
        client.get("/retrieve", params=RETRIEVE_PARAMS)
        mock_hash.put_item.assert_not_called()

    def test_new_data_updates_hash_keys_table(self, client, mock_tables):
        mock_hash, mock_scraped = mock_tables
        mock_scraped.scan.return_value = {"Items": [SAMPLE_STORED_RECORD]}
        mock_hash.get_item.return_value = {"Item": {"hash_key": "stale_hash"}}
        mock_hash.put_item.return_value = {}
        client.get("/retrieve", params=RETRIEVE_PARAMS)
        mock_hash.put_item.assert_called_once()

    def test_first_retrieval_treated_as_new_data(self, client, mock_tables):
        mock_hash, mock_scraped = mock_tables
        mock_scraped.scan.return_value = {"Items": [SAMPLE_STORED_RECORD]}
        mock_hash.get_item.return_value = {}  # no stored hash yet
        mock_hash.put_item.return_value = {}
        body = client.get("/retrieve", params=RETRIEVE_PARAMS).json()
        assert body["status"] == "NEW DATA"


# ── GET /retrieve/{hash_key} ─────────────────────────────────────────────────


class TestRetrieveByHashEndpoint:
    def test_valid_hash_returns_200(self, client, mock_tables):
        _, mock_scraped = mock_tables
        mock_scraped.get_item.return_value = {"Item": SAMPLE_STORED_RECORD}
        assert client.get(f"/retrieve/{SAMPLE_HASH}").status_code == 200

    def test_unknown_hash_returns_404(self, client, mock_tables):
        _, mock_scraped = mock_tables
        mock_scraped.get_item.return_value = {}
        assert client.get("/retrieve/unknownhash123").status_code == 404

    def test_response_status_is_found(self, client, mock_tables):
        _, mock_scraped = mock_tables
        mock_scraped.get_item.return_value = {"Item": SAMPLE_STORED_RECORD}
        assert client.get(f"/retrieve/{SAMPLE_HASH}").json()["status"] == "FOUND"

    def test_response_contains_data_field(self, client, mock_tables):
        _, mock_scraped = mock_tables
        mock_scraped.get_item.return_value = {"Item": SAMPLE_STORED_RECORD}
        assert "data" in client.get(f"/retrieve/{SAMPLE_HASH}").json()

    def test_response_contains_business_name(self, client, mock_tables):
        _, mock_scraped = mock_tables
        mock_scraped.get_item.return_value = {"Item": SAMPLE_STORED_RECORD}
        body = client.get(f"/retrieve/{SAMPLE_HASH}").json()
        assert body["business_name"] == "Subway"

    def test_response_contains_total_results(self, client, mock_tables):
        _, mock_scraped = mock_tables
        mock_scraped.get_item.return_value = {"Item": SAMPLE_STORED_RECORD}
        body = client.get(f"/retrieve/{SAMPLE_HASH}").json()
        assert "total_results" in body
        assert body["total_results"] == len(SAMPLE_DATA)

    def test_response_hash_key_matches_requested(self, client, mock_tables):
        _, mock_scraped = mock_tables
        mock_scraped.get_item.return_value = {"Item": SAMPLE_STORED_RECORD}
        body = client.get(f"/retrieve/{SAMPLE_HASH}").json()
        assert body["hash_key"] == SAMPLE_HASH


# ── GET /companies ───────────────────────────────────────────────────────────


class TestCompaniesEndpoint:
    def test_returns_200(self, client, mock_tables):
        mock_hash, _ = mock_tables
        mock_hash.scan.return_value = {"Items": []}
        assert client.get("/companies").status_code == 200

    def test_response_contains_count(self, client, mock_tables):
        mock_hash, _ = mock_tables
        mock_hash.scan.return_value = {"Items": []}
        assert "count" in client.get("/companies").json()

    def test_response_contains_companies_list(self, client, mock_tables):
        mock_hash, _ = mock_tables
        mock_hash.scan.return_value = {"Items": []}
        assert "companies" in client.get("/companies").json()

    def test_empty_table_returns_zero_count(self, client, mock_tables):
        mock_hash, _ = mock_tables
        mock_hash.scan.return_value = {"Items": []}
        assert client.get("/companies").json()["count"] == 0

    def test_count_reflects_number_of_items(self, client, mock_tables):
        mock_hash, _ = mock_tables
        mock_hash.scan.return_value = {"Items": [SAMPLE_STORED_RECORD, SAMPLE_STORED_RECORD]}
        assert client.get("/companies").json()["count"] == 2

    def test_dynamodb_error_returns_500(self, client, mock_tables):
        mock_hash, _ = mock_tables
        mock_hash.scan.side_effect = _dynamo_error("Table does not exist")
        assert client.get("/companies").status_code == 500
