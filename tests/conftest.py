"""
Shared pytest fixtures for the Ghostie Data Retrieval test suite.

Strategy: Component + Unit tests
- Unit tests (test_utils.py): pure functions, no fixtures needed
- Component tests (test_api.py): full HTTP layer via FastAPI TestClient,
  with DynamoDB tables replaced by MagicMock so no real AWS calls are made.

The two module-level DynamoDB table objects (hash_keys_table, scraped_data_table)
are patched via monkeypatch.setattr before each test, so all route handlers use
the mocks instead of the real AWS resource. Fixtures are scoped to 'function'
(default) so every test starts with a clean mock state.
"""
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import DataRetrieval


@pytest.fixture
def mock_tables(monkeypatch):
    """
    Replace both DynamoDB table objects with MagicMock instances.

    Returns (mock_hash_keys_table, mock_scraped_data_table) so individual
    tests can configure return values and assert call counts.
    """
    mock_hash = MagicMock()
    mock_scraped = MagicMock()
    monkeypatch.setattr(DataRetrieval, "hash_keys_table", mock_hash)
    monkeypatch.setattr(DataRetrieval, "scraped_data_table", mock_scraped)
    return mock_hash, mock_scraped


@pytest.fixture
def client(mock_tables):
    """
    Return a synchronous FastAPI TestClient backed by mocked DynamoDB tables.
    Depends on mock_tables so the patch is always active when the client runs.
    """
    return TestClient(DataRetrieval.app)
