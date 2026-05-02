import json
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from main import app, CanonicalEvent, Normalizer, generate_hash, mock_db, aggregates


client = TestClient(app)


def test_normalizer_maps_inconsistent_keys():
    raw = {
        "eventId": "evt-123",
        "ts": "2024-01-01T00:00:00",
        "type": "purchase",
        "userId": "user-456",
        "amount": "1200",
        "meta": {"source": "mobile"},
    }

    event = Normalizer.normalize(raw)

    assert event.event_id == "evt-123"
    assert event.event_type == "purchase"
    assert event.user_id == "user-456"
    assert event.value == 1200
    assert event.metadata == {"source": "mobile"}


def test_normalizer_casts_string_to_int():
    raw = {
        "event_id": "evt-123",
        "timestamp": "2024-01-01T00:00:00",
        "event_type": "purchase",
        "user_id": "user-456",
        "value": "1200",
    }

    event = Normalizer.normalize(raw)
    assert isinstance(event.value, int)
    assert event.value == 1200


def test_normalizer_preserves_unknown_keys():
    raw = {
        "event_id": "evt-123",
        "timestamp": "2024-01-01T00:00:00",
        "event_type": "purchase",
        "user_id": "user-456",
        "value": 100,
        "custom_field": "preserved",
    }

    event = Normalizer.normalize(raw)
    assert event.metadata == {"custom_field": "preserved"}


def test_generate_hash_is_deterministic():
    payload = {"a": 1, "b": 2}
    hash1 = generate_hash(payload)
    hash2 = generate_hash(payload)
    assert hash1 == hash2


def test_generate_hash_order_independent():
    payload1 = {"a": 1, "b": 2}
    payload2 = {"b": 2, "a": 1}
    assert generate_hash(payload1) == generate_hash(payload2)


def test_ingest_creates_event():
    mock_db.clear()
    aggregates._counts.clear()
    aggregates._sums.clear()

    payload = {
        "event_id": "evt-001",
        "timestamp": "2024-01-01T00:00:00",
        "event_type": "purchase",
        "user_id": "user-001",
        "value": 100,
    }

    response = client.post("/ingest", json={"payload": payload, "simulate_failure": False})

    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "created"
    assert data["event_id"] == "evt-001"
    assert "hash" in data


def test_ingest_idempotent():
    mock_db.clear()

    payload = {
        "event_id": "evt-002",
        "timestamp": "2024-01-01T00:00:00",
        "event_type": "purchase",
        "user_id": "user-002",
        "value": 200,
    }

    response1 = client.post("/ingest", json={"payload": payload})
    assert response1.status_code == 201

    response2 = client.post("/ingest", json={"payload": payload})
    assert response2.status_code == 409
    assert "already exists" in response2.json()["detail"]


def test_ingest_simulated_failure():
    payload = {"event_id": "evt-003", "timestamp": "2024-01-01T00:00:00", "event_type": "purchase", "user_id": "user-003", "value": 300}

    response = client.post("/ingest", json={"payload": payload, "simulate_failure": True})

    assert response.status_code == 500
    assert "Simulated failure" in response.json()["detail"]


def test_aggregates_updated():
    mock_db.clear()
    aggregates._counts.clear()
    aggregates._sums.clear()

    payloads = [
        {"event_id": "evt-001", "timestamp": "2024-01-01T00:00:00", "event_type": "purchase", "user_id": "user-001", "value": 100},
        {"event_id": "evt-002", "timestamp": "2024-01-01T00:00:00", "event_type": "purchase", "user_id": "user-002", "value": 200},
        {"event_id": "evt-003", "timestamp": "2024-01-01T00:00:00", "event_type": "refund", "user_id": "user-003", "value": 50},
    ]

    for payload in payloads:
        client.post("/ingest", json={"payload": payload})

    response = client.get("/aggregates")
    data = response.json()

    assert data["counts"]["purchase"] == 2
    assert data["sums"]["purchase"] == 300
    assert data["counts"]["refund"] == 1
    assert data["sums"]["refund"] == 50


def test_list_events():
    mock_db.clear()

    payload = {"event_id": "evt-001", "timestamp": "2024-01-01T00:00:00", "event_type": "purchase", "user_id": "user-001", "value": 100}
    client.post("/ingest", json={"payload": payload})

    response = client.get("/events")
    events = response.json()

    assert len(events) == 1
    assert events[0]["event_id"] == "evt-001"
