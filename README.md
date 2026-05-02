# Fault-Tolerant Data Processing System

A FastAPI-based backend for ingesting and normalizing events with idempotency guarantees.

## Features

- **Canonical Data Model**: Pydantic-based `CanonicalEvent` schema
- **Normalizer**: Maps inconsistent field names and handles type casting
- **Idempotency**: SHA-256 hash-based duplicate detection
- **Aggregates**: Real-time event counting and value summation by type
- **Failure Simulation**: Optional `simulate_failure` flag for testing

## Installation

```bash
pip install -r requirements.txt
```

## Running

```bash
uvicorn main:app --reload
```

## API Endpoints

### POST /ingest

Ingest a new event.

**Request Body:**
```json
{
  "payload": {
    "eventId": "evt-123",
    "ts": "2024-01-01T00:00:00",
    "type": "purchase",
    "userId": "user-456",
    "amount": "1200"
  },
  "simulate_failure": false
}
```

**Response (201 Created):**
```json
{
  "status": "created",
  "event_id": "evt-123",
  "hash": "a1b2c3d4...",
  "message": "Event ingested successfully"
}
```

**Error Responses:**
- `409 Conflict`: Duplicate event (same hash already exists)
- `422 Unprocessable Entity`: Normalization failed
- `500 Internal Server Error`: Simulated failure

### GET /aggregates

Retrieve current aggregates.

**Response:**
```json
{
  "counts": {"purchase": 5, "refund": 2},
  "sums": {"purchase": 15000, "refund": 300}
}
```

### GET /events

List all ingested events.

## Supported Field Aliases

| Input Key | Canonical Key |
|-----------|---------------|
| id, eventId, event_id | event_id |
| time, ts, timestamp | timestamp |
| type, eventType, event_type | event_type |
| userId, user_id, uid | user_id |
| amount, val, value | value |
| meta, metadata | metadata |

## Testing

```bash
pytest test_main.py -v
```
