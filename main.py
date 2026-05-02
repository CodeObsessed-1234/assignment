from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, field_validator


app = FastAPI(title="Fault-Tolerant Data Processing System")


class DuplicateEventError(Exception):
    pass


class NormalizationError(Exception):
    pass


class CanonicalEvent(BaseModel):
    event_id: str
    timestamp: datetime
    event_type: str
    user_id: str
    value: int
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("value", mode="before")
    @classmethod
    def cast_value(cls, v: Any) -> int:
        if isinstance(v, str):
            try:
                return int(v)
            except ValueError as e:
                raise ValueError(f"Cannot cast '{v}' to int") from e
        if not isinstance(v, int):
            raise ValueError(f"Expected int or str, got {type(v).__name__}")
        return v


class Normalizer:
    KEY_MAP: dict[str, str] = {
        "id": "event_id",
        "eventId": "event_id",
        "event_id": "event_id",
        "time": "timestamp",
        "ts": "timestamp",
        "timestamp": "timestamp",
        "type": "event_type",
        "eventType": "event_type",
        "event_type": "event_type",
        "userId": "user_id",
        "user_id": "user_id",
        "uid": "user_id",
        "amount": "value",
        "val": "value",
        "value": "value",
        "meta": "metadata",
        "metadata": "metadata",
    }

    @classmethod
    def normalize(cls, raw: dict[str, Any]) -> CanonicalEvent:
        normalized: dict[str, Any] = {}

        for key, value in raw.items():
            mapped_key = cls.KEY_MAP.get(key, key)
            normalized[mapped_key] = value

        try:
            return CanonicalEvent(**normalized)
        except Exception as e:
            raise NormalizationError(f"Failed to normalize event: {e}") from e


def generate_hash(payload: dict[str, Any]) -> str:
    payload_str = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload_str.encode()).hexdigest()


class Aggregates:
    def __init__(self) -> None:
        self._counts: defaultdict[str, int] = defaultdict(int)
        self._sums: defaultdict[str, int] = defaultdict(int)

    def record(self, event: CanonicalEvent) -> None:
        self._counts[event.event_type] += 1
        self._sums[event.event_type] += event.value

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    @property
    def sums(self) -> dict[str, int]:
        return dict(self._sums)


mock_db: dict[str, CanonicalEvent] = {}
aggregates = Aggregates()


class IngestRequest(BaseModel):
    payload: dict[str, Any]
    simulate_failure: bool = False


class IngestResponse(BaseModel):
    status: str
    event_id: str
    hash: str
    message: str


@app.post("/ingest", response_model=IngestResponse, status_code=status.HTTP_201_CREATED)
async def ingest(request: IngestRequest) -> IngestResponse:
    if request.simulate_failure:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Simulated failure triggered",
        )

    event_hash = generate_hash(request.payload)

    if event_hash in mock_db:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Event with hash {event_hash} already exists",
        )

    try:
        event = Normalizer.normalize(request.payload)
    except NormalizationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        ) from e

    mock_db[event_hash] = event
    aggregates.record(event)

    return IngestResponse(
        status="created",
        event_id=event.event_id,
        hash=event_hash,
        message="Event ingested successfully",
    )


@app.get("/aggregates")
async def get_aggregates() -> dict[str, Any]:
    return {"counts": aggregates.counts, "sums": aggregates.sums}


@app.get("/events")
async def list_events() -> list[dict[str, Any]]:
    return [event.model_dump() for event in mock_db.values()]
