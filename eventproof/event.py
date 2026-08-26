"""Versioned operational-state event and canonical JSON helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
from typing import Any

SCHEMA_VERSION = "operational-state.v1"


def canonical_json(value: dict[str, Any]) -> bytes:
    """Encode a mapping to stable, standards-compliant UTF-8 JSON bytes."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def sha256_hex(value: dict[str, Any]) -> str:
    """Return the SHA-256 of a canonical JSON mapping."""

    return hashlib.sha256(canonical_json(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class OperationalState:
    """One normalized state change for one operational entity."""

    event_id: str
    run_id: str
    sequence: int
    source: str
    entity_type: str
    entity_id: str
    event_type: str
    event_time: str
    received_at: str
    state: str
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
        if not all(
            (
                self.event_id,
                self.run_id,
                self.source,
                self.entity_type,
                self.entity_id,
                self.event_type,
                self.state,
            )
        ):
            raise ValueError("event, entity and state fields must not be empty")
        if len(self.run_id) > 80:
            raise ValueError("run_id must contain 1 to 80 characters")
        if self.sequence < 0:
            raise ValueError("sequence must be non-negative")
        if len(self.event_id) != 64 or any(c not in "0123456789abcdef" for c in self.event_id):
            raise ValueError("event_id must be a lowercase SHA-256 hex digest")
        for name, value in (("event_time", self.event_time), ("received_at", self.received_at)):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError(f"{name} must include a timezone")

    def to_dict(self) -> dict[str, Any]:
        """Return the event using the contract field names."""

        return asdict(self)
