"""Generate deterministic operational state events and a verification manifest."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
from typing import Any, Iterable, Iterator, TextIO

from .event import SCHEMA_VERSION, OperationalState, canonical_json, sha256_hex

DEFAULT_START = "2026-08-24T00:00:00Z"
DEFAULT_INTERVAL_SECONDS = 300
LATE_OFFSET_SECONDS = 120
SCENARIOS = ("baseline", "duplicate", "late-120s")
STATES = ("created", "processing", "completed")
DUPLICATE_NOTE = (
    "A duplicate repeats the complete event. A future consumer must acknowledge the "
    "redelivery but must not apply it a second time."
)
LATE_NOTE = (
    "A late delivery has an older event_time than existing state for the same entity. "
    "It must not overwrite the newer materialized state."
)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    """Parse a contract timestamp and require an explicit timezone."""

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return parsed


def generate(
    run_id: str,
    seed: int,
    count: int,
    start_time: str = DEFAULT_START,
    scenario: str = "baseline",
) -> Iterator[OperationalState]:
    """Yield the same ordered events for the same five arguments."""

    if not run_id or len(run_id) > 80:
        raise ValueError("run_id must contain 1 to 80 characters")
    if count < 0:
        raise ValueError("count must be non-negative")
    if scenario not in SCENARIOS:
        raise ValueError(f"scenario must be one of: {', '.join(SCENARIOS)}")
    if scenario != "baseline" and count < 2:
        raise ValueError(f"{scenario} scenario requires count of at least 2")

    start = parse_time(start_time)
    rng = random.Random(seed)
    first_event: OperationalState | None = None

    for sequence in range(count):
        if scenario == "duplicate" and sequence == 1:
            assert first_event is not None
            yield first_event
            continue

        is_late = scenario == "late-120s" and sequence == 1
        entity_number = 0 if is_late else sequence
        event_time = (
            start - timedelta(seconds=LATE_OFFSET_SECONDS)
            if is_late
            else start + timedelta(seconds=DEFAULT_INTERVAL_SECONDS * sequence)
        )
        identity = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "sequence": sequence,
            "source": "synthetic",
            "entity_type": "resource",
            "entity_id": f"resource-{entity_number:04d}",
            "event_type": "state.updated",
            "event_time": _iso(event_time),
            "state": (
                "processing"
                if is_late
                else "completed"
                if scenario == "late-120s" and sequence == 0
                else STATES[rng.randrange(len(STATES))]
            ),
        }
        received_at = (
            start + timedelta(seconds=1)
            if is_late
            else event_time + timedelta(milliseconds=rng.randint(20, 500))
        )
        event = OperationalState(
            event_id=sha256_hex(identity),
            received_at=_iso(received_at),
            **{key: value for key, value in identity.items() if key != "schema_version"},
        )
        if first_event is None:
            first_event = event
        yield event


def _write_row(handle: TextIO, row: dict[str, Any]) -> None:
    handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def write_run(
    events: Iterable[OperationalState],
    output: Path,
    manifest: Path,
    scenario: str = "baseline",
) -> int:
    """Write event and verification-manifest NDJSON files."""

    if scenario not in SCENARIOS:
        raise ValueError(f"scenario must be one of: {', '.join(SCENARIOS)}")

    output.parent.mkdir(parents=True, exist_ok=True)
    manifest.parent.mkdir(parents=True, exist_ok=True)

    first_arrival: dict[str, int] = {}
    duplicate_arrivals: list[int] = []
    late_arrivals: list[int] = []
    newest_by_entity: dict[str, OperationalState] = {}
    run_id: str | None = None
    emitted = 0

    with output.open("w", encoding="utf-8", newline="\n") as event_file, manifest.open(
        "w", encoding="utf-8", newline="\n"
    ) as manifest_file:
        for arrival, event in enumerate(events):
            emitted = arrival + 1
            run_id = event.run_id
            payload = event.to_dict()
            event_file.write(canonical_json(payload).decode() + "\n")

            duplicate_of = first_arrival.setdefault(event.event_id, arrival)
            if duplicate_of != arrival:
                duplicate_arrivals.append(arrival)

            current = newest_by_entity.get(event.entity_id)
            current_time = None if current is None else parse_time(current.event_time)
            event_time = parse_time(event.event_time)
            late_by = 0.0
            if current_time is not None and event_time < current_time:
                late_by = (current_time - event_time).total_seconds()
                late_arrivals.append(arrival)
            elif current_time is None or event_time > current_time:
                newest_by_entity[event.entity_id] = event

            _write_row(
                manifest_file,
                {
                    "record": "event",
                    "run_id": event.run_id,
                    "arrival_index": arrival,
                    "sequence": event.sequence,
                    "event_id": event.event_id,
                    "entity_id": event.entity_id,
                    "event_time": event.event_time,
                    "received_at": event.received_at,
                    "state": event.state,
                    "payload_sha256": sha256_hex(payload),
                    "duplicate_of_arrival_index": None if duplicate_of == arrival else duplicate_of,
                    "late_by_seconds": late_by,
                },
            )

        notes = []
        if duplicate_arrivals:
            notes.append(DUPLICATE_NOTE)
        if late_arrivals:
            notes.append(LATE_NOTE)
        _write_row(
            manifest_file,
            {
                "record": "summary",
                "run_id": run_id,
                "scenario": scenario,
                "emitted_events": emitted,
                "expected_accepted_events": len(first_arrival),
                "duplicate_arrival_indexes": duplicate_arrivals,
                "late_arrival_indexes": late_arrivals,
                "expected_current_state": [
                    {
                        "entity_id": event.entity_id,
                        "event_id": event.event_id,
                        "event_time": event.event_time,
                        "state": event.state,
                    }
                    for event in sorted(newest_by_entity.values(), key=lambda item: item.entity_id)
                ],
                "notes": notes,
            },
        )
    return emitted


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--start-time", default=DEFAULT_START)
    parser.add_argument("--scenario", choices=SCENARIOS, default="baseline")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    written = write_run(
        generate(args.run_id, args.seed, args.count, args.start_time, args.scenario),
        args.output,
        args.manifest,
        args.scenario,
    )
    print(f"wrote {written} events to {args.output} and {args.manifest}")


if __name__ == "__main__":
    main()
