from __future__ import annotations

import hashlib
import json
from pathlib import Path
import unittest

from jsonschema import Draft202012Validator, FormatChecker

from eventproof.event import canonical_json
from eventproof.simulator import SCENARIOS, generate, parse_time, write_run

SCHEMA_PATH = Path("contracts/operational-state.v1.schema.json")


class SimulatorTest(unittest.TestCase):
    def scratch(self, name: str) -> Path:
        # ponytail: fixed names are enough until the test suite runs in parallel.
        path = Path("benchmarks/evidence") / f".test-{name}"
        self.addCleanup(path.unlink, missing_ok=True)
        return path

    def run_manifest(self, name: str, scenario: str, count: int = 4) -> list[dict]:
        output = self.scratch(f"{name}-events")
        manifest = self.scratch(f"{name}-manifest")
        write_run(generate(name, 5, count, scenario=scenario), output, manifest, scenario)
        return [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]

    def test_every_scenario_validates_against_schema(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, format_checker=FormatChecker())

        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario):
                for event in generate("schema", 1, 4, scenario=scenario):
                    self.assertEqual(list(validator.iter_errors(event.to_dict())), [])

    def test_same_input_produces_identical_files(self) -> None:
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario):
                first_events = self.scratch(f"{scenario}-first-events")
                first_manifest = self.scratch(f"{scenario}-first-manifest")
                second_events = self.scratch(f"{scenario}-second-events")
                second_manifest = self.scratch(f"{scenario}-second-manifest")
                args = ("repeatable", 42, 20, "2026-08-24T00:00:00Z", scenario)
                write_run(generate(*args), first_events, first_manifest, scenario)
                write_run(generate(*args), second_events, second_manifest, scenario)
                self.assertEqual(first_events.read_bytes(), second_events.read_bytes())
                self.assertEqual(first_manifest.read_bytes(), second_manifest.read_bytes())

    def test_manifest_matches_unique_event_payloads(self) -> None:
        output = self.scratch("identity-events")
        manifest = self.scratch("identity-manifest")
        self.assertEqual(write_run(generate("identity", 7, 250), output, manifest), 250)

        events = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
        rows = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
        summary = rows.pop()
        self.assertEqual(len({event["event_id"] for event in events}), 250)
        for arrival, (event, row) in enumerate(zip(events, rows, strict=True)):
            self.assertEqual(row["record"], "event")
            self.assertEqual(row["arrival_index"], arrival)
            self.assertEqual(row["event_id"], event["event_id"])
            self.assertIsNone(row["duplicate_of_arrival_index"])
            self.assertEqual(row["late_by_seconds"], 0.0)
            self.assertEqual(
                row["payload_sha256"], hashlib.sha256(canonical_json(event)).hexdigest()
            )
        self.assertEqual(summary["scenario"], "baseline")
        self.assertEqual(summary["emitted_events"], 250)
        self.assertEqual(summary["expected_accepted_events"], 250)
        self.assertEqual(len(summary["expected_current_state"]), 250)
        self.assertEqual(summary["notes"], [])

    def test_duplicate_scenario_repeats_the_exact_first_event(self) -> None:
        events = list(generate("duplicate", 11, 4, scenario="duplicate"))

        self.assertEqual(events[1], events[0])
        self.assertEqual(canonical_json(events[1].to_dict()), canonical_json(events[0].to_dict()))
        self.assertEqual(len({event.event_id for event in events}), 3)

    def test_duplicate_manifest_states_the_redelivery_relationship(self) -> None:
        rows = self.run_manifest("dup-manifest", "duplicate")
        summary = rows.pop()

        self.assertEqual(rows[1]["duplicate_of_arrival_index"], 0)
        self.assertEqual(rows[1]["event_id"], rows[0]["event_id"])
        self.assertEqual(rows[1]["payload_sha256"], rows[0]["payload_sha256"])
        self.assertEqual(summary["scenario"], "duplicate")
        self.assertEqual(summary["emitted_events"], 4)
        self.assertEqual(summary["expected_accepted_events"], 3)
        self.assertEqual(summary["duplicate_arrival_indexes"], [1])
        self.assertEqual(len(summary["expected_current_state"]), 3)
        self.assertIn("must not apply it a second time", " ".join(summary["notes"]))

    def test_late_scenario_delivers_older_state_after_newer_state(self) -> None:
        newer, late = list(generate("late", 13, 2, scenario="late-120s"))

        self.assertEqual(newer.state, "completed")
        self.assertEqual(late.state, "processing")
        self.assertEqual(late.entity_id, newer.entity_id)
        self.assertEqual(
            (parse_time(newer.event_time) - parse_time(late.event_time)).total_seconds(), 120
        )
        self.assertGreater(parse_time(late.received_at), parse_time(newer.received_at))
        self.assertNotEqual(late.event_id, newer.event_id)

    def test_late_manifest_preserves_newer_state_per_entity(self) -> None:
        rows = self.run_manifest("late-manifest", "late-120s", count=4)
        summary = rows.pop()
        current = {row["entity_id"]: row for row in summary["expected_current_state"]}

        self.assertEqual(rows[1]["late_by_seconds"], 120.0)
        self.assertEqual(rows[1]["entity_id"], rows[0]["entity_id"])
        self.assertIsNone(rows[1]["duplicate_of_arrival_index"])
        self.assertEqual(summary["scenario"], "late-120s")
        self.assertEqual(summary["emitted_events"], 4)
        self.assertEqual(summary["expected_accepted_events"], 4)
        self.assertEqual(summary["late_arrival_indexes"], [1])
        self.assertEqual(len(current), 3)
        self.assertEqual(current[rows[0]["entity_id"]]["event_id"], rows[0]["event_id"])
        self.assertEqual(current[rows[0]["entity_id"]]["state"], "completed")
        self.assertIn("must not overwrite the newer materialized state", " ".join(summary["notes"]))

    def test_invalid_inputs_fail_before_events_are_yielded(self) -> None:
        with self.assertRaisesRegex(ValueError, "JSON compliant"):
            canonical_json({"not_json": float("nan")})
        with self.assertRaisesRegex(ValueError, "count"):
            list(generate("bad-count", 1, -1))
        with self.assertRaisesRegex(ValueError, "run_id"):
            list(generate("", 1, 1))
        with self.assertRaisesRegex(ValueError, "timezone"):
            list(generate("bad-time", 1, 1, "2026-08-24T00:00:00"))
        with self.assertRaisesRegex(ValueError, "scenario"):
            list(generate("bad-scenario", 1, 2, scenario="unknown"))
        with self.assertRaisesRegex(ValueError, "at least 2"):
            list(generate("too-short", 1, 1, scenario="duplicate"))
        with self.assertRaisesRegex(ValueError, "scenario"):
            write_run([], self.scratch("unused-events"), self.scratch("unused-manifest"), "unknown")


if __name__ == "__main__":
    unittest.main()
