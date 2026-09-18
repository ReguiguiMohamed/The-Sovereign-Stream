"""Checkpoint and recovery evidence, from Flink's own REST API.

An object under the checkpoint prefix does not say that a checkpoint completed
for the running job, and a count of log lines does not say what was restored.
This reads the JobManager: the completed checkpoint and the GCS path holding it,
then, after the TaskManager is deleted, the checkpoint the job was restored
from, a checkpoint completed after that restore, and records still flowing.

  python3 deploy/recovery_check.py http://localhost:8081 gs://<bucket>
"""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

NAMESPACE = "eventproof"
TASKMANAGER = "app=current-state,component=taskmanager"
PATIENCE = 600
STEP = 10


def api(base, path):
    with urllib.request.urlopen(base + path, timeout=30) as response:
        return json.loads(response.read())


def running_job(overview):
    for job in (overview or {}).get("jobs", []):
        if job.get("state") == "RUNNING":
            return job["jid"]
    return None


def completed_under(payload, bucket, minimum=-1):
    """A completed checkpoint after `minimum` whose state is in this bucket."""
    completed = ((payload or {}).get("latest") or {}).get("completed")
    if not completed or completed.get("id", -1) <= minimum:
        return None
    if not str(completed.get("external_path") or "").startswith(bucket):
        return None
    return completed


def restored_from(payload, bucket, minimum, since):
    """The checkpoint this job was restored from, for a restore after `since`."""
    restored = ((payload or {}).get("latest") or {}).get("restored")
    if not restored or restored.get("is_savepoint"):
        return None
    if restored.get("id", -1) < minimum or restored.get("restore_timestamp", 0) <= since:
        return None
    if not str(restored.get("external_path") or "").startswith(bucket):
        return None
    return restored


def restored_at(payload):
    restored = ((payload or {}).get("latest") or {}).get("restored") or {}
    return restored.get("restore_timestamp", 0)


def records(base, jid):
    """Records read and written across the job's vertices."""
    total = 0
    for vertex in api(base, "/jobs/" + jid).get("vertices", []):
        metrics = vertex.get("metrics", {})
        for name in ("read-records", "write-records"):
            total += int(metrics.get(name) or 0)
    return total


def progressed(base, jid, before):
    """The record total once it has risen above `before`."""
    now = records(base, jid)
    return now if now > before else None


def wait(what, produce):
    deadline = time.time() + PATIENCE
    while True:
        try:
            found = produce()
        except (urllib.error.URLError, OSError, ValueError):
            found = None
        if found:
            return found
        if time.time() >= deadline:
            raise SystemExit("ACCEPTANCE FAILED: timed out waiting for " + what)
        time.sleep(STEP)


def self_check():
    bucket = "gs://p-flink"
    payload = {"latest": {
        "completed": {"id": 7, "external_path": bucket + "/checkpoints/j/chk-7"},
        "restored": {"id": 7, "restore_timestamp": 500, "is_savepoint": False,
                     "external_path": bucket + "/checkpoints/j/chk-7"}}}
    assert completed_under(payload, bucket)["id"] == 7
    assert completed_under(payload, bucket, 7) is None
    assert completed_under({"latest": {"completed": {"id": 7, "external_path": "file:/tmp/chk-7"}}},
                           bucket) is None
    assert completed_under({}, bucket) is None
    assert restored_from(payload, bucket, 7, 0)["id"] == 7
    assert restored_from(payload, bucket, 8, 0) is None
    assert restored_from(payload, bucket, 7, 500) is None
    assert restored_from({"latest": {"restored": dict(payload["latest"]["restored"],
                                                      is_savepoint=True)}}, bucket, 7, 0) is None
    assert restored_at({}) == 0
    assert running_job({"jobs": [{"jid": "a", "state": "RESTARTING"},
                                 {"jid": "b", "state": "RUNNING"}]}) == "b"
    assert running_job({"jobs": []}) is None


def main(argv):
    self_check()
    base, bucket = argv[1].rstrip("/"), argv[2].rstrip("/")

    def checkpoints(jid):
        return api(base, "/jobs/{}/checkpoints".format(jid))

    jid = wait("a RUNNING Flink job", lambda: running_job(api(base, "/jobs/overview")))
    completed = wait("a completed checkpoint under " + bucket,
                     lambda: completed_under(checkpoints(jid), bucket))
    print("ACCEPT checkpoint {} completed at {}".format(
        completed["id"], completed["external_path"]))

    since = restored_at(checkpoints(jid))
    subprocess.run(["kubectl", "-n", NAMESPACE, "delete", "pod", "-l", TASKMANAGER,
                    "--wait=false"], check=True)

    jid = wait("a RUNNING Flink job after the TaskManager was deleted",
               lambda: running_job(api(base, "/jobs/overview")))
    restored = wait("a restore from checkpoint {} or later".format(completed["id"]),
                    lambda: restored_from(checkpoints(jid), bucket, completed["id"], since))
    print("ACCEPT job {} restored from checkpoint {} at {}".format(
        jid, restored["id"], restored["external_path"]))

    newer = wait("a checkpoint completed after the restore",
                 lambda: completed_under(checkpoints(jid), bucket, restored["id"]))
    print("ACCEPT checkpoint {} completed after recovery at {}".format(
        newer["id"], newer["external_path"]))

    before = records(base, jid)
    after = wait("records processed after recovery",
                 lambda: progressed(base, jid, before))
    print("ACCEPT {} records read and written after recovery, up from {}".format(after, before))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
