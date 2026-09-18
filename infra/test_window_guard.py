"""Checks that window_guard refuses a schedule that would not tear the window down.

Run by the guard step before it trusts the guard: python3 infra/test_window_guard.py
"""

import base64
import copy
import json
from datetime import datetime, timedelta, timezone

from window_guard import problems

NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)
BUILD = {
    "serviceAccount": "projects/p/serviceAccounts/cloud-build@p.iam.gserviceaccount.com",
    "source": {"storageSource": {"bucket": "p-tfstate", "object": "teardown/evidence.tgz",
                                 "generation": "17"}},
    "steps": [{"id": "gate", "env": ["PROJECT_ID=p", "WINDOW_END=2026-09-18T20:00:00Z"]}],
}
TARGET = {
    "uri": "https://cloudbuild.googleapis.com/v1/projects/p/locations/europe-west1/builds",
    "body": base64.b64encode(json.dumps(BUILD).encode()).decode(),
    "http_method": "POST",
    "oauth_token": [{"service_account_email": "teardown-scheduler@p.iam.gserviceaccount.com"}],
}
STATE = {
    "outputs": {"window_end": {"value": "2026-09-18T20:00:00Z"},
                "bundle_generation": {"value": "17"}},
    "resources": [{
        "type": "google_cloud_scheduler_job", "name": "teardown",
        "instances": [{"attributes": {
            "name": "evidence-teardown", "project": "p", "region": "europe-west1",
            "schedule": "*/15 * * * *", "time_zone": "Etc/UTC",
            "attempt_deadline": "180s", "paused": False,
            "http_target": [TARGET],
        }}],
    }],
}
JOB = {
    "name": "projects/p/locations/europe-west1/jobs/evidence-teardown",
    "state": "ENABLED", "schedule": "*/15 * * * *", "timeZone": "Etc/UTC",
    "attemptDeadline": "180s",
    "httpTarget": {"uri": TARGET["uri"], "body": TARGET["body"], "httpMethod": "POST",
                   "oauthToken": {"serviceAccountEmail":
                                  "teardown-scheduler@p.iam.gserviceaccount.com"}},
}


def refused(job=None, state=None, now=NOW):
    return problems(state or STATE, JOB if job is None else job, now)


def rebody(job, change):
    job = copy.deepcopy(job)
    build = json.loads(base64.b64decode(job["httpTarget"]["body"]))
    change(build)
    job["httpTarget"]["body"] = base64.b64encode(json.dumps(build).encode()).decode()
    return job


assert refused() == [], refused()

# The schedule is gone, paused by a completed teardown, or in another project.
assert refused(job={})
assert refused(job=dict(JOB, state="PAUSED"))
assert refused(job=dict(JOB, name="projects/p/locations/us-central1/jobs/evidence-teardown"))

# It exists but would not destroy this stack.
assert refused(job=rebody(JOB, lambda build: build["source"]["storageSource"]
                          .update(generation="16")))
assert refused(job=rebody(JOB, lambda build: build["steps"][0]["env"]
                          .__setitem__(1, "WINDOW_END=2026-09-19T20:00:00Z")))
assert refused(job=rebody(JOB, lambda build: build.update(
    serviceAccount="projects/p/serviceAccounts/other@p.iam.gserviceaccount.com")))
assert refused(job=rebody(JOB, lambda build: build["steps"].pop()))

# It exists but is not the identity, endpoint or cadence that was reviewed.
target = dict(JOB["httpTarget"], oauthToken={"serviceAccountEmail": "other@p.iam.gserviceaccount.com"})
assert refused(job=dict(JOB, httpTarget=target))
assert refused(job=dict(JOB, httpTarget=dict(JOB["httpTarget"], uri="https://example.invalid/")))
# A GET reaches the same endpoint and submits nothing.
assert refused(job=dict(JOB, httpTarget=dict(JOB["httpTarget"], httpMethod="GET")))
assert refused(job=dict(JOB, schedule="0 0 1 1 *"))
assert refused(job=dict(JOB, attemptDeadline="1800s"))

# The window is over, or too close to the teardown to build in.
assert refused(now=NOW + timedelta(hours=8))
assert refused(now=datetime(2026, 9, 18, 19, 31, tzinfo=timezone.utc))
assert refused(now=datetime(2026, 9, 18, 19, 29, tzinfo=timezone.utc)) == []

# A state that never recorded the deadline still holds the service default.
default = copy.deepcopy(STATE)
del default["resources"][0]["instances"][0]["attributes"]["attempt_deadline"]
assert refused(state=default) == [], refused(state=default)
assert refused(job=dict(JOB, attemptDeadline="600s"), state=default)

# The control root was never applied, or declares a schedule that never runs.
assert refused(state={"outputs": {}, "resources": []})
paused = copy.deepcopy(STATE)
paused["resources"][0]["instances"][0]["attributes"]["paused"] = True
assert refused(state=paused)

print("window_guard: all refusals hold")
