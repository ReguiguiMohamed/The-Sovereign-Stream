"""Refuses an evidence apply unless the live teardown schedule will run it.

Saved Terraform outputs only say what was applied once. This compares the
Cloud Scheduler job as it exists now with the control configuration that
declared it, and with the window those outputs describe.

  python3 infra/window_guard.py control.tfstate job.json
  -> "<window_end> <bundle_generation>" on stdout, or the reasons on stderr.
"""

import base64
import json
import sys
from datetime import datetime, timedelta, timezone

JOB = "evidence-teardown"
# An apply is refused this close to the teardown, so the window cannot end
# while resources are still being created.
MARGIN = timedelta(minutes=30)


def declared(state):
    """The scheduler job as the control root last applied it."""
    for resource in state.get("resources", []):
        if (resource.get("type") == "google_cloud_scheduler_job"
                and resource.get("name") == "teardown"):
            return resource["instances"][0]["attributes"]
    return None


def outputs(state):
    return {name: value["value"] for name, value in state.get("outputs", {}).items()}


def body(encoded):
    return json.loads(base64.b64decode(encoded))


def problems(state, job, now):
    """Every reason this window must not be applied, most fundamental first."""
    found = []
    want = declared(state)
    if want is None:
        return ["the control root holds no teardown schedule: apply infra/control first"]
    out = outputs(state)
    full_name = "projects/{}/locations/{}/jobs/{}".format(
        want["project"], want["region"], want["name"])
    if want["name"] != JOB:
        found.append("the control root schedules {}, not {}".format(want["name"], JOB))
    if want.get("paused"):
        found.append("the control root declares the schedule paused")

    if not job:
        return found + ["scheduler job {} does not exist".format(full_name)]
    if job.get("name") != full_name:
        return found + ["scheduler job is {}, expected {}".format(job.get("name"), full_name)]
    if job.get("state") != "ENABLED":
        found.append("scheduler job is {}, not ENABLED".format(job.get("state")))

    target = job.get("httpTarget", {})
    wanted_target = want["http_target"][0]
    for label, live, declared_value in [
        ("schedule", job.get("schedule"), want["schedule"]),
        ("time zone", job.get("timeZone"), want["time_zone"]),
        # Cloud Scheduler's own default when the configuration leaves it out.
        ("attempt deadline", job.get("attemptDeadline"),
         want.get("attempt_deadline") or "180s"),
        ("build submission uri", target.get("uri"), wanted_target["uri"]),
        ("http method", target.get("httpMethod"), wanted_target["http_method"]),
        ("dispatch identity",
         target.get("oauthToken", {}).get("serviceAccountEmail"),
         wanted_target["oauth_token"][0]["service_account_email"]),
    ]:
        if live != declared_value:
            found.append("scheduler {} is {!r}, declared {!r}".format(
                label, live, declared_value))

    live_build = body(target["body"]) if target.get("body") else None
    if live_build is None:
        found.append("the scheduler job submits no build")
    else:
        if live_build != body(wanted_target["body"]):
            found.append("the scheduled build differs from the control configuration")
        source = live_build.get("source", {}).get("storageSource", {})
        if source.get("generation") != out.get("bundle_generation"):
            found.append("scheduled teardown bundle is generation {}, pinned {}".format(
                source.get("generation"), out.get("bundle_generation")))
        steps = live_build.get("steps") or [{}]
        window = [value for value in steps[0].get("env", [])
                  if value.startswith("WINDOW_END=")]
        if window != ["WINDOW_END=" + str(out.get("window_end"))]:
            found.append("the scheduled teardown carries {}, outputs say {}".format(
                window, out.get("window_end")))

    end = datetime.fromisoformat(out["window_end"].replace("Z", "+00:00"))
    if end - now < MARGIN:
        found.append("the window ends {}, less than {} from now".format(
            out["window_end"], MARGIN))
    return found


def main(argv):
    state = json.load(open(argv[1], encoding="utf-8"))
    live = open(argv[2], encoding="utf-8").read().strip()
    found = problems(state, json.loads(live) if live else {}, datetime.now(timezone.utc))
    if found:
        for reason in found:
            print("refusing to apply: " + reason, file=sys.stderr)
        return 1
    out = outputs(state)
    print(out["window_end"], out["bundle_generation"])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
