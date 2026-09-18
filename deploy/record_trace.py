"""Traces a captured record from Bigtable through the API back to Bluesky.

An HTTP 200 proves nothing on its own. This takes entities the pipeline
actually captured, asks the private Cloud Run API for them with a Google-signed
identity token, and checks the answer against the record as Bluesky serves it
now. Likes carry the subject of the liked post, which is what the source can
confirm independently.

  PROJECT_ID=... python3 deploy/record_trace.py /workspace/outputs.json
"""

import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

ENTITY_TYPE = "app.bsky.feed.like"
CANDIDATES = 10
METADATA = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default"
BLUESKY = "https://public.api.bsky.app/xrpc/com.atproto.repo.getRecord"
# The source's own words for "this record is no longer there", as opposed to
# "the source could not answer".
GONE = {"RecordNotFound", "RepoNotFound", "RepoDeactivated", "RepoTakendown",
        "AccountDeactivated", "AccountTakedown"}


def fetch(url, headers=None, data=None):
    """Returns (status, parsed body). A 4xx is an answer here, not a crash."""
    request = urllib.request.Request(url, data=data, headers=headers or {},
                                     method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        body = error.read()
        try:
            return error.code, json.loads(body or b"null")
        except ValueError:
            return error.code, body.decode("utf-8", "replace")


def credential(path):
    request = urllib.request.Request(METADATA + path, headers={"Metadata-Flavor": "Google"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode()


def captured(project, instance, table, access):
    """Entity ids (did/rkey) of the likes currently in the table."""
    prefix = ENTITY_TYPE + "#"
    query = {
        "rows": {"rowRanges": [{
            "startKeyClosed": base64.b64encode(prefix.encode()).decode(),
            "endKeyOpen": base64.b64encode((ENTITY_TYPE + "$").encode()).decode(),
        }]},
        "filter": {"stripValueTransformer": True},
        "rowsLimit": CANDIDATES,
    }
    url = "https://bigtable.googleapis.com/v2/projects/{}/instances/{}/tables/{}:readRows".format(
        project, instance, table)
    status, body = fetch(url,
                         {"Authorization": "Bearer " + access,
                          "Content-Type": "application/json"},
                         json.dumps(query).encode())
    if status != 200:
        raise SystemExit("Bigtable read failed: {} {}".format(status, body))
    keys = []
    for part in body or []:
        for chunk in part.get("chunks", []):
            key = chunk.get("rowKey")
            if key:
                key = base64.b64decode(key).decode()
                if key.startswith(prefix) and key not in keys:
                    keys.append(key)
    return [key[len(prefix):] for key in keys]


def verify(event, status, record):
    """Why the served record does or does not match the source record.

    Returns None when the source confirms it, otherwise (blame, reason).
    `blame` is "record" when this candidate is unusable, which a record deleted
    or rewritten after capture legitimately is, and "source" when Bluesky did
    not answer for it: an outage or a rate limit says nothing about the record.
    """
    if status != 200:
        if status in (400, 404) and (record or {}).get("error") in GONE:
            return ("record", "gone from the source since capture: " + record["error"])
        return ("source", "the source answered HTTP {} ({})".format(
            status, (record or {}).get("error", "no error given")))
    subject = record.get("value", {}).get("subject", {}).get("uri")
    if not subject:
        return ("record", "the source record carries no subject")
    if event.get("state") != "active":
        return ("record",
                "served as {}, so the source should not still have it".format(event.get("state")))
    if event.get("subject") != subject:
        return ("record",
                "served subject {}, source says {}".format(event.get("subject"), subject))
    return None


def self_check():
    served = {"state": "active", "subject": "at://did:plc:a/app.bsky.feed.post/abc"}
    source = {"value": {"subject": {"uri": served["subject"]}}}
    assert verify(served, 200, source) is None
    assert verify(served, 200, {"value": {"subject": {"uri": "at://other"}}})[0] == "record"
    assert verify(served, 200, {"value": {}})[0] == "record"
    assert verify({"state": "deleted", "subject": served["subject"]}, 200, source)[0] == "record"
    assert verify(served, 400, {"error": "RecordNotFound"})[0] == "record"
    # An outage or a rate limit is not a deletion.
    assert verify(served, 503, None)[0] == "source"
    assert verify(served, 429, {"error": "RateLimitExceeded"})[0] == "source"
    assert verify(served, 400, {"error": "InvalidRequest"})[0] == "source"


def main(argv):
    self_check()
    project = os.environ["PROJECT_ID"]
    outputs = {name: value["value"] for name, value in
               json.load(open(argv[1], encoding="utf-8")).items()}
    api = outputs["api_url"].rstrip("/") + "/v1/current-state"

    status, _ = fetch(api + "?entity_type=" + ENTITY_TYPE + "&entity_id=none")
    if status not in (401, 403):
        raise SystemExit("the API answered {} without a token; it must stay private".format(status))
    print("ACCEPT unauthenticated request rejected with", status)

    identity = credential("/identity?audience=" + urllib.parse.quote(outputs["api_url"], safe=""))
    access = json.loads(credential("/token"))["access_token"]
    entities = captured(project, outputs["bigtable_instance"], outputs["bigtable_table"], access)
    if not entities:
        raise SystemExit("the table holds no captured " + ENTITY_TYPE + " record yet")

    skipped = []
    for entity_id in entities:
        status, event = fetch(
            "{}?entity_type={}&entity_id={}".format(
                api, ENTITY_TYPE, urllib.parse.quote(entity_id, safe="")),
            {"Authorization": "Bearer " + identity})
        if status == 404:
            skipped.append((entity_id, "record", "no longer served: purged or superseded"))
            continue
        if status != 200:
            raise SystemExit("the API answered {} for {}: {}".format(status, entity_id, event))
        if event.get("entity_type") != ENTITY_TYPE or event.get("entity_id") != entity_id:
            raise SystemExit("the API answered with {} {}, asked for {}".format(
                event.get("entity_type"), event.get("entity_id"), entity_id))
        did, _, rkey = entity_id.partition("/")
        source_status, source = fetch("{}?repo={}&collection={}&rkey={}".format(
            BLUESKY, urllib.parse.quote(did), ENTITY_TYPE, urllib.parse.quote(rkey)))
        problem = verify(event, source_status, source)
        if problem:
            skipped.append((entity_id,) + problem)
            continue
        print("ACCEPT {} {} revision {} subject {}, confirmed by the source".format(
            ENTITY_TYPE, entity_id, event["revision"], event["subject"]))
        return 0

    for entity_id, blame, reason in skipped:
        print("skipped {}: {}".format(entity_id, reason))
    if all(blame == "source" for _, blame, _ in skipped):
        raise SystemExit("the source answered for none of the {} candidates; "
                         "acceptance is undecided, not failed".format(len(skipped)))
    raise SystemExit("no captured record could be confirmed against the source")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
