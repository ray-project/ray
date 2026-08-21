---
name: fetch-buildkite-logs
description: Fetch Buildkite CI job logs, and a job's uploaded artifacts (such as the failed_test_logs archives), from a Buildkite build URL or build number, then summarize the failures
---

# Fetch Buildkite Logs

## Prerequisites
- `BUILDKITE_API_TOKEN` must be set in the environment (typically `~/.bashrc`)
- If not configured, direct user to `doc/source/ray-contribute/agent-development.md` for setup

## Parsing the Buildkite URL

A Buildkite URL has the form:

```
https://buildkite.com/ray-project/<PIPELINE>/builds/<BUILD_NUM>#<JOB_ID>
```

Always extract `<PIPELINE>` (e.g. `premerge`, `postmerge`) and `<BUILD_NUM>` from the URL the user provides. **Do not hardcode `premerge`** — the same skill is used for all pipelines. If a `#<JOB_ID>` fragment is present, it identifies a specific real job (not a group/wait job) and can be queried directly.

## Steps

1. Verify token: `[ -n "$BUILDKITE_API_TOKEN" ] && echo "token set" || echo "token MISSING"`
   (do not echo the token itself — a command that prints secret characters gets blocked)
2. If token missing, stop and show setup instructions from the dev docs
3. Fetch build (use the pipeline from the URL):
   ```bash
   curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/<PIPELINE>/builds/<BUILD_NUM>"
   ```
4. If a job ID is present in the URL fragment, look it up directly:
   ```bash
   curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/<PIPELINE>/builds/<BUILD_NUM>" \
     | python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; [print(f\"{j['id']} {j.get('name')} -> {j.get('state')}\") for j in jobs if j['id']=='<JOB_ID>']"
   ```
5. Otherwise list failed/broken jobs:
   ```bash
   curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/<PIPELINE>/builds/<BUILD_NUM>" \
     | python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; [print(f\"{j['id']} {j.get('name')} -> {j['state']}\") for j in jobs if j.get('state') in ('failed','broken')]"
   ```
6. Fetch individual job log:
   ```bash
   curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/<PIPELINE>/builds/<BUILD_NUM>/jobs/<JOB_ID>/log" \
     > /tmp/log_<JOB_ID>.json
   ```
   Logs come back as JSON with a `content` field containing ANSI escape codes — strip them with `re.sub(r'\x1b\[[0-9;]*m', '', content)` before grepping.
7. Summarize failures and suggest fixes.

## Artifacts (only when the log is not enough)

**Do not download artifacts routinely.** The job log answers most questions. Reach for artifacts only when it demonstrably does not — a hang/timeout with no pytest failure, or a traceback truncated right where the cause would be. Ray uploads a `failed_test_logs` zip per failing test: a whole Ray session log directory (`raylet.out`, `gcs_server.out`, `python-core-worker-*.log`, `events/`).

Requires the token to have the **`read_artifacts`** scope — `read_build_logs` alone is not enough, and the API answers `HTTP 403` if it is missing.

1. List a job's artifacts:
   ```bash
   curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/<PIPELINE>/builds/<BUILD_NUM>/jobs/<JOB_ID>/artifacts?per_page=100" \
     | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(f\"API error: {d}\") if isinstance(d,dict) else [print(f\"{a['id']}  {a['state']:>8}  {a['file_size']:>9}  {a['filename']}\") for a in d]"
   ```
   An empty list is a normal result, not an error — a shard where nothing failed uploads nothing. Only artifacts with `state` `finished` are downloadable.

   `per_page=100` is the API maximum, and Ray uploads a zip per failing test, so a busy shard overflows one page. If exactly 100 come back, there are probably more: append `&page=2`, `&page=3`, ... until a page comes back short. (The response's `Link` header says `rel="next"` while pages remain.)
2. Download one, by the `id` from that listing:
   ```bash
   curl -sL -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/<PIPELINE>/builds/<BUILD_NUM>/jobs/<JOB_ID>/artifacts/<ARTIFACT_ID>/download" \
     -o "/tmp/<FILENAME>"
   ```
   `-L` is required — the endpoint 302s to S3, and without it you get a 300-byte redirect body instead of the artifact. Keep the `Authorization` header on: curl drops it on the cross-host hop, which is what S3 wants (sending it yields `400 InvalidRequest`).
3. `unzip` the archive before reading it — grepping the `.zip` itself only matches the archive's filename table, not the logs inside.

## Authentication note

If `curl` returns `{"message":"No organization found"}`, the configured token does not have access to `ray-project`. The user may have a separate org-scoped token — ask them which env var to source.
