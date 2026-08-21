---
name: fetch-buildkite-logs
description: Fetch Buildkite CI job logs, and a job's artifacts when the logs are not enough, from a Buildkite build URL or build number, then summarize the failures
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

## Artifacts

If the log does not let you identify the root cause, look for more logs in the job's artifacts.

The token needs the **`read_artifacts`** scope in addition to `read_build_logs`; without it these calls return `HTTP 403`.

1. List a job's artifacts (raise `page` if a full page of 100 comes back):
   ```bash
   curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/<PIPELINE>/builds/<BUILD_NUM>/jobs/<JOB_ID>/artifacts?per_page=100&page=1"
   ```
2. Download one by its `id`:
   ```bash
   curl -fsL -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/<PIPELINE>/builds/<BUILD_NUM>/jobs/<JOB_ID>/artifacts/<ARTIFACT_ID>/download" \
     -o /tmp/<ARTIFACT_ID>.zip
   ```
   `-L` is required: the endpoint returns HTTP 302 and redirects to S3. Keep the `Authorization` header — curl drops it on the cross-host hop, which is what S3 wants (sending it yields `400 InvalidRequest`).

## Authentication note

If `curl` returns `{"message":"No organization found"}`, the configured token does not have access to `ray-project`. The user may have a separate org-scoped token — ask them which env var to source.
