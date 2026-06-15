---
name: fetch-buildkite-logs
description: Fetch and analyze Buildkite CI build logs for failures
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

1. Verify token: `echo $BUILDKITE_API_TOKEN | head -c4`
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
     | python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; [print(f\"{j['id']} {j['name']} -> {j['state']}\") for j in jobs if j.get('state') in ('failed','broken')]"
   ```
6. Fetch individual job log:
   ```bash
   curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/<PIPELINE>/builds/<BUILD_NUM>/jobs/<JOB_ID>/log" \
     > /tmp/log_<JOB_ID>.json
   ```
   Logs come back as JSON with a `content` field containing ANSI escape codes — strip them with `re.sub(r'\x1b\[[0-9;]*m', '', content)` before grepping.
7. Summarize failures and suggest fixes.

## Authentication note

If `curl` returns `{"message":"No organization found"}`, the configured token does not have access to `ray-project`. The user may have a separate org-scoped token — ask them which env var to source.
