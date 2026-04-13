---
name: fetch-buildkite-logs
description: Fetch and analyze Buildkite CI build logs for failures
---

# Fetch Buildkite Logs

## Prerequisites
- BUILDKITE_API_TOKEN must be set in the environment (typically ~/.bashrc)
- If not configured, direct user to doc/source/ray-contribute/agent-development.rst for setup

## Steps
1. Verify token: `echo $BUILDKITE_API_TOKEN | head -c4`
2. If token missing, stop and show setup instructions from the dev docs
3. Fetch build:
   ```bash
   curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/premerge/builds/<BUILD_NUM>"
   ```
4. List failed jobs:
   ```bash
   curl -s -H "Authorization: Bearer $BUILDKITE_API_TOKEN" \
     "https://api.buildkite.com/v2/organizations/ray-project/pipelines/premerge/builds/<BUILD_NUM>" \
     | python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; [print(f\"{j['id']} {j['name']} -> {j['state']}\") for j in jobs if j.get('state') in ('failed','broken')]"
   ```
5. Fetch individual job logs and analyze root causes
6. Summarize failures and suggest fixes
