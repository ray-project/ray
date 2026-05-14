#!/usr/bin/env bash
# Emit a JSON array of {name, dir} entries for every template in
# anyscale/templates/BUILD.yaml on main. Designed to run from CI: pulls the
# file straight from raw.githubusercontent.com so we don't need a local clone
# (and don't need to provision a checkout of anyscale/templates in the runner).
#
# BUILD_URL is overridable for local testing against a fork or branch.
set -euo pipefail

BUILD_URL="${BUILD_URL:-https://raw.githubusercontent.com/anyscale/templates/main/BUILD.yaml}"

curl -fsSL "$BUILD_URL" | python3 -c '
import json, sys, yaml
data = yaml.safe_load(sys.stdin)
print(json.dumps([{"name": t["name"], "dir": t["dir"]} for t in data], indent=2))
print(f"{len(data)} templates from BUILD.yaml on main", file=sys.stderr)
'
