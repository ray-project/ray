#!/usr/bin/env bash
# Emit JSON array of {name, dir} from anyscale/templates/BUILD.yaml on main.
# Pulled via raw.githubusercontent.com so CI doesn't need a checkout.
# BUILD_URL overridable for local testing.
set -euo pipefail

BUILD_URL="${BUILD_URL:-https://raw.githubusercontent.com/anyscale/templates/main/BUILD.yaml}"

curl -fsSL "$BUILD_URL" | python3 -c '
import json, sys, yaml
data = yaml.safe_load(sys.stdin)
print(json.dumps([{"name": t["name"], "dir": t["dir"]} for t in data], indent=2))
print(f"{len(data)} templates from BUILD.yaml on main", file=sys.stderr)
'
