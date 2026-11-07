#!/usr/bin/env bash
set -euo pipefail

# Registry/build operations must not inherit a host-only proxy.
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
exec python3 "$(dirname "$0")/hive_auth/run.py" "$@"
