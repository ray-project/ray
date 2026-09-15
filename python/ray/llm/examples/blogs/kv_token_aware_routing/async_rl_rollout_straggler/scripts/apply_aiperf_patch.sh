#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
DEFAULT_AIPERF_ROOT="${AIPERF_ROOT:-$(cd "$AIPERF_SRC/.." && pwd)}"
PATCH="$SCRIPT_DIR/../patches/aiperf-local-benchmark-support.patch"
EXPECTED_COMMIT=c2f5e9d459005d362457716bbd865d247232fa30

AIPERF_ROOT="$DEFAULT_AIPERF_ROOT"
if [[ $# -eq 2 && "$1" == "--source" ]]; then
  AIPERF_ROOT="$2"
elif [[ $# -ne 0 ]]; then
  echo "Usage: $0 [--source DIRECTORY]" >&2
  exit 2
fi
[[ -d "$AIPERF_ROOT/.git" ]] || { echo "missing AIPerf source: $AIPERF_ROOT" >&2; exit 2; }

[[ "$(git -C "$AIPERF_ROOT" rev-parse HEAD)" == "$EXPECTED_COMMIT" ]] || {
  echo "AIPerf must be pinned at $EXPECTED_COMMIT" >&2; exit 2;
}
if git -C "$AIPERF_ROOT" apply --reverse --check "$PATCH" >/dev/null 2>&1; then
  echo "AIPerf patch already applied"
elif git -C "$AIPERF_ROOT" apply --check "$PATCH" >/dev/null 2>&1; then
  git -C "$AIPERF_ROOT" apply "$PATCH"
else
  echo "AIPerf patch cannot be applied" >&2; exit 2
fi
