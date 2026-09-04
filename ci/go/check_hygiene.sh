#!/usr/bin/env bash
# Open-source hygiene checks for the Go runtime and its C++ bridge.
#
# Checks (fails fast, exits non-zero on any violation):
#   1. Apache-2.0 license header on every .go/.cc/.h/BUILD.bazel file
#   2. No CJK characters (comments and logs must be English)
#   3. No internal identifiers (internal artifact hosts, internal IPs, internal task tags)
#   4. gofmt cleanliness of the go/ tree
#
# Usable both locally (`ci/go/check_hygiene.sh`) and in GitHub Actions.
set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

TARGETS=(go src/ray/core_worker/lib/go)
fail=0

check_headers() {
  local f
  while IFS= read -r f; do
    if ! head -3 "$f" | grep -q "Apache License"; then
      echo "MISSING LICENSE HEADER: $f"
      fail=1
    fi
  done < <(find "${TARGETS[@]}" -type f \( -name '*.go' -o -name '*.cc' -o -name '*.h' -o -name 'BUILD.bazel' \) 2>/dev/null)
}

check_no_cjk() {
  local f
  while IFS= read -r f; do
    if grep -qP '[\x{4e00}-\x{9fff}]' "$f"; then
      echo "CONTAINS CJK CHARACTERS: $f"
      fail=1
    fi
  done < <(find "${TARGETS[@]}" -type f \( -name '*.go' -o -name '*.cc' -o -name '*.h' \) 2>/dev/null)
}

check_no_internal_ids() {
  local f
  while IFS= read -r f; do
    if grep -qiE 'artnj|10\.166\.|RDC:' "$f"; then
      echo "CONTAINS INTERNAL IDENTIFIER: $f"
      fail=1
    fi
  done < <(find "${TARGETS[@]}" -type f \( -name '*.go' -o -name '*.cc' -o -name '*.h' -o -name 'BUILD.bazel' \) 2>/dev/null)
}

check_gofmt() {
  local out
  out="$(gofmt -l go 2>/dev/null)"
  if [ -n "$out" ]; then
    echo "GOFMT NEEDED:"
    echo "$out"
    fail=1
  fi
}

check_headers
check_no_cjk
check_no_internal_ids
check_gofmt

if [ "$fail" -eq 0 ]; then
  echo "go runtime hygiene checks passed"
fi
exit "$fail"
