#!/usr/bin/env bash
# Checks that every py_test source file under python/ contains the
# `if __name__ == "__main__":` pytest snippet.

set -euxo pipefail

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)/../.."
cd "${WORKSPACE_DIR}"

# Read the team list from the team-owner checker rather than hardcoding here
while read -r team; do
    # this does the following:
    # - find all py_test rules in bazel that have the specified team tag EXCEPT ones with "no_main" tag and outputs them as xml
    # - converts the xml to json
    # - feeds the json into pytest_checker.py
    bazel query "kind(py_test.*, tests(python/...) intersect attr(tags, \"\bteam:$team\b\", python/...) except attr(tags, \"\bno_main\b\", python/...))" --output xml | xq | python ci/lint/pytest_checker.py
done < <(python ci/lint/check_bazel_team_owner.py --print-teams)
