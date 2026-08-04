#!/usr/bin/env bash
# Checks bazel buildifier format

set -euxo pipefail

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)/../.."
cd "${WORKSPACE_DIR}"
for team in "team:core" "team:ml" "team:rllib" "team:serve" "team:llm"; do
    # this does the following:
    # - find all py_test rules in bazel that have the specified team tag EXCEPT ones with "no_main" tag and outputs them as xml
    # - converts the xml to json
    # - feeds the json into pytest_checker.py
    bazel query "kind(py_test.*, tests(python/...) intersect attr(tags, \"\b$team\b\", python/...) except attr(tags, \"\bno_main\b\", python/...))" --output xml | xq | python ci/lint/pytest_checker.py
done
