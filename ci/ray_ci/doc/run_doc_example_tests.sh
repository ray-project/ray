#!/bin/bash
#
# Run the doc example tests that a pull request's changed doc files feed
# directly. Used by the "docs-example-test" opt-in step in
# .buildkite/others.rayci.yml.
#
# Usage: run_doc_example_tests.sh [test_in_docker flags...]
#
# Selects targets with //ci/ray_ci/doc:cmd_doc_example_targets, which never
# falls back to a library's whole docs example suite. test_in_docker filters
# targets by team, so this runs it once per team that has targets, all with the
# same flags and so in the same environment. --exact-targets stops microcheck
# from narrowing the selection further. Exits 0 without starting a container
# when no target names a changed file.

set -euo pipefail

echo "--- Selecting doc example tests for the changed files"
selection="$(bazel run --noshow_progress //ci/ray_ci/doc:cmd_doc_example_targets)"

if [[ -z "${selection}" ]]; then
  echo "--- No doc example tests name the changed files"
  exit 0
fi

teams=()
while read -r team; do
  teams+=("${team}")
done < <(cut -d' ' -f1 <<< "${selection}" | sort -u)

status=0
for team in "${teams[@]}"; do
  targets=()
  while read -r target_team target; do
    if [[ "${target_team}" == "${team}" ]]; then
      targets+=("${target}")
    fi
  done <<< "${selection}"

  echo "--- Running ${#targets[@]} ${team} doc example tests"
  bazel run //ci/ray_ci:test_in_docker -- "${targets[@]}" "${team}" \
    --exact-targets "$@" || status=$?
done

exit "${status}"
