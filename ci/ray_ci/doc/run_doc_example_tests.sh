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

# Hand the selector the same --except-tags that test_in_docker gets, so a
# target those tags would drop is reported as not run instead of logged as
# running.
except_tags=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  if [[ "${args[i]}" == "--except-tags" && $((i + 1)) -lt ${#args[@]} ]]; then
    except_tags="${args[i + 1]}"
  fi
done

# Diff against FETCH_HEAD rather than origin/<base>: a clone with a restricted
# refspec may never create a local origin/<base>. Same pattern as the redirect
# check in .buildkite/doc.rayci.yml. No `|| true`, so a failed fetch fails the
# step instead of selecting nothing.
echo "--- Fetching the pull request's base branch"
git fetch -q origin "${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-master}"

echo "--- Selecting doc example tests for the changed files"
selection="$(bazel run --noshow_progress //ci/ray_ci/doc:cmd_doc_example_targets -- \
  --base FETCH_HEAD --except-tags "${except_tags}")"

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
