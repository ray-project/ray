#!/bin/bash
#
# Run the >>> docstring examples in only the modules a pull request changed,
# across every library, in the docstringbuild image with the PR's Ray installed.
# Used by the "docstring-only" label's "doc: docstring examples" step in
# .buildkite/others.rayci.yml.
#
# Usage: run_docstring_examples.sh <build name> <python version>
#
# Lists the changed modules under python/ray on the CI host, keeps the ones a
# CPU doctest() target covers, then builds the image with the PR's Ray via
# test_in_docker --build-only, the same Ray install every library test step
# uses, and runs ci/ray_ci/doc/run_docstring_examples.py on those modules in
# it. Exits 0 without building an image when no covered module changed.

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <build name> <python version>"
  exit 2
fi
build_name="$1"
python_version="$2"
source_dir="python/ray"

echo "--- Listing the changed modules under ${source_dir}"
files="$(python ci/ray_ci/doc/run_docstring_examples.py \
  --source-dir "${source_dir}" --list-changed)"

if [[ -z "${files}" ]]; then
  echo "No modules changed under ${source_dir}; no docstring examples to run."
  exit 0
fi

echo "--- Keeping the modules a CPU doctest() target covers"
# The files CPU doctest() targets run. The docstringbuild image is CPU-only,
# and an example no doctest() target runs isn't held to running here either.
labels="$(mktemp)"
log="$(mktemp)"
trap 'rm -f "${labels}" "${log}"' EXIT
bazel query --output=label \
  'labels(data, attr(tags, "\bcpu\b", attr(tags, "\bdoctest\b", tests(//python/ray/...))))' \
  > "${labels}"
# shellcheck disable=SC2086 # Word splitting is intended: one file per word.
files="$(python ci/ray_ci/doc/run_docstring_examples.py \
  --doctest-labels "${labels}" --list-changed ${files})"

if [[ -z "${files}" ]]; then
  echo "No changed module is covered by a CPU doctest() target; nothing to run."
  exit 0
fi
echo "${files}"

echo "--- Building the ${build_name} image with this PR's Ray"
bazel run //ci/ray_ci:test_in_docker -- "//${source_dir}/..." none \
  --build-only --build-name "${build_name}" --python-version "${python_version}"

echo "--- Running the docstring examples"
# A login shell loads the image's Python environment, as the java tests step does.
status=0
docker run -i --rm --shm-size=2.5gb \
  "${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${build_name}" \
  /bin/bash -iecuo pipefail \
  "python ci/ray_ci/doc/run_docstring_examples.py ${files//$'\n'/ }" \
  | tee "${log}" || status=$?

# Repeat the examples skipped for an omitted library, so they're visible
# without reading the full log. The step's container can't reach
# buildkite-agent to annotate the build. On a pass, they go in a "+++" section,
# which Buildkite opens expanded. On a failure, Buildkite expands the last
# section, so they stay in the examples section and the failures stay in view.
skips="$(grep '^SKIP .*needs ' "${log}" || true)"
if [[ -n "${skips}" ]]; then
  header="Docstring examples skipped: the ${build_name} image omits a library they need"
  if [[ "${status}" -eq 0 ]]; then
    echo "+++ ${header}"
  else
    echo "${header}"
  fi
  echo "They run in their library's doctest() target instead."
  echo "${skips}"
fi
exit "${status}"
