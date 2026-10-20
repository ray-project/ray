#!/bin/bash
#
# Run the >>> docstring examples in only the library modules a pull request
# changed, in the library's own CI image with the PR's Ray installed. Used by
# the "docstring-only" label's per-library steps, such as
# "data: docstring examples" in .buildkite/data.rayci.yml.
#
# Usage: run_docstring_examples.sh <source dir> <team> <build name> <python version>
#
# Lists the changed modules on the CI host, then builds the library's test image
# with test_in_docker --build-only, the same Ray install every library test step
# uses, and runs ci/ray_ci/doc/run_docstring_examples.py on those modules in it.
# Exits 0 without building an image when no module changed.

set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "Usage: $0 <source dir> <team> <build name> <python version>"
  exit 2
fi
source_dir="${1%/}"
team="$2"
build_name="$3"
python_version="$4"

echo "--- Listing the changed modules under ${source_dir}"
files="$(python ci/ray_ci/doc/run_docstring_examples.py \
  --source-dir "${source_dir}" --list-changed)"

if [[ -z "${files}" ]]; then
  echo "No modules changed under ${source_dir}; no docstring examples to run."
  exit 0
fi
echo "${files}"

echo "--- Building the ${build_name} image with this PR's Ray"
bazel run //ci/ray_ci:test_in_docker -- "//${source_dir}/..." "${team}" \
  --build-only --build-name "${build_name}" --python-version "${python_version}"

echo "--- Running the docstring examples"
# A login shell loads the image's Python environment, as the java tests step does.
docker run -i --rm --shm-size=2.5gb \
  "${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${build_name}" \
  /bin/bash -iecuo pipefail \
  "python ci/ray_ci/doc/run_docstring_examples.py ${files//$'\n'/ }"
