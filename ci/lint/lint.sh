#!/bin/bash
#
# This script runs all the lint checks.
#

set -exuo pipefail

clang_format() {
  pip install -c python/requirements_compiled.txt clang-format
  ./ci/lint/check-git-clang-format-output.sh
}

code_format() {
  pip install -c python/requirements_compiled.txt -r python/requirements/lint-requirements.txt
  FORMAT_SH_PRINT_DIFF=1 ./ci/lint/format.sh --all-scripts
}

untested_code_snippet() {
  pip install -c python/requirements_compiled.txt semgrep
  semgrep ci --config semgrep.yml
}

banned_words() {
  ./ci/lint/check-banned-words.sh
}

doc_readme() {
  pip install -c python/requirements_compiled.txt docutils
  cd python && python setup.py check --restructuredtext --strict --metadata
}

dashboard_format() {
  ./ci/lint/check-dashboard-format.sh
}

copyright_format() {
  ./ci/lint/copyright-format.sh -c
}

bazel_team() {
  bazel query 'kind("cc_test", //...)' --output=xml | python ./ci/lint/check-bazel-team-owner.py
  bazel query 'kind("py_test", //...)' --output=xml | python ./ci/lint/check-bazel-team-owner.py
}

bazel_buildifier() {
  ./ci/lint/check-bazel-buildifier.sh
}

pytest_format() {
  pip install -c python/requirements_compiled.txt yq
  ./ci/lint/check-pytest-format.sh
}

test_coverage() {
  python ci/pipeline/check-test-run.py
}

api_annotations() {
  # shellcheck disable=SC2102
  RAY_DISABLE_EXTRA_CPP=1 pip install -e python/[all]
  ./ci/lint/check_api_annotations.py
}

documentation_style() {
  ./ci/lint/check-documentation-style.sh
}

"$@"
