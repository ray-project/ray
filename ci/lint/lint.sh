#!/bin/bash
#
# This script runs all the lint checks.
#

set -exuo pipefail

clang_format() {
  pip install -c python/requirements_compiled.txt clang-format
  ./ci/lint/check-git-clang-format-output.sh
}

pre_commit() {
  # Run pre-commit on all files
  # TODO(MortalHappiness): Run all pre-commit checks because currently we only run some of them.
  pip install -c python/requirements_compiled.txt pre-commit clang-format
  pre-commit run ruff --all-files --show-diff-on-failure
  pre-commit run check-added-large-files --all-files --show-diff-on-failure
  pre-commit run check-ast --all-files --show-diff-on-failure
  pre-commit run check-toml --all-files --show-diff-on-failure
  pre-commit run black --all-files --show-diff-on-failure
  pre-commit run prettier --all-files --show-diff-on-failure
  pre-commit run mypy --all-files --show-diff-on-failure
  pre-commit run rst-directive-colons --all-files --show-diff-on-failure
  pre-commit run rst-inline-touching-normal --all-files --show-diff-on-failure
  pre-commit run python-check-mock-methods --all-files --show-diff-on-failure
  pre-commit run clang-format --all-files --show-diff-on-failure
  pre-commit run docstyle --all-files --show-diff-on-failure
  pre-commit run check-import-order --all-files --show-diff-on-failure
  pre-commit run check-cpp-files-inclusion --all-files --show-diff-on-failure
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
  TMP_DIR="$(mktemp -d)"
  bazelisk query 'kind("cc_test|py_test", //...)' --output=xml > "${TMP_DIR}/tests.xml"
  bazelisk run //ci/lint:check_bazel_team_owner < "${TMP_DIR}/tests.xml"
  rm -rf "${TMP_DIR}"
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
  RAY_DISABLE_EXTRA_CPP=1 pip install -e "python[all]"
  ./ci/lint/check_api_annotations.py
}

api_policy_check() {
  # install ray and compile doc to generate API files
  make -C doc/ html
  RAY_DISABLE_EXTRA_CPP=1 pip install -e "python[all]"
  # validate the API files
  bazel run //ci/ray_ci/doc:cmd_check_api_discrepancy -- /ray "$@"
}

documentation_style() {
  ./ci/lint/check-documentation-style.sh
}

"$@"
