#!/usr/bin/env bash

set -euxo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/.."
WORKSPACE_DIR="${ROOT_DIR}/.."

readme() {
  if python -s -c "import docutils" >/dev/null 2>/dev/null; then
    (
      cd "${WORKSPACE_DIR}"/python
      python setup.py check --restructuredtext --strict --metadata
    )
  else
    echo "Skipping README lint because the docutils package is not installed" 1>&2
  fi
}

code_format() {
  FORMAT_SH_PRINT_DIFF=1 "${ROOT_DIR}"/lint/format.sh --all-scripts
}

banned_words() {
  "${ROOT_DIR}"/lint/check-banned-words.sh
}

annotations() {
  "${ROOT_DIR}"/lint/check_api_annotations.py
}

run_semgrep() {
  semgrep ci --config "${WORKSPACE_DIR}/semgrep.yml"
}

bazel_buildifier() {
  LINT_BAZEL_TMP="$(mktemp -d)"
  curl -sl "https://github.com/bazelbuild/buildtools/releases/download/v6.1.2/buildifier-linux-amd64" \
    -o "${LINT_BAZEL_TMP}/buildifier"
  chmod +x "${LINT_BAZEL_TMP}/buildifier"
  BUILDIFIER="${LINT_BAZEL_TMP}/buildifier" "${ROOT_DIR}/lint/bazel-format.sh"

  rm -rf "${LINT_BAZEL_TMP}"  # Clean up
}

bazel_pytest() {
  pip install yq
  cd "${WORKSPACE_DIR}"
  for team in "team:core" "team:ml" "team:rllib" "team:serve"; do
    # this does the following:
    # - find all py_test rules in bazel that have the specified team tag EXCEPT ones with "no_main" tag and outputs them as xml
    # - converts the xml to json
    # - feeds the json into pytest_checker.py
    bazel query "kind(py_test.*, tests(python/...) intersect attr(tags, \"\b$team\b\", python/...) except attr(tags, \"\bno_main\b\", python/...))" --output xml | xq | python ci/lint/pytest_checker.py
  done
}

copyright() {
  (
    "${ROOT_DIR}"/lint/copyright-format.sh -c
  )
}

clang() {
  if command -v clang-format > /dev/null; then
    "${ROOT_DIR}"/lint/check-git-clang-format-output.sh
  else
    { echo "WARNING: Skipping linting C/C++ as clang-format is not installed."; } 2> /dev/null
  fi
}

test_script() {
  # lint test script
  pushd "${WORKSPACE_DIR}"
    bazel query 'kind("cc_test", //...)' --output=xml | python "${ROOT_DIR}"/lint/check-bazel-team-owner.py
    bazel query 'kind("py_test", //...)' --output=xml | python "${ROOT_DIR}"/lint/check-bazel-team-owner.py
  popd
}

test_run() {
  # Make sure tests will be run by CI.
  python "${ROOT_DIR}"/pipeline/check-test-run.py
}

LINT=1 "${ROOT_DIR}"/env/install-dependencies.sh
"$@"
