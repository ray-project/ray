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

  HOOKS=(
    python-no-log-warn
    ruff
    check-added-large-files
    check-ast
    check-toml
    black
    prettier
    mypy
    pyrefly-serve
    rst-directive-colons
    rst-inline-touching-normal
    python-check-mock-methods
    clang-format
    shellcheck
    docstyle
    check-import-order
    check-cpp-files-inclusion
    end-of-file-fixer
    check-json
    trailing-whitespace
    cpplint
    buildifier
    buildifier-lint
    eslint
  )

  for HOOK in "${HOOKS[@]}"; do
    pre-commit run "$HOOK" --all-files --show-diff-on-failure
  done
}

# The subset of the pre_commit hooks above that reaches Markdown and
# reStructuredText. Cross-referencing the `files` and `types` filters in
# .pre-commit-config.yaml, exactly these three have no filter and so apply to
# prose; every other hook in the list is scoped to Python, C++, BUILD files,
# TS/TSX, or shell. (prettier is `files: doc/` but `types: [javascript, ts,
# tsx, html, css]`, so it does not touch prose either.)
#
# This exists so a documentation-prose pull request keeps whitespace and
# end-of-file checking without paying for the full pre_commit step. That step
# runs 23 hooks serially, each with --all-files, which makes its cost
# independent of the diff: it took 443s on a one-file Markdown change.
#
# Prose style linting is not here; `documentation_style` runs vale directly via
# ci/lint/check-documentation-style.sh as its own step.
pre_commit_docs() {
  pip install -c python/requirements_compiled.txt pre-commit

  HOOKS=(
    trailing-whitespace
    end-of-file-fixer
    check-added-large-files
  )

  for HOOK in "${HOOKS[@]}"; do
    pre-commit run "$HOOK" --all-files --show-diff-on-failure
  done
}

pre_commit_pydoclint() {
  # Run pre-commit pydoclint on all files
  pip install -c python/requirements_compiled.txt pre-commit clang-format
  pre-commit run pydoclint --all-files --show-diff-on-failure
}

code_format() {
  pip install -c python/requirements_compiled.txt -r python/requirements/lint-requirements.txt
  FORMAT_SH_PRINT_DIFF=1 ./ci/lint/format.sh --all-scripts
}

semgrep_lint() {
  pip install -c python/requirements_compiled.txt semgrep pre-commit
  pre-commit run semgrep --all-files --show-diff-on-failure
}

banned_words() {
  ./ci/lint/check-banned-words.sh
}

# Use system python to avoid conflicts with uv python in forge image
doc_readme() {
  /usr/bin/python -m pip install -c python/requirements_compiled.txt docutils
  cd python && /usr/bin/python setup.py check --restructuredtext --strict --metadata
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

_install_ray_no_deps() {
  if [[ -d /opt/ray-build ]]; then
    unzip -o -q /opt/ray-build/ray_pkg.zip -d python
    unzip -o -q /opt/ray-build/ray_py_proto.zip -d python
    mkdir -p python/ray/dashboard/client/build
    tar -xzf /opt/ray-build/dashboard.tar.gz -C python/ray/dashboard/client/build
    SKIP_BAZEL_BUILD=1 pip install -e "python[all]" --no-deps
  else
    RAY_DISABLE_EXTRA_CPP=1 pip install -e "python[all]" --no-deps
  fi
}

api_annotations() {
  echo "--- Install Ray"
  _install_ray_no_deps

  echo "--- Check API annotations"
  ./ci/lint/check_api_annotations.py
}

api_policy_check() {
  echo "--- Install Ray"
  _install_ray_no_deps

  echo "--- Generate API doc stubs"
  # The consistency check reads autosummary stub .rst files. Generate only those
  # stubs instead of a full `make -C doc/ html` (which built the entire site just
  # to produce them). This exits nonzero if generation produces nothing, so a
  # broken autogen step fails here instead of silently. Stubs are generated after
  # installing Ray so they reflect the checkout's source.
  PYTHONPATH="$(pwd)${PYTHONPATH:+:$PYTHONPATH}" python doc/source/api_autogen.py

  echo "--- Check API/doc consistency"
  # Run via the image interpreter, not `bazel run`: the bazel target's @py_deps_py310
  # (cp310) wheels can't import under the py3.11 docbuild image (e.g. rpds).
  # TODO(elliot-barn): #64070 switch back to bazel once hermetic python 3.11 is setup
  PYTHONPATH="$(pwd)${PYTHONPATH:+:$PYTHONPATH}" python ci/ray_ci/doc/cmd_check_api_discrepancy.py /ray "$@"
}

documentation_style() {
  ./ci/lint/check-documentation-style.sh
}

doc_no_new_rst() {
  python doc/test_no_new_rst.py
}

"$@"
