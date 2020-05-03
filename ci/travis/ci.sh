#!/usr/bin/env bash

{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null  # Push caller's shell options (quietly)

unset -f cd  # Travis defines this on Mac for RVM, but it floods the trace log and isn't relevant for us

set -eo pipefail && if [ -z "${TRAVIS_PULL_REQUEST-}" ] || [ -n "${OSTYPE##darwin*}" ]; then set -ux; fi  # some options interfere with Travis's RVM on Mac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"
WORKSPACE_DIR="${ROOT_DIR}/../.."

suppress_output() {
  "${WORKSPACE_DIR}"/ci/suppress_output "$@"
}

keep_alive() {
  "${WORKSPACE_DIR}"/ci/keep_alive "$@"
}

# If provided the names of one or more environment variables, returns success if any of them is triggered.
# Usage: should_run_job [VAR_NAME]...
should_run_job() {
  local skip=0
  if [ -n "${1-}" ]; then  # were any triggers provided? (if not, then the job will always run)
    local envvar active_triggers=()
    for envvar in "$@"; do
      if [ "${!envvar}" = 1 ]; then
        active_triggers+=("${envvar}=${!envvar}")  # success! we found at least one of the given triggers is occurring
      fi
    done
    if [ 0 -eq "${#active_triggers[@]}" ]; then
      echo "Job is not triggered by any of $1; skipping job."
      sleep 15  # make sure output is flushed
      skip=1
    else
      echo "Job is triggered by: ${active_triggers[*]}"
    fi
  fi
  return "${skip}"
}

# Idempotent environment loading
reload_env() {
  # TODO: We should really just use a new login shell instead of doing this manually.
  # Otherwise we might source a script that isn't idempotent (e.g. one that blindly prepends to PATH).

  if [ -n "${GITHUB_PULL_REQUEST-}" ]; then
    case "${GITHUB_PULL_REQUEST}" in
      [1-9]*) TRAVIS_PULL_REQUEST="${GITHUB_PULL_REQUEST}";;
      *) TRAVIS_PULL_REQUEST=false;;
    esac
    export TRAVIS_PULL_REQUEST
  fi

  export PYTHON3_BIN_PATH=python
  export GOROOT="${HOME}/go" GOPATH="${HOME}/go_dir"
  if [ "${OSTYPE}" = msys ]; then
    export USE_CLANG_CL=1
  fi

  # NOTE: Modifying PATH invalidates Bazel's cache! Do not add to PATH unnecessarily.
  PATH="${HOME}/miniconda/bin":"${PATH}"
  if [ "${OSTYPE}" = msys ]; then
    PATH="${HOME}/miniconda/bin/Scripts":"${PATH}"
  fi

  # Deduplicate PATH
  PATH="$(set +x && printf "%s\n" "${PATH}" | tr ":" "\n" | awk '!a[$0]++' | tr "\n" ":")"
  PATH="${PATH%:}"  # Remove trailing colon
}

need_wheels() {
  local error_code=1
  case "${OSTYPE}" in
    linux*) if [ "${LINUX_WHEELS-}" = 1 ]; then error_code=0; fi;;
    darwin*) if [ "${MAC_WHEELS-}" = 1 ]; then error_code=0; fi;;
    msys*) if [ "${WINDOWS_WHEELS-}" = 1 ]; then error_code=0; fi;;
  esac
  return "${error_code}"
}

upload_wheels() {
  local branch="" commit
  commit="$(git rev-parse --verify HEAD)"
  if [ -z "${branch}" ]; then branch="${TRAVIS_BRANCH-}"; fi
  if [ -z "${branch}" ]; then branch="${GITHUB_BASE_REF-}"; fi
  if [ -z "${branch}" ]; then branch="${GITHUB_REF#refs/heads/}"; fi
  if [ -z "${branch}" ]; then echo "Unable to detect branch name" 1>&2; return 1; fi
  local local_dir="python/dist"
  local remote_dir="${branch}/${commit}"
  if [ -d "${local_dir}" ]; then
    if command -V aws; then
      aws s3 sync --acl public-read --no-progress "${local_dir}" "s3://ray-wheels/${remote_dir}"
    fi
  fi
}


test_python() {
  if [ "${OSTYPE}" = msys ]; then
    # Windows -- most tests won't work yet; just do the ones we know work
    PYTHONPATH=python python -m pytest --durations=5 --timeout=300 python/ray/tests/test_mini.py
  fi
}

test_cpp() {
  bazel test --config=ci //cpp:all --build_tests_only --test_output=streamed
}

test_wheels() {
  local result=0 flush_logs=0

  if need_wheels; then
    "${WORKSPACE_DIR}"/ci/travis/test-wheels.sh || { result=$? && flush_logs=1; }
  fi

  if [ 0 -ne "${flush_logs}" ]; then
    cat -- /tmp/ray/session_latest/logs/* || true
    sleep 60  # Explicitly sleep 60 seconds for logs to go through
  fi

  return "${result}"
}

build_sphinx_docs() {
  (
    cd "${WORKSPACE_DIR}"/doc
    if [ "${OSTYPE}" = msys ]; then
      echo "WARNING: Documentation not built on Windows due to currently-unresolved issues"
    else
      sphinx-build -q -W -E -T -b html source _build/html
    fi
  )
}

install_cython_examples() {
  "${ROOT_DIR}"/install-cython-examples.sh
}

install_go() {
  eval "$(curl -sL https://raw.githubusercontent.com/travis-ci/gimme/master/gimme | GIMME_GO_VERSION=stable bash)"
}

install_ray() {
  bazel build -k "//:*"   # Do a full build first to ensure everything passes
  "${ROOT_DIR}"/install-ray.sh
}

build_wheels() {
  case "${OSTYPE}" in
    linux*)
      # Mount bazel cache dir to the docker container.
      # For the linux wheel build, we use a shared cache between all
      # wheels, but not between different travis runs, because that
      # caused timeouts in the past. See the "cache: false" line below.
      local MOUNT_BAZEL_CACHE=(-v "${HOME}/ray-bazel-cache":/root/ray-bazel-cache -e TRAVIS=true -e TRAVIS_PULL_REQUEST="${TRAVIS_PULL_REQUEST:-false}" -e encrypted_1c30b31fe1ee_key="${encrypted_1c30b31fe1ee_key-}" -e encrypted_1c30b31fe1ee_iv="${encrypted_1c30b31fe1ee_iv-}")

      # This command should be kept in sync with ray/python/README-building-wheels.md,
      # except the "${MOUNT_BAZEL_CACHE[@]}" part.
      suppress_output docker run --rm -w /ray -v "${PWD}":/ray "${MOUNT_BAZEL_CACHE[@]}" -e TRAVIS_COMMIT="${TRAVIS_COMMIT}" rayproject/arrow_linux_x86_64_base:python-3.8.0 /ray/python/build-wheel-manylinux1.sh
      ;;
    darwin*)
      # This command should be kept in sync with ray/python/README-building-wheels.md.
      suppress_output "${WORKSPACE_DIR}"/python/build-wheel-macos.sh
      ;;
    msys*)
      if ! python -c "import ray" 2> /dev/null; then
        # This installs e.g. the pickle5 dependencies, which are needed for correct wheel testing
        install_ray
      fi
      (
        cd "${WORKSPACE_DIR}"/python
        python setup.py --quiet bdist_wheel
      )
      ;;
  esac
}

lint_readme() {
  (
    cd "${WORKSPACE_DIR}"/python
    python setup.py check --restructuredtext --strict --metadata
  )
}

lint_python() {
  # ignore dict vs {} (C408), others are defaults
  command -V python
  python -m flake8 --inline-quotes '"' --no-avoid-escape --exclude=python/ray/core/generated/,streaming/python/generated,doc/source/conf.py,python/ray/cloudpickle/,python/ray/thirdparty_files --ignore=C408,E121,E123,E126,E226,E24,E704,W503,W504,W605
  "${ROOT_DIR}"/format.sh --all
}

lint_bazel() {
  # Run buildifier without affecting external environment variables
  (
    # TODO: Move installing Go & building buildifier to the dependency installation step?
    if [ ! -d "${GOROOT}" ]; then
      curl -s -L "https://dl.google.com/go/go1.14.2.linux-amd64.tar.gz" | tar -C "${GOROOT%/*}" -xz
    fi
    mkdir -p -- "${GOPATH}"
    export PATH="${GOPATH}/bin":"${GOROOT}/bin":"${PATH}"

    # Build buildifier
    go get github.com/bazelbuild/buildtools/buildifier

    # Now run buildifier
    "${ROOT_DIR}"/bazel-format.sh
  )
}

lint_web() {
  (
    cd "${WORKSPACE_DIR}"/python/ray/dashboard/client
    set +x # suppress set -x since it'll get very noisy here
    . "${HOME}/.nvm/nvm.sh"
    install_npm_project
    nvm use node
    node_modules/.bin/eslint --max-warnings 0 $(find src -name "*.ts" -or -name "*.tsx")
    node_modules/.bin/prettier --check $(find src -name "*.ts" -or -name "*.tsx")
    node_modules/.bin/prettier --check public/index.html
  )
}

_lint() {
  local platform=""
  case "${OSTYPE}" in
    linux*) platform=linux;;
  esac

  if [ -n "${TRAVIS_BRANCH-}" ]; then
    "${ROOT_DIR}"/check-git-clang-format-output.sh
  else
    # TODO(mehrdadn): Implement this on GitHub Actions
    echo "WARNING: Not running clang-format due to TRAVIS_BRANCH not being defined."
  fi

  # Run Python linting
  lint_python

  # Make sure that the README is formatted properly.
  lint_readme

  if [ "${platform}" = linux ]; then
    # Run Bazel linter Buildifier.
    lint_bazel

    # Run TypeScript and HTML linting.
    lint_web
  fi
}

lint() {
  # Checkout a clean copy of the repo to avoid seeing changes that have been made to the current one
  (
    WORKSPACE_DIR="$(TMPDIR="${WORKSPACE_DIR}/.." mktemp -d)"
    ROOT_DIR="${WORKSPACE_DIR}"/ci/travis
    git worktree add -q "${WORKSPACE_DIR}"
    pushd "${WORKSPACE_DIR}"
      . "${ROOT_DIR}"/ci.sh _lint
    popd  # this is required so we can remove the worktree when we're done
    git worktree remove --force "${WORKSPACE_DIR}"
  )
}

_preload() {
  local job_names
  job_names="$1"

  local variable_definitions
  variable_definitions=($(python "${ROOT_DIR}"/determine_tests_to_run.py))
  if [ 0 -lt "${#variable_definitions[@]}" ]; then
    local expression
    expression="$(printf "%q " "${variable_definitions[@]}")"
    eval "${expression}"
    printf "%s\n" "${expression}" >> ~/.bashrc
  fi

  if ! (set +x && should_run_job ${job_names//,/ }); then
    if [ -n "${GITHUB_WORKFLOW-}" ]; then
      # If this job is to be skipped, emit an 'exit' command into .bashrc to quickly exit all following steps.
      # This isn't needed for Travis (since everything runs in a single shell), but it is needed for GitHub Actions.
      cat <<EOF1 >> ~/.bashrc
      cat <<EOF2 1>&2
Exiting shell as no triggers were active for this job:
  ${job_names//,/}
The active triggers during job initialization were the following:
  ${variable_definitions[*]}
EOF2
      exit 0
EOF1
    fi
    exit 0
  fi
}

configure_system() {
  git config --global advice.detachedHead false
  git config --global core.askpass ""
  git config --global credential.helper ""
  git config --global credential.modalprompt false
}

# Initializes the environment for the current job. Performs the following tasks:
# - Calls 'exit 0' in this job step and all subsequent steps to quickly exit if provided a list of job names and
#   none of them has been triggered.
# - Sets variables to indicate the job names that have been triggered.
#   Note: Please avoid exporting these variables. Instead, source any callees that need to use them.
#   This helps reduce implicit coupling of callees to their parents, as they will be unable to run when not sourced, (especially with set -u).
# - Installs dependencies for the current job.
# - Exports any environment variables necessary to run the build.
# Usage: init [JOB_NAMES]
# - JOB_NAMES (optional): Comma-separated list of job names to trigger on.
init() {
  _preload "${1-}"

  configure_system

  "${ROOT_DIR}"/install-bazel.sh
  . "${ROOT_DIR}"/install-dependencies.sh

  cat <<EOF >> ~/.bashrc
. "${BASH_SOURCE:-$0}"  # reload environment
EOF
}

build() {
  if ! need_wheels; then
    install_ray
    if [ "${LINT-}" = 1 ]; then
      # Try generating Sphinx documentation. To do this, we need to install Ray first.
      build_sphinx_docs
    fi
  fi

  if [ "${RAY_CYTHON_EXAMPLES-}" = 1 ]; then
    install_cython_examples
  fi

  if [ "${RAY_DEFAULT_BUILD-}" = 1 ]; then
    install_go
  fi

  if need_wheels; then
    build_wheels
  fi
}

_main() {
  if [ -n "${GITHUB_WORKFLOW-}" ]; then
    # Necessary for GitHub Actions (which uses separate shells for different commands)
    # Unnecessary for Travis (which uses one shell for different commands)
    reload_env
  fi
  "$@"
}

_main "$@"

{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null  # Pop caller's shell options (quietly)
