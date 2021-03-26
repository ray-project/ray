#!/usr/bin/env bash

# Push caller's shell options (quietly)
{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null

set -eo pipefail
if [ -z "${TRAVIS_PULL_REQUEST-}" ] || [ -n "${OSTYPE##darwin*}" ]; then set -ux; fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"
WORKSPACE_DIR="${ROOT_DIR}/../.."

suppress_output() {
  "${WORKSPACE_DIR}"/ci/suppress_output "$@"
}

keep_alive() {
  "${WORKSPACE_DIR}"/ci/keep_alive "$@"
}

# Calls the provided command with set -x temporarily suppressed
suppress_xtrace() {
  {
    local restore_shell_state=""
    if [ -o xtrace ]; then set +x; restore_shell_state="set -x"; fi
  } 2> /dev/null
  local status=0
  "$@" || status=$?
  ${restore_shell_state}
  { return "${status}"; } 2> /dev/null
}

# If provided the names of one or more environment variables, returns 0 if any of them is triggered.
# Usage: should_run_job [VAR_NAME]...
should_run_job() {
  local skip=0
  if [ -n "${1-}" ]; then  # were any triggers provided? (if not, then the job will always run)
    local envvar active_triggers=()
    for envvar in "$@"; do
      if [ "${!envvar}" = 1 ]; then
        # success! we found at least one of the given triggers is occurring
        active_triggers+=("${envvar}=${!envvar}")
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
  # Try to only modify CI-specific environment variables here (TRAVIS_... or GITHUB_...),
  # e.g. for CI cross-compatibility.
  # Normal environment variables should be set up at software installation time, not here.

  if [ -n "${GITHUB_PULL_REQUEST-}" ]; then
    case "${GITHUB_PULL_REQUEST}" in
      [1-9]*) TRAVIS_PULL_REQUEST="${GITHUB_PULL_REQUEST}";;
      *) TRAVIS_PULL_REQUEST=false;;
    esac
    export TRAVIS_PULL_REQUEST
  fi

  if [ "${GITHUB_ACTIONS-}" = true ] && [ -z "${TRAVIS_BRANCH-}" ]; then
    # Define TRAVIS_BRANCH to make Travis scripts run on GitHub Actions.
    TRAVIS_BRANCH="${GITHUB_BASE_REF:-${GITHUB_REF}}"  # For pull requests, the base branch name
    TRAVIS_BRANCH="${TRAVIS_BRANCH#refs/heads/}"  # Remove refs/... prefix
    # TODO(mehrdadn): Make TRAVIS_BRANCH be a named ref (e.g. 'master') like it's supposed to be.
    # For now we use a hash because GitHub Actions doesn't clone refs the same way as Travis does.
    TRAVIS_BRANCH="${GITHUB_HEAD_SHA:-${TRAVIS_BRANCH}}"
    export TRAVIS_BRANCH
  fi
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
  if [ -z "${branch}" ]; then branch="${GITHUB_BASE_REF-}"; fi
  if [ -z "${branch}" ]; then branch="${GITHUB_REF#refs/heads/}"; fi
  if [ -z "${branch}" ]; then branch="${TRAVIS_BRANCH-}"; fi
  if [ -z "${branch}" ]; then echo "Unable to detect branch name" 1>&2; return 1; fi
  local local_dir="python/dist"
  if [ -d "${local_dir}" ]; then
    ls -a -l -- "${local_dir}"
    local remote_dir
    for remote_dir in latest "${branch}/${commit}"; do
      if command -V aws; then
        aws s3 sync --acl public-read --no-progress "${local_dir}" "s3://ray-wheels/${remote_dir}"
      fi
    done
  fi
  (
    cd "${WORKSPACE_DIR}"/python
    if ! python -s -c "import ray, sys; sys.exit(0 if ray._raylet.OPTIMIZED else 1)"; then
      echo "ERROR: Uploading non-optimized wheels! Performance will suffer for users!"
      false
    fi
  )
}

test_core() {
  local args=(
    "//:*"
  )
  case "${OSTYPE}" in
    msys)
      args+=(
        -//:core_worker_test
        -//:event_test
        -//:gcs_pub_sub_test
        -//:gcs_server_test
        -//:gcs_server_rpc_test
      )
      ;;
  esac
  # shellcheck disable=SC2046
  bazel test --config=ci --build_tests_only $(./scripts/bazel_export_options) -- "${args[@]}"
}

test_python() {
  local pathsep=":" args=()
  if [ "${OSTYPE}" = msys ]; then
    pathsep=";"
    args+=(
      python/ray/serve/...
      python/ray/tests/...
      -python/ray/serve:test_api # segfault on windows? https://github.com/ray-project/ray/issues/12541
      -python/ray/serve:test_handle # "fatal error" (?) https://github.com/ray-project/ray/pull/13695
      -python/ray/serve:test_backend_worker # memory error
      -python/ray/tests:test_actor_advanced # timeout
      -python/ray/tests:test_advanced_2
      -python/ray/tests:test_advanced_3  # test_invalid_unicode_in_worker_log() fails on Windows
      -python/ray/tests:test_autoscaler_aws
      -python/ray/tests:test_component_failures
      -python/ray/tests:test_component_failures_3 # timeout
      -python/ray/tests:test_basic_2  # hangs on shared cluster tests
      -python/ray/tests:test_basic_2_client_mode
      -python/ray/tests:test_basic_3  # timeout
      -python/ray/tests:test_basic_3_client_mode
      -python/ray/tests:test_cli
      -python/ray/tests:test_client_init # timeout
      -python/ray/tests:test_failure
      -python/ray/tests:test_global_gc
      -python/ray/tests:test_job
      -python/ray/tests:test_memstat
      -python/ray/tests:test_metrics
      -python/ray/tests:test_metrics_agent # timeout
      -python/ray/tests:test_multi_node
      -python/ray/tests:test_multi_node_2
      -python/ray/tests:test_multi_node_3
      -python/ray/tests:test_multiprocessing  # test_connect_to_ray() fails to connect to raylet
      -python/ray/tests:test_node_manager
      -python/ray/tests:test_object_manager
      -python/ray/tests:test_placement_group # timeout and OOM
      -python/ray/tests:test_ray_init  # test_redis_port() seems to fail here, but pass in isolation
      -python/ray/tests:test_resource_demand_scheduler
      -python/ray/tests:test_stress  # timeout
      -python/ray/tests:test_stress_sharded  # timeout
      -python/ray/tests:test_k8s_cluster_launcher
      -python/ray/tests:test_k8s_operator_examples
      -python/ray/tests:test_k8s_operator_mock
    )
  fi
  if [ 0 -lt "${#args[@]}" ]; then  # Any targets to test?
    install_ray
    # TODO(mehrdadn): We set PYTHONPATH here to let Python find our pickle5 under pip install -e.
    # It's unclear to me if this should be necessary, but this is to make tests run for now.
    # Check why this issue doesn't arise on Linux/Mac.
    # Ideally importing ray.cloudpickle should import pickle5 automatically.
    # shellcheck disable=SC2046
    bazel test --config=ci --build_tests_only $(./scripts/bazel_export_options) \
      --test_env=PYTHONPATH="${PYTHONPATH-}${pathsep}${WORKSPACE_DIR}/python/ray/pickle5_files" -- \
      "${args[@]}";
  fi
}

test_cpp() {
  bazel build --config=ci //cpp:all
  # shellcheck disable=SC2046
  bazel test --config=ci $(./scripts/bazel_export_options) //cpp:all --build_tests_only
  # run the cpp example
  bazel run //cpp/example:example

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

install_npm_project() {
  if [ "${OSTYPE}" = msys ]; then
    # Not Windows-compatible: https://github.com/npm/cli/issues/558#issuecomment-584673763
    { echo "WARNING: Skipping NPM due to module incompatibilities with Windows"; } 2> /dev/null
  else
    npm ci -q
  fi
}

build_dashboard_front_end() {
  if [ "${OSTYPE}" = msys ]; then
    { echo "WARNING: Skipping dashboard due to NPM incompatibilities with Windows"; } 2> /dev/null
  else
    (
      cd ray/new_dashboard/client

      if [ -z "${BUILDKITE-}" ]; then
        set +x  # suppress set -x since it'll get very noisy here
        . "${HOME}/.nvm/nvm.sh"
        nvm use --silent node
      fi
      install_npm_project
      npm run -s build
    )
  fi
}

build_sphinx_docs() {
  (
    cd "${WORKSPACE_DIR}"/doc
    if [ "${OSTYPE}" = msys ]; then
      echo "WARNING: Documentation not built on Windows due to currently-unresolved issues"
    else
      sphinx-build -q -E -W -T -b html source _build/html
    fi
  )
}

install_cython_examples() {
  (
    cd "${WORKSPACE_DIR}"/doc/examples/cython
    pip install scipy
    python setup.py install --user
  )
}

install_go() {
  local gimme_url="https://raw.githubusercontent.com/travis-ci/gimme/master/gimme"
  suppress_xtrace eval "$(curl -f -s -L "${gimme_url}" | GIMME_GO_VERSION=1.14.2 bash)"

  if [ -z "${GOPATH-}" ]; then
    GOPATH="${GOPATH:-${HOME}/go_dir}"
    export GOPATH
  fi
}

_bazel_build_before_install() {
  local target
  if [ "${OSTYPE}" = msys ]; then
    # On Windows, we perform as full of a build as possible, to ensure the repository always remains buildable on Windows.
    # (Pip install will not perform a full build.)
    target="//:*"
  else
    # Just build Python on other platforms.
    # This because pip install captures & suppresses the build output, which causes a timeout on CI.
    target="//:ray_pkg"
  fi
  # NOTE: Do not add build flags here. Use .bazelrc and --config instead.
  bazel build "${target}"
}


_bazel_build_protobuf() {
  bazel build "//:install_py_proto"
}

install_ray() {
  # TODO(mehrdadn): This function should be unified with the one in python/build-wheel-windows.sh.
  (
    cd "${WORKSPACE_DIR}"/python
    build_dashboard_front_end
    keep_alive pip install -v -e .
  )
}

build_wheels() {
  case "${OSTYPE}" in
    linux*)
      # Mount bazel cache dir to the docker container.
      # For the linux wheel build, we use a shared cache between all
      # wheels, but not between different travis runs, because that
      # caused timeouts in the past. See the "cache: false" line below.
      local MOUNT_BAZEL_CACHE=(
        -v "${HOME}/ray-bazel-cache":/root/ray-bazel-cache
        -e "TRAVIS=true"
        -e "TRAVIS_PULL_REQUEST=${TRAVIS_PULL_REQUEST:-false}"
        -e "encrypted_1c30b31fe1ee_key=${encrypted_1c30b31fe1ee_key-}"
        -e "encrypted_1c30b31fe1ee_iv=${encrypted_1c30b31fe1ee_iv-}"
        -e "TRAVIS_COMMIT=${TRAVIS_COMMIT}"
        -e "CI=${CI}"
        -e "RAY_INSTALL_JAVA=${RAY_INSTALL_JAVA:-}"
        -e "BUILDKITE=${BUILDKITE:-}"
        -e "BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL:-}"
      )


      if [ -z "${BUILDKITE-}" ]; then
        # This command should be kept in sync with ray/python/README-building-wheels.md,
        # except the "${MOUNT_BAZEL_CACHE[@]}" part.
        docker run --rm -w /ray -v "${PWD}":/ray "${MOUNT_BAZEL_CACHE[@]}" \
        quay.io/pypa/manylinux2014_x86_64 /ray/python/build-wheel-manylinux2014.sh
      else
        cp -rT /ray /ray-mount
        ls /ray-mount
        docker run --rm -v /ray:/ray-mounted ubuntu:focal ls /
        docker run --rm -v /ray:/ray-mounted ubuntu:focal ls /ray-mounted
        docker run --rm -w /ray -v /ray:/ray "${MOUNT_BAZEL_CACHE[@]}" \
          quay.io/pypa/manylinux2014_x86_64 /ray/python/build-wheel-manylinux2014.sh
        cp -rT /ray-mount /ray # copy new files back here
        find . | grep whl # testing
      fi
      ;;
    darwin*)
      # This command should be kept in sync with ray/python/README-building-wheels.md.
      # Remove suppress_output for now to avoid timeout
      "${WORKSPACE_DIR}"/python/build-wheel-macos.sh
      ;;
    msys*)
      keep_alive "${WORKSPACE_DIR}"/python/build-wheel-windows.sh
      ;;
  esac
}

lint_readme() {
  if python -s -c "import docutils" >/dev/null 2>/dev/null; then
    (
      cd "${WORKSPACE_DIR}"/python
      python setup.py check --restructuredtext --strict --metadata
    )
  else
    echo "Skipping README lint because the docutils package is not installed" 1>&2
  fi
}

lint_scripts() {
  FORMAT_SH_PRINT_DIFF=1 "${ROOT_DIR}"/format.sh --all
}

lint_bazel() {
  # Run buildifier without affecting external environment variables
  (
    mkdir -p -- "${GOPATH}"
    export PATH="${GOPATH}/bin:${GOROOT}/bin:${PATH}"

    # Build buildifier
    go get github.com/bazelbuild/buildtools/buildifier

    # Now run buildifier
    "${ROOT_DIR}"/bazel-format.sh
  )
}

lint_web() {
  (
    cd "${WORKSPACE_DIR}"/python/ray/new_dashboard/client
    set +x # suppress set -x since it'll get very noisy here

    if [ -z "${BUILDKITE-}" ]; then
      . "${HOME}/.nvm/nvm.sh"
      nvm use --silent node
    fi

    install_npm_project
    local filenames
    # shellcheck disable=SC2207
    filenames=($(find src -name "*.ts" -or -name "*.tsx"))
    node_modules/.bin/eslint --max-warnings 0 "${filenames[@]}"
    node_modules/.bin/prettier --check "${filenames[@]}"
    node_modules/.bin/prettier --check public/index.html
  )
}

_lint() {
  local platform=""
  case "${OSTYPE}" in
    linux*) platform=linux;;
  esac

  if command -v clang-format > /dev/null; then
    "${ROOT_DIR}"/check-git-clang-format-output.sh
  else
    { echo "WARNING: Skipping linting C/C++ as clang-format is not installed."; } 2> /dev/null
  fi

  # Run script linting
  lint_scripts

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
  install_go
  # Checkout a clean copy of the repo to avoid seeing changes that have been made to the current one
  (
    WORKSPACE_DIR="$(TMPDIR="${WORKSPACE_DIR}/.." mktemp -d)"
    # shellcheck disable=SC2030
    ROOT_DIR="${WORKSPACE_DIR}"/ci/travis
    git worktree add -q "${WORKSPACE_DIR}"
    pushd "${WORKSPACE_DIR}"
      . "${ROOT_DIR}"/ci.sh _lint
    popd  # this is required so we can remove the worktree when we're done
    git worktree remove --force "${WORKSPACE_DIR}"
  )
}

_check_job_triggers() {
  local job_names
  job_names="$1"

  local variable_definitions
  # shellcheck disable=SC2031
  variable_definitions=($(python "${ROOT_DIR}"/determine_tests_to_run.py))
  if [ 0 -lt "${#variable_definitions[@]}" ]; then
    local expression restore_shell_state=""
    if [ -o xtrace ]; then set +x; restore_shell_state="set -x;"; fi  # Disable set -x (noisy here)
    {
      expression="$(printf "%q " "${variable_definitions[@]}")"
      printf "%s\n" "${expression}" >> ~/.bashrc
    }
    eval "${restore_shell_state}" "${expression}"  # Restore set -x, then evaluate expression
  fi

  # shellcheck disable=SC2086
  if ! (set +x && should_run_job ${job_names//,/ }); then
    if [ "${GITHUB_ACTIONS-}" = true ]; then
      # If this job is to be skipped, emit 'exit' into .bashrc to quickly exit all following steps.
      # This isn't needed on Travis (since everything runs in one shell), but is on GitHub Actions.
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
# - Calls 'exit 0' in this job step and all subsequent steps to quickly exit if provided a list of
#   job names and none of them has been triggered.
# - Sets variables to indicate the job names that have been triggered.
#   Note: Please avoid exporting these variables. Instead, source any callees that need to use them.
#   This helps reduce implicit coupling of callees to their parents, as they will be unable to run
#   when not sourced, (especially with set -u).
# - Installs dependencies for the current job.
# - Exports any environment variables necessary to run the build.
# Usage: init [JOB_NAMES]
# - JOB_NAMES (optional): Comma-separated list of job names to trigger on.
init() {
  _check_job_triggers "${1-}"

  configure_system

  # shellcheck disable=SC2031
  . "${ROOT_DIR}"/install-dependencies.sh  # Script is sourced to propagate up environment changes
}

build() {
  if [ "${LINT-}" != 1 ]; then
    _bazel_build_before_install
  else
    _bazel_build_protobuf
  fi

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

  if [ "${RAY_DEFAULT_BUILD-}" = 1 ] || [ "${LINT-}" = 1 ]; then
    install_go
  fi

  if need_wheels; then
    build_wheels
  fi
}

_main() {
  if [ "${GITHUB_ACTIONS-}" = true ]; then
    exec 2>&1  # Merge stdout and stderr to prevent out-of-order buffering issues
    reload_env
  fi
  "$@"
}

_main "$@"

# Pop caller's shell options (quietly)
{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null
