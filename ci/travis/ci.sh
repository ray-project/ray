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
  bazel test --config=ci --build_tests_only -- //:all -rllib/...
}

test_python() {
  if [ "${OSTYPE}" = msys ]; then
    local args=(python/ray/tests/...)
    args+=(
      -python/ray/tests:test_actor_advanced
      -python/ray/tests:test_actor_failures
      -python/ray/tests:test_advanced_2
      -python/ray/tests:test_advanced_3
      -python/ray/tests:test_array
      -python/ray/tests:test_asyncio
      -python/ray/tests:test_autoscaler_aws
      -python/ray/tests:test_autoscaler_yaml
      -python/ray/tests:test_cancel
      -python/ray/tests:test_component_failures
      -python/ray/tests:test_cython
      -python/ray/tests:test_dynres
      -python/ray/tests:test_failure
      -python/ray/tests:test_global_gc
      -python/ray/tests:test_global_state
      -python/ray/tests:test_iter
      -python/ray/tests:test_memory_scheduling
      -python/ray/tests:test_memstat
      -python/ray/tests:test_metrics
      -python/ray/tests:test_multi_node
      -python/ray/tests:test_multi_node_2
      -python/ray/tests:test_multinode_failures_2
      -python/ray/tests:test_multiprocessing
      -python/ray/tests:test_node_manager
      -python/ray/tests:test_object_manager
      -python/ray/tests:test_projects
      -python/ray/tests:test_queue
      -python/ray/tests:test_ray_init
      -python/ray/tests:test_reconstruction
      -python/ray/tests:test_stress
      -python/ray/tests:test_stress_sharded
      -python/ray/tests:test_webui
    )
    bazel test -k --config=ci --test_timeout=600 --build_tests_only -- "${args[@]}";
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
      cd ray/dashboard/client
      set +x  # suppress set -x since it'll get very noisy here
      . "${HOME}/.nvm/nvm.sh"
      nvm use --silent node
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
      sphinx-build -q -E -T -b html source _build/html
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

install_ray() {
  (
    cd "${WORKSPACE_DIR}"/python
    build_dashboard_front_end
    pip install -v -e .
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
        -e TRAVIS=true
        -e TRAVIS_PULL_REQUEST="${TRAVIS_PULL_REQUEST:-false}"
        -e encrypted_1c30b31fe1ee_key="${encrypted_1c30b31fe1ee_key-}"
        -e encrypted_1c30b31fe1ee_iv="${encrypted_1c30b31fe1ee_iv-}"
      )

      # This command should be kept in sync with ray/python/README-building-wheels.md,
      # except the "${MOUNT_BAZEL_CACHE[@]}" part.
      suppress_output docker run --rm -w /ray -v "${PWD}":/ray "${MOUNT_BAZEL_CACHE[@]}" \
        -e TRAVIS_COMMIT="${TRAVIS_COMMIT}" \
        rayproject/arrow_linux_x86_64_base:python-3.8.0 /ray/python/build-wheel-manylinux1.sh
      ;;
    darwin*)
      # This command should be kept in sync with ray/python/README-building-wheels.md.
      suppress_output "${WORKSPACE_DIR}"/python/build-wheel-macos.sh
      ;;
    msys*)
      (
        local backup_conda="${CONDA_PREFIX}.bak" ray_uninstall_status=0
        test ! -d "${backup_conda}"
        pip uninstall -y ray || ray_uninstall_status=1
        mv -n -T -- "${CONDA_PREFIX}" "${backup_conda}"  # Back up conda

        local pyversion pyversions=()
        for pyversion in 3.6 3.7 3.8; do
          if [ "${pyversion}" = "${PYTHON-}" ]; then continue; fi  # we'll build ${PYTHON} last
          pyversions+=("${pyversion}")
        done

        pyversions+=("${PYTHON-}")  # build this last so any subsequent steps use the right version
        local local_dir="python/dist"
        for pyversion in "${pyversions[@]}"; do
          if [ -z "${pyversion}" ]; then continue; fi
          "${ROOT_DIR}"/bazel-preclean.sh
          git clean -q -f -f -x -d -e "${local_dir}" -e python/ray/dashboard/client
          git checkout -q -f -- .
          cp -R -f -a -T -- "${backup_conda}" "${CONDA_PREFIX}"
          local existing_version
          existing_version="$(python -s -c "import sys; print('%s.%s' % sys.version_info[:2])")"
          if [ "${pyversion}" != "${existing_version}" ]; then
            suppress_output conda install python="${pyversion}"
          fi
          install_ray
          (cd "${WORKSPACE_DIR}"/python && python setup.py --quiet bdist_wheel)
          pip uninstall -y ray
          rm -r -f -- "${CONDA_PREFIX}"
        done

        mv -n -T -- "${backup_conda}" "${CONDA_PREFIX}"
        "${ROOT_DIR}"/bazel-preclean.sh
        if [ 0 -eq "${ray_uninstall_status}" ]; then  # If Ray was previously installed, restore it
          install_ray
        fi
      )
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

lint_python() {
  # ignore dict vs {} (C408), others are defaults
  command -V python
  python -m flake8 --inline-quotes '"' --no-avoid-escape \
    --exclude=python/ray/core/generated/,streaming/python/generated,doc/source/conf.py,python/ray/cloudpickle/,python/ray/thirdparty_files \
    --ignore=C408,E121,E123,E126,E226,E24,E704,W503,W504,W605
  "${ROOT_DIR}"/format.sh --all
}

lint_bazel() {
  # Run buildifier without affecting external environment variables
  (
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
    nvm use --silent node
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

  if command -v clang-format > /dev/null; then
    "${ROOT_DIR}"/check-git-clang-format-output.sh
  else
    { echo "WARNING: Skipping linting C/C++ as clang-format is not installed."; } 2> /dev/null
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
  install_go
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

_check_job_triggers() {
  local job_names
  job_names="$1"

  local variable_definitions
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

  . "${ROOT_DIR}"/install-dependencies.sh  # Script is sourced to propagate up environment changes
}

build() {
  if ! need_wheels; then
    # NOTE: Do not add build flags here. Use .bazelrc and --config instead.
    bazel build -k "//:*"  # Full build first, since pip install will build only a subset of targets
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
