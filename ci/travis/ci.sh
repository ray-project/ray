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
      skip=1
    else
      echo "Job is triggered by: ${#active_triggers[*]}"
    fi
  fi
  return "${skip}"
}

reload_env() {
  # TODO: We should really just use a new login shell instead of doing this manually.
  # Otherwise we might source a script that isn't idempotent (e.g. one that blindly prepends to PATH).
  { local set_x="${-//[^x]/}"; } 2> /dev/null  # save set -x to suppress noise
  set +x
  local to_add="$HOME/miniconda/bin" old_path=":${PATH}:"
  if [ "${old_path}" = "${old_path##*:${to_add}:*}" ]; then
    PATH="${to_add}:$PATH"
  fi
  test -z "${set_x}" || set -x  # restore set -x
  if [ "${OSTYPE}" = msys ]; then
    PATH="${PATH// :/:}"  # HACK: Work around https://github.com/actions/virtual-environments/issues/635#issue-589318442
  fi
  PYTHON3_BIN_PATH=python
  export PATH PYTHON3_BIN_PATH
}

preload() {
  local job_names="${1-}"

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
  preload

  if [ "${OSTYPE}" = msys ]; then
    export USE_CLANG_CL=1
  fi

  local wheels="${LINUX_WHEELS-}${MAC_WHEELS-}"
  if [ -z "${wheels}" ]; then  # NOT building wheels
    "${ROOT_DIR}"/install-bazel.sh
  fi
  . "${ROOT_DIR}"/install-dependencies.sh
  reload_env  # We just modified our environment; reload it so we can continue
}

build() {
  local wheels="${LINUX_WHEELS-}${MAC_WHEELS-}"
  if [ -z "${wheels}" ]; then  # NOT building wheels
    if [ "${LINT-}" != 1 ]; then  # NOT linting
      bazel build -k "//:*"   # Do a full build first to ensure it passes
      "${ROOT_DIR}"/install-ray.sh
    fi
  fi

  if [ "${RAY_CYTHON_EXAMPLES-}" = 1 ]; then
    "${ROOT_DIR}"/install-cython-examples.sh
  fi

  if [ "${RAY_DEFAULT_BUILD-}" = 1 ]; then
    eval "$(curl -sL https://raw.githubusercontent.com/travis-ci/gimme/master/gimme | GIMME_GO_VERSION=stable bash)"
  fi

  if [ "${LINUX_WHEELS-}" = 1 ]; then
    # Mount bazel cache dir to the docker container.
    # For the linux wheel build, we use a shared cache between all
    # wheels, but not between different travis runs, because that
    # caused timeouts in the past. See the "cache: false" line below.
    local MOUNT_BAZEL_CACHE=(-v "${HOME}/ray-bazel-cache":/root/ray-bazel-cache -e TRAVIS=true -e TRAVIS_PULL_REQUEST="${TRAVIS_PULL_REQUEST}" -e encrypted_1c30b31fe1ee_key="${encrypted_1c30b31fe1ee_key-}" -e encrypted_1c30b31fe1ee_iv="${encrypted_1c30b31fe1ee_iv-}")

    # This command should be kept in sync with ray/python/README-building-wheels.md,
    # except the "${MOUNT_BAZEL_CACHE[@]}" part.
    suppress_output docker run --rm -w /ray -v "${PWD}":/ray "${MOUNT_BAZEL_CACHE[@]}" -e TRAVIS_COMMIT="${TRAVIS_COMMIT}" -ti rayproject/arrow_linux_x86_64_base:python-3.8.0 /ray/python/build-wheel-manylinux1.sh
  fi

  if [ "${MAC_WHEELS-}" = 1 ]; then
    # This command should be kept in sync with ray/python/README-building-wheels.md.
    suppress_output "${WORKSPACE_DIR}"/python/build-wheel-macos.sh
  fi
}

"$@"

{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null  # Pop caller's shell options (quietly)
