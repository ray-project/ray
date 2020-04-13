#!/usr/bin/env bash

{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null  # Push caller's shell options (quietly)

unset -f cd  # Travis defines this on Mac for RVM, but it floods the trace log and isn't relevant for us

set -eo pipefail && if [ -n "${OSTYPE##darwin*}" ]; then set -ux; fi  # some options interfere with Travis's RVM on Mac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"
WORKSPACE_DIR="${ROOT_DIR}/../.."

should_run_job() {
  local skip=0 envvar
  if [ -n "$1" ]; then  # were any triggers provided? (if not, then the job will always run)
    local active_triggers=()
    for envvar in ${1//,/ }; do
      if [ "${!envvar}" = 1 ]; then
        active_triggers+="${envvar}=${!envvar}"  # success! we found at least one of the given triggers is occurring
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

prepare() {
  local test_env_var_names="${1-}"  # optional comma-separated list of names of environment variables that trigger us
  local script
  script="$(python "${ROOT_DIR}"/determine_tests_to_run.py)"
  eval "${script}"
  if ! should_run_job "${test_env_var_names}"; then
    exit 0  # we use 'exit' instead of 'return' so that when this script is sourced, the entire script exits
  fi

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
      . "${ROOT_DIR}"/install-ray.sh
    fi
  fi

  if [ "${RAY_CYTHON_EXAMPLES-}" = 1 ]; then
    . "${ROOT_DIR}"/install-cython-examples.sh
  fi

  if [ "${LINUX_WHEELS-}" = 1 ]; then
    # Mount bazel cache dir to the docker container.
    # For the linux wheel build, we use a shared cache between all
    # wheels, but not between different travis runs, because that
    # caused timeouts in the past. See the "cache: false" line below.
    local MOUNT_BAZEL_CACHE=(-v "${HOME}/ray-bazel-cache":/root/ray-bazel-cache -e TRAVIS=true -e TRAVIS_PULL_REQUEST="${TRAVIS_PULL_REQUEST}" -e encrypted_1c30b31fe1ee_key="${encrypted_1c30b31fe1ee_key-}" -e encrypted_1c30b31fe1ee_iv="${encrypted_1c30b31fe1ee_iv-}")

    # This command should be kept in sync with ray/python/README-building-wheels.md,
    # except the "${MOUNT_BAZEL_CACHE[@]}" part.
    "${WORKSPACE_DIR}"/ci/suppress_output docker run --rm -w /ray -v "${PWD}":/ray "${MOUNT_BAZEL_CACHE[@]}" -e TRAVIS_COMMIT="${TRAVIS_COMMIT}" -ti rayproject/arrow_linux_x86_64_base:python-3.8.0 /ray/python/build-wheel-manylinux1.sh
  fi

  if [ "${MAC_WHEELS-}" = 1 ]; then
    # This command should be kept in sync with ray/python/README-building-wheels.md.
    "${WORKSPACE_DIR}"/ci/suppress_output "${WORKSPACE_DIR}"/python/build-wheel-macos.sh
  fi
}

"$@"

{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null  # Pop caller's shell options (quietly)
