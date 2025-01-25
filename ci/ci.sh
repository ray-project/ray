#!/usr/bin/env bash

# Push caller's shell options (quietly)
{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null

set -eo pipefail
if [ -z "${TRAVIS_PULL_REQUEST-}" ] || [ -n "${OSTYPE##darwin*}" ]; then set -ux; fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"
WORKSPACE_DIR="${ROOT_DIR}/.."

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

_need_wheels() {
  local result="false"
  case "${OSTYPE}" in
    linux*) if [[ "${LINUX_WHEELS-}" == "1" ]]; then result="true"; fi;;
    darwin*) if [[ "${MAC_WHEELS-}" == "1" ]]; then result="true"; fi;;
    msys*) if [[ "${WINDOWS_WHEELS-}" == "1" ]]; then result="true"; fi;;
  esac
  echo "${result}"
}

NEED_WHEELS="$(_need_wheels)"

compile_pip_dependencies() {
  # Compile boundaries
  TARGET="${1-requirements_compiled.txt}"

  if [[ "${HOSTTYPE}" == "aarch64" || "${HOSTTYPE}" = "arm64" ]]; then
    # Resolution currently does not work on aarch64 as some pinned packages
    # are not available. Once they are reasonably upgraded we should be able
    # to enable this here.p
    echo "Skipping for aarch64"
    return 0
  fi

  (
    # shellcheck disable=SC2262
    alias pip="python -m pip"

    cd "${WORKSPACE_DIR}"

    echo "Target file: $TARGET"
    pip install pip-tools

    # Required packages to lookup e.g. dragonfly-opt
    HAS_TORCH=0
    python -c "import torch" 2>/dev/null && HAS_TORCH=1
    pip install --no-cache-dir numpy torch

    pip-compile --verbose --resolver=backtracking \
      --pip-args --no-deps --strip-extras --no-header \
      --unsafe-package ray \
      --unsafe-package pip \
      --unsafe-package setuptools \
      -o "python/$TARGET" \
      python/requirements.txt \
      python/requirements/lint-requirements.txt \
      python/requirements/test-requirements.txt \
      python/requirements/cloud-requirements.txt \
      python/requirements/docker/ray-docker-requirements.txt \
      python/requirements/ml/core-requirements.txt \
      python/requirements/ml/data-requirements.txt \
      python/requirements/ml/data-test-requirements.txt \
      python/requirements/ml/dl-cpu-requirements.txt \
      python/requirements/ml/rllib-requirements.txt \
      python/requirements/ml/rllib-test-requirements.txt \
      python/requirements/ml/train-requirements.txt \
      python/requirements/ml/train-test-requirements.txt \
      python/requirements/ml/tune-requirements.txt \
      python/requirements/ml/tune-test-requirements.txt \
      python/requirements/security-requirements.txt

    # Delete local installation
    sed -i "/@ file/d" "python/$TARGET"

    # Remove +cpu and +pt20cpu suffixes e.g. for torch dependencies
    # This is needed because we specify the requirements as torch==version, but
    # the resolver adds the device-specific version tag. If this is not removed,
    # pip install will complain about irresolvable constraints.
    sed -i -E 's/==([\.0-9]+)\+[^\b]*cpu/==\1/g' "python/$TARGET"

    cat "python/$TARGET"

    if [[ "$HAS_TORCH" == "0" ]]; then
      pip uninstall -y torch
    fi
  )
}

test_cpp() {
  # C++ worker example need _GLIBCXX_USE_CXX11_ABI flag, but if we put the flag into .bazelrc, the linux ci can't pass.
  # So only set the flag in c++ worker example. More details: https://github.com/ray-project/ray/pull/18273
  echo build --cxxopt="-D_GLIBCXX_USE_CXX11_ABI=0" >> ~/.bazelrc
  bazel build --config=ci //cpp:all

  BAZEL_EXPORT_OPTIONS=($(./ci/run/bazel_export_options))
  bazel test --config=ci "${BAZEL_EXPORT_OPTIONS[@]}" --test_strategy=exclusive //cpp:all --build_tests_only
  # run cluster mode test with external cluster
  bazel test //cpp:cluster_mode_test --test_arg=--external_cluster=true \
    --test_arg=--ray_redis_password="1234" --test_arg=--ray_redis_username="default"
  bazel test --test_output=all //cpp:test_python_call_cpp

  # run the cpp example, currently does not work on mac
  if [[ "${OSTYPE}" != darwin* ]]; then
    rm -rf ray-template
    ray cpp --generate-bazel-project-template-to ray-template
    pushd ray-template && bash run.sh
  fi
}

test_wheels() {
  local TEST_WHEEL_RESULT=0

  "${WORKSPACE_DIR}"/ci/build/test-wheels.sh || TEST_WHEEL_RESULT=$?

  if [[ "${TEST_WHEEL_RESULT}" != 0 ]]; then
    cat -- /tmp/ray/session_latest/logs/* || true
    sleep 60  # Explicitly sleep 60 seconds for logs to go through
  fi

  return "${TEST_WHEEL_RESULT}"
}

install_npm_project() {
  if [ "${OSTYPE}" = msys ]; then
    # Not Windows-compatible: https://github.com/npm/cli/issues/558#issuecomment-584673763
    { echo "WARNING: Skipping NPM due to module incompatibilities with Windows"; } 2> /dev/null
  else
    npm ci
  fi
}

build_dashboard_front_end() {
  if [ "${OSTYPE}" = msys ]; then
    { echo "WARNING: Skipping dashboard due to NPM incompatibilities with Windows"; } 2> /dev/null
  elif [ "${NO_DASHBOARD-}" = "1" ]; then
    echo "Skipping dashboard build"
  else
    (
      cd ray/dashboard/client

      # skip nvm activation on buildkite linux instances.
      if [ -z "${BUILDKITE-}" ] || [[ "${OSTYPE}" != linux* ]]; then
        set +x  # suppress set -x since it'll get very noisy here
        . "${HOME}/.nvm/nvm.sh"
        NODE_VERSION="14"
        nvm install $NODE_VERSION
        nvm use --silent $NODE_VERSION
      fi
      install_npm_project
      npm run build
    )
  fi
}

build_sphinx_docs() {
  install_ray

  (
    cd "${WORKSPACE_DIR}"/doc
    if [ "${OSTYPE}" = msys ]; then
      echo "WARNING: Documentation not built on Windows due to currently-unresolved issues"
    else
      make html
      pip install datasets==2.0.0
    fi
  )
}

check_sphinx_links() {
  (
    cd "${WORKSPACE_DIR}"/doc
    if [ "${OSTYPE}" = msys ]; then
      echo "WARNING: Documentation not built on Windows due to currently-unresolved issues"
    else
      make linkcheck
    fi
  )
}

_bazel_build_before_install() {
  local target
  if [ "${OSTYPE}" = msys ]; then
    target="//:ray_pkg"
  else
    # Just build Python on other platforms.
    # This because pip install captures & suppresses the build output, which causes a timeout on CI.
    target="//:ray_pkg"
  fi
  # NOTE: Do not add build flags here. Use .bazelrc and --config instead.

  if [ -z "${RAY_DEBUG_BUILD-}" ]; then
    bazel build "${target}"
  elif [ "${RAY_DEBUG_BUILD}" = "asan" ]; then
    # bazel build --config asan "${target}"
    echo "Not needed"
  elif [ "${RAY_DEBUG_BUILD}" = "debug" ]; then
    bazel build --config debug "${target}"
  else
    echo "Invalid config given"
    exit 1
  fi
}

install_ray() {
  # TODO(mehrdadn): This function should be unified with the one in python/build-wheel-windows.sh.
  (
    cd "${WORKSPACE_DIR}"/python
    build_dashboard_front_end
    keep_alive pip install -v -e .
  )
  (
    # For runtime_env tests, wheels are needed
    cd "${WORKSPACE_DIR}"
    keep_alive pip wheel -e python -w .whl
  )
}

validate_wheels_commit_str() {
  if [ "${OSTYPE}" = msys ]; then
    echo "Windows builds do not set the commit string, skipping wheel commit validity check."
    return 0
  fi

  if [ -n "${BUILDKITE_COMMIT}" ]; then
    EXPECTED_COMMIT=${BUILDKITE_COMMIT:-}
  else
    EXPECTED_COMMIT=${TRAVIS_COMMIT:-}
  fi

  if [ -z "$EXPECTED_COMMIT" ]; then
    echo "Could not validate expected wheel commits: TRAVIS_COMMIT is empty."
    return 0
  fi

  for whl in .whl/*.whl; do
    basename=${whl##*/}

    if [[ "$basename" =~ "_cpp" ]]; then
      # cpp wheels cannot be checked this way
      echo "Skipping CPP wheel ${basename} for wheel commit validation."
      continue
    fi

    WHL_COMMIT=$(unzip -p "$whl" "*ray/_version.py" | grep "^commit" | awk -F'"' '{print $2}')

    if [ "${WHL_COMMIT}" != "${EXPECTED_COMMIT}" ]; then
      echo "Wheel ${basename} has incorrect commit: (${WHL_COMMIT}) is not expected commit (${EXPECTED_COMMIT}). Aborting."
      exit 1
    fi

    echo "Wheel ${basename} has the correct commit: ${WHL_COMMIT}"
  done

  echo "All wheels passed the sanity check and have the correct wheel commit set."
}

build_wheels_and_jars() {
  _bazel_build_before_install

  # Create wheel output directory and empty contents
  # If buildkite runners are re-used, wheels from previous builds might be here, so we delete them.
  mkdir -p .whl
  rm -rf .whl/* || true

  case "${OSTYPE}" in
    linux*)
      # Mount bazel cache dir to the docker container.
      # For the linux wheel build, we use a shared cache between all
      # wheels, but not between different travis runs, because that
      # caused timeouts in the past. See the "cache: false" line below.
      local MOUNT_BAZEL_CACHE=(
        -e "TRAVIS=true"
        -e "TRAVIS_PULL_REQUEST=${TRAVIS_PULL_REQUEST:-false}"
        -e "TRAVIS_COMMIT=${TRAVIS_COMMIT}"
        -e "CI=${CI}"
        -e "RAY_INSTALL_JAVA=${RAY_INSTALL_JAVA:-1}"
        -e "BUILDKITE=${BUILDKITE:-}"
        -e "BUILDKITE_PULL_REQUEST=${BUILDKITE_PULL_REQUEST:-}"
        -e "BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL:-}"
        -e "RAY_DEBUG_BUILD=${RAY_DEBUG_BUILD:-}"
        -e "BUILD_ONE_PYTHON_ONLY=${BUILD_ONE_PYTHON_ONLY:-}"
      )

      IMAGE_NAME="quay.io/pypa/manylinux2014_${HOSTTYPE}"
      IMAGE_TAG="2022-12-20-b4884d9"

      local MOUNT_ENV=()
      if [[ "${LINUX_JARS-}" == "1" ]]; then
        MOUNT_ENV+=(-e "BUILD_JAR=1")
      fi

      if [[ -z "${BUILDKITE-}" ]]; then
        # This command should be kept in sync with ray/python/README-building-wheels.md,
        # except the "${MOUNT_BAZEL_CACHE[@]}" part.
        docker run --rm -w /ray -v "${PWD}":/ray "${MOUNT_BAZEL_CACHE[@]}" \
          "${MOUNT_ENV[@]}" "${IMAGE_NAME}:${IMAGE_TAG}" /ray/python/build-wheel-manylinux2014.sh
      else
        rm -rf /ray-mount/*
        rm -rf /ray-mount/.whl || true
        rm -rf /ray/.whl || true
        cp -rT /ray /ray-mount
        ls -a /ray-mount
        docker run --rm -w /ray -v /ray:/ray "${MOUNT_BAZEL_CACHE[@]}" \
          "${MOUNT_ENV[@]}" "${IMAGE_NAME}:${IMAGE_TAG}" /ray/python/build-wheel-manylinux2014.sh
        cp -rT /ray-mount /ray # copy new files back here
        find . | grep whl # testing

        # Sync the directory to buildkite artifacts
        rm -rf /artifact-mount/.whl || true

        if [ "${UPLOAD_WHEELS_AS_ARTIFACTS-}" = "1" ]; then
          cp -r .whl /artifact-mount/.whl
          chmod -R 777 /artifact-mount/.whl
        fi

        validate_wheels_commit_str
      fi
      ;;
    darwin*)
      # This command should be kept in sync with ray/python/README-building-wheels.md.
      "${WORKSPACE_DIR}"/python/build-wheel-macos.sh
      mkdir -p /tmp/artifacts/.whl
      rm -rf /tmp/artifacts/.whl || true

      if [[ "${UPLOAD_WHEELS_AS_ARTIFACTS-}" == "1" ]]; then
        cp -r .whl /tmp/artifacts/.whl
        chmod -R 777 /tmp/artifacts/.whl
      fi

      validate_wheels_commit_str
      ;;
    msys*)
      keep_alive "${WORKSPACE_DIR}"/python/build-wheel-windows.sh
      ;;
  esac
}

configure_system() {
  git config --global advice.detachedHead false
  git config --global core.askpass ""
  git config --global credential.helper ""
  git config --global credential.modalprompt false

  # Requests library need root certificates.
  if [[ "${OSTYPE}" == "msys" ]]; then
    certutil -generateSSTFromWU roots.sst && certutil -addstore -f root roots.sst && rm roots.sst
  fi
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
  configure_system

  "${ROOT_DIR}/env/install-dependencies.sh"
}

build() {
  if [[ "${NEED_WHEELS}" == "true" ]]; then
    build_wheels_and_jars
    return
  fi

  # Build and install ray into the system.
  # For building the wheel, see build_wheels_and_jars.
  _bazel_build_before_install
  install_ray
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
