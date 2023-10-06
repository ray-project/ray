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

  echo "Target file: $TARGET"

  # shellcheck disable=SC2262
  alias pip="python -m pip"
  pip install pip-tools

  # Required packages to lookup e.g. dragonfly-opt
  HAS_TORCH=0
  python -c "import torch" 2>/dev/null && HAS_TORCH=1
  pip install --no-cache-dir numpy torch

  pip-compile --resolver=backtracking -q \
     --pip-args --no-deps --strip-extras --no-header -o \
    "${WORKSPACE_DIR}/python/$TARGET" \
    "${WORKSPACE_DIR}/python/requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/lint-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/test-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/docker/ray-docker-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/core-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/data-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/data-test-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/dl-cpu-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/rllib-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/rllib-test-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/train-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/train-test-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/tune-requirements.txt" \
    "${WORKSPACE_DIR}/python/requirements/ml/tune-test-requirements.txt" \
    "${WORKSPACE_DIR}/doc/requirements-doc.txt"

  # Remove some pins from upstream dependencies:
  # ray, xgboost-ray, lightgbm-ray, tune-sklearn
  sed -i "/^ray==/d;/^xgboost-ray==/d;/^lightgbm-ray==/d;/^tune-sklearn==/d" "${WORKSPACE_DIR}/python/$TARGET"

  # Delete local installation
  sed -i "/@ file/d" "${WORKSPACE_DIR}/python/$TARGET"

  # Remove +cpu and +pt20cpu suffixes e.g. for torch dependencies
  # This is needed because we specify the requirements as torch==version, but
  # the resolver adds the device-specific version tag. If this is not removed,
  # pip install will complain about irresolvable constraints.
  sed -i -E 's/==([\.0-9]+)\+[^\b]*cpu/==\1/g' "${WORKSPACE_DIR}/python/$TARGET"

  # Add python_version < 3.11 to scikit-image, scikit-optimize, scipy, networkx
  # as they need more recent versions in python 3.11.
  # These will be automatically resolved. Remove as
  # soon as we resolve to versions of scikit-image that are built for py311.
  sed -i -E 's/((scikit-image|scikit-optimize|scipy|networkx)==[\.0-9]+\b)/\1 ; python_version < "3.11"/g' "${WORKSPACE_DIR}/python/$TARGET"

  cat "${WORKSPACE_DIR}/python/$TARGET"

  if [ "$HAS_TORCH" -eq 0 ]; then
    pip uninstall -y torch
  fi
}

test_core() {
  local args=(
    "//:*" "//src/..."
  )
  case "${OSTYPE}" in
    msys)
      args+=(
        -//:core_worker_test
        -//src/ray/util/tests:event_test
        -//:gcs_server_rpc_test
        -//src/ray/common/test:ray_syncer_test # TODO (iycheng): it's flaky on windows. Add it back once we figure out the cause
        -//:gcs_health_check_manager_test
        -//:gcs_client_reconnection_test
      )
      ;;
  esac
  # shellcheck disable=SC2046
  bazel test --config=ci --build_tests_only $(./ci/run/bazel_export_options) -- "${args[@]}"
}

prepare_docker() {
    rm "${WORKSPACE_DIR}"/python/dist/* ||:
    pushd "${WORKSPACE_DIR}/python"
    pip install -e . --verbose
    python setup.py bdist_wheel
    tmp_dir="/tmp/prepare_docker_$RANDOM"
    mkdir -p $tmp_dir
    cp "${WORKSPACE_DIR}"/python/dist/*.whl $tmp_dir
    wheel=$(ls "${WORKSPACE_DIR}"/python/dist/)
    base_image=$(python -c "import sys; print(f'rayproject/ray-deps:nightly-py{sys.version_info[0]}{sys.version_info[1]}-cpu')")
    echo "
    FROM $base_image

    ENV LC_ALL=C.UTF-8
    ENV LANG=C.UTF-8
    COPY ./*.whl /
    EXPOSE 8000
    EXPOSE 10001
    RUN pip install /${wheel}[serve]
    RUN (sudo apt update || true) && sudo apt install curl -y
    " > $tmp_dir/Dockerfile

    pushd $tmp_dir
    docker build . -t ray_ci:v1
    popd

    popd
}

# For running Serve tests on Windows.
test_serve() {
  local pathsep=":" args=()
  if [ "${OSTYPE}" = msys ]; then
    pathsep=";"
    args+=(
      python/ray/serve/...
      -python/ray/serve/tests:test_cross_language # Ray java not built on Windows yet.
      -python/ray/serve/tests:test_gcs_failure # Fork not supported in windows
      -python/ray/serve/tests:test_standalone_2 # Multinode not supported on Windows
      -python/ray/serve/tests:test_gradio
      -python/ray/serve/tests:test_gradio_visualization
      -python/ray/serve/tests:test_air_integrations_gpu
      -python/ray/serve/tests:test_fastapi
      -python/ray/serve/tests:test_get_deployment # address violation
      -python/ray/serve/tests:test_proxy_state # skip running on Windows until we refactor the test
    )
  fi
  if [ 0 -lt "${#args[@]}" ]; then  # Any targets to test?
    install_ray

    # Shard the args.
    BUILDKITE_PARALLEL_JOB=${BUILDKITE_PARALLEL_JOB:-'0'}
    BUILDKITE_PARALLEL_JOB_COUNT=${BUILDKITE_PARALLEL_JOB_COUNT:-'1'}
    test_shard_selection=$(python ./ci/ray_ci/bazel_sharding.py --exclude_manual --index "${BUILDKITE_PARALLEL_JOB}" --count "${BUILDKITE_PARALLEL_JOB_COUNT}" "${args[@]}")

    # TODO(mehrdadn): We set PYTHONPATH here to let Python find our pickle5 under pip install -e.
    # It's unclear to me if this should be necessary, but this is to make tests run for now.
    # Check why this issue doesn't arise on Linux/Mac.
    # Ideally importing ray.cloudpickle should import pickle5 automatically.
    # shellcheck disable=SC2046,SC2086
    bazel test --config=ci \
      --build_tests_only $(./ci/run/bazel_export_options) \
      --test_env=PYTHONPATH="${PYTHONPATH-}${pathsep}${WORKSPACE_DIR}/python/ray/pickle5_files" \
      --test_env=CI="1" \
      --test_env=RAY_CI_POST_WHEEL_TESTS="1" \
      --test_env=USERPROFILE="${USERPROFILE}" \
      --test_output=streamed \
      -- \
      ${test_shard_selection};
  fi
}

# For running Python tests on Windows (excluding Serve).
test_python() {
  local pathsep=":" args=()
  if [ "${OSTYPE}" = msys ]; then
    pathsep=";"
    args+=(
      python/ray/tests/...
      python/ray/train:test_windows
      -python/ray/tests:test_actor_advanced  # crashes in shutdown
      -python/ray/tests:test_autoscaler # We don't support Autoscaler on Windows
      -python/ray/tests:test_autoscaler_aws
      -python/ray/tests:test_cli
      -python/ray/tests:test_client_init # timeout
      -python/ray/tests:test_command_runner # We don't support Autoscaler on Windows
      -python/ray/tests:test_gcp_tpu_command_runner # We don't support Autoscaler on Windows
      -python/ray/tests:test_gcs_fault_tolerance # flaky
      -python/ray/tests:test_global_gc
      -python/ray/tests:test_job
      -python/ray/tests:test_memstat
      -python/ray/tests:test_multi_node_3
      -python/ray/tests:test_multiprocessing_client_mode # Flaky on Windows
      -python/ray/tests:test_object_manager # OOM on test_object_directory_basic
      -python/ray/tests:test_resource_demand_scheduler
      -python/ray/tests:test_stress  # timeout
      -python/ray/tests:test_stress_sharded  # timeout
      -python/ray/tests:test_tracing  # tracing not enabled on windows
      -python/ray/tests:kuberay/test_autoscaling_e2e # irrelevant on windows
      -python/ray/tests:vsphere/test_vsphere_node_provider # irrelevant on windows
      -python/ray/tests/xgboost/... # Requires ML dependencies, should not be run on Windows
      -python/ray/tests/lightgbm/... # Requires ML dependencies, should not be run on Windows
      -python/ray/tests/horovod/... # Requires ML dependencies, should not be run on Windows
      -python/ray/tests:test_batch_node_provider_unit.py # irrelevant on windows
      -python/ray/tests:test_batch_node_provider_integration.py # irrelevant on windows
    )
  fi
  if [ 0 -lt "${#args[@]}" ]; then  # Any targets to test?
    install_ray

    # Shard the args.
    BUILDKITE_PARALLEL_JOB=${BUILDKITE_PARALLEL_JOB:-'0'}
    BUILDKITE_PARALLEL_JOB_COUNT=${BUILDKITE_PARALLEL_JOB_COUNT:-'1'}
    test_shard_selection=$(python ./ci/ray_ci/bazel_sharding.py --exclude_manual --index "${BUILDKITE_PARALLEL_JOB}" --count "${BUILDKITE_PARALLEL_JOB_COUNT}" "${args[@]}")

    # TODO(mehrdadn): We set PYTHONPATH here to let Python find our pickle5 under pip install -e.
    # It's unclear to me if this should be necessary, but this is to make tests run for now.
    # Check why this issue doesn't arise on Linux/Mac.
    # Ideally importing ray.cloudpickle should import pickle5 automatically.
    # shellcheck disable=SC2046,SC2086
    bazel test --config=ci \
      --build_tests_only $(./ci/run/bazel_export_options) \
      --test_env=PYTHONPATH="${PYTHONPATH-}${pathsep}${WORKSPACE_DIR}/python/ray/pickle5_files" \
      --test_env=CI="1" \
      --test_env=RAY_CI_POST_WHEEL_TESTS="1" \
      --test_env=USERPROFILE="${USERPROFILE}" \
      --test_output=streamed \
      -- \
      ${test_shard_selection};
  fi
}

# For running large Python tests on Linux and MacOS.
test_large() {
  # shellcheck disable=SC2046
  bazel test --config=ci $(./ci/run/bazel_export_options) --test_env=CONDA_EXE --test_env=CONDA_PYTHON_EXE \
      --test_env=CONDA_SHLVL --test_env=CONDA_PREFIX --test_env=CONDA_DEFAULT_ENV --test_env=CONDA_PROMPT_MODIFIER \
      --test_env=CI --test_tag_filters="large_size_python_tests_shard_${BUILDKITE_PARALLEL_JOB}"  "$@" \
      -- python/ray/tests/...
}

test_cpp() {
  # C++ worker example need _GLIBCXX_USE_CXX11_ABI flag, but if we put the flag into .bazelrc, the linux ci can't pass.
  # So only set the flag in c++ worker example. More details: https://github.com/ray-project/ray/pull/18273
  echo build --cxxopt="-D_GLIBCXX_USE_CXX11_ABI=0" >> ~/.bazelrc
  bazel build --config=ci //cpp:all
  # shellcheck disable=SC2046
  bazel test --config=ci $(./ci/run/bazel_export_options) --test_strategy=exclusive //cpp:all --build_tests_only
  # run cluster mode test with external cluster
  bazel test //cpp:cluster_mode_test --test_arg=--external_cluster=true --test_arg=--redis_password="1234" \
    --test_arg=--ray_redis_password="1234"
  bazel test --test_output=all //cpp:test_python_call_cpp

  # run the cpp example
  rm -rf ray-template
  ray cpp --generate-bazel-project-template-to ray-template
  pushd ray-template && bash run.sh
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
      FAST=True make html
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
      FAST=True make linkcheck
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

    WHL_COMMIT=$(unzip -p "$whl" "*ray/__init__.py" | grep "^__commit__" | awk -F'"' '{print $2}')

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
        -v "${HOME}/ray-bazel-cache":/root/ray-bazel-cache
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
        docker run --rm -v /ray:/ray-mounted ubuntu:focal ls /
        docker run --rm -v /ray:/ray-mounted ubuntu:focal ls /ray-mounted
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

_check_job_triggers() {
  local job_names
  job_names="$1"

  local variable_definitions
  if command -v python3; then
    # shellcheck disable=SC2031
    variable_definitions=($(python3 "${ROOT_DIR}"/pipeline/determine_tests_to_run.py))
  else
    # shellcheck disable=SC2031
    variable_definitions=($(python "${ROOT_DIR}"/pipeline/determine_tests_to_run.py))
  fi
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

  # Requests library need root certificates.
  if [ "${OSTYPE}" = msys ]; then
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
  # TODO(jjyao): fix it for windows
  if [ "${OSTYPE}" != msys ]; then
    _check_job_triggers "${1-}"
  fi

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

run_minimal_test() {
  EXPECTED_PYTHON_VERSION=$1
  BAZEL_EXPORT_OPTIONS="$(./ci/run/bazel_export_options)"
  # Ignoring shellcheck is necessary because if ${BAZEL_EXPORT_OPTIONS} is wrapped by the double quotation,
  # bazel test cannot recognize the option.
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 --test_env=EXPECTED_PYTHON_VERSION=$EXPECTED_PYTHON_VERSION ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_minimal_install
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 --test_env=TEST_EXTERNAL_REDIS=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic_2
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 --test_env=TEST_EXTERNAL_REDIS=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic_2
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic_3
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 --test_env=TEST_EXTERNAL_REDIS=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic_3
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic_4
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 --test_env=TEST_EXTERNAL_REDIS=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic_4
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic_5
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 --test_env=TEST_EXTERNAL_REDIS=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_basic_5
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_output
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_runtime_env_ray_minimal
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_utils

  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_serve_ray_minimal
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 ${BAZEL_EXPORT_OPTIONS} python/ray/dashboard/test_dashboard
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_usage_stats
  # shellcheck disable=SC2086
  bazel test --test_output=streamed --config=ci --test_env=RAY_MINIMAL=1 --test_env=TEST_EXTERNAL_REDIS=1 ${BAZEL_EXPORT_OPTIONS} python/ray/tests/test_usage_stats
}

test_minimal() {
  ./ci/env/install-minimal.sh "$1"
  echo "Installed minimal dependencies."
  ./ci/env/env_info.sh
  python ./ci/env/check_minimal_install.py --expected-python-version "$1"
  run_minimal_test "$1"
}


test_latest_core_dependencies() {
  ./ci/env/install-minimal.sh "$1"
  echo "Installed minimal dependencies."
  ./ci/env/env_info.sh
  ./ci/env/install-core-prerelease-dependencies.sh
  echo "Installed Core prerelease dependencies."
  ./ci/env/env_info.sh
  run_minimal_test "$1"
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
