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

# Source requirements compiled into requirements_compiled*.txt.
COMPILE_PIP_SOURCES=(
  python/requirements.txt
  python/requirements/lint-requirements.txt
  python/requirements/test-requirements.txt
  python/requirements/cloud-requirements.txt
  python/requirements/serve/serve-test-requirements.txt
  python/requirements/docker/ray-docker-requirements.txt
  python/requirements/ml/core-requirements.txt
  python/requirements/ml/data-requirements.txt
  python/requirements/ml/data-test-requirements.txt
  python/requirements/ml/dl-cpu-requirements.txt
  python/requirements/ml/ml-requirements.txt
  python/requirements/ml/third_party.txt
  python/requirements/ml/rllib-requirements.txt
  python/requirements/ml/rllib-test-requirements.txt
  python/requirements/ml/train-requirements.txt
  python/requirements/ml/train-test-requirements.txt
  python/requirements/ml/tune-requirements.txt
  python/requirements/ml/tune-test-requirements.txt
  python/requirements/security-requirements.txt
)

# Python versions that get their own compiled constraints file. 3.11's is
# requirements_compiled.txt (requirements_compiled_py3.11.txt links to it); the
# others are requirements_compiled_py3.X.txt.
COMPILED_PYTHON_VERSIONS=(3.10 3.11 3.12 3.13 3.14)

# Compiles one constraints file per Python version from COMPILE_PIP_SOURCES,
# each resolved for that version alone. uv prefers the pins already in each
# output file, so a recompile only moves what the sources force.
compile_pip_dependencies() {
  # 3.14 installs from wheels only, so every 3.14 pin has a cp314 wheel. These
  # ship only an sdist, for every Python version, so building them is not a 3.14
  # regression. hyperopt is pinned to a git commit, so it is always built from
  # source. (3.10-3.13 still build a few pinned sdists, e.g. hpbandster.)
  local sdist_only=(
    crcmod deepspeed fairscale feather-format gcs-oauth2-boto-plugin gsutil halo
    hyperopt promise pyspark pyu2f retry-decorator s3torchconnector
  )
  local wheels_only_args=(--only-binary :all:)
  for pkg in "${sdist_only[@]}"; do
    wheels_only_args+=(--no-binary "${pkg}")
  done

  (
    cd "${WORKSPACE_DIR}"
    # Match the uv that raydepsets pins (uv_x86_64-linux in WORKSPACE).
    pip install "uv==0.13.4"

    local version target binary_args override_args
    for version in "${COMPILED_PYTHON_VERSIONS[@]}"; do
      if [[ "${version}" == "3.11" ]]; then
        target="python/requirements_compiled.txt"
      else
        target="python/requirements_compiled_py${version}.txt"
      fi
      binary_args=()
      if [[ "${version}" == "3.14" ]]; then
        binary_args=("${wheels_only_args[@]}")
      fi
      # lerobot (3.12 only) caps wandb and packaging below Ray's pins. Lift the
      # caps the same way the 3.12 data depsets do.
      override_args=()
      if [[ "${version}" == "3.12" ]]; then
        override_args=(--override python/requirements/ml/lerobot_overrides.txt)
      fi
      echo "--- Compiling ${target} (Python ${version})"

      # uv writes whatever index it used into the lockfile. Keep CI's temporary
      # mirror URL out, or the regenerated file stops matching the committed
      # one. Ray images are glibc 2.35 (Ubuntu 22.04), hence manylinux_2_35.
      env -u PIP_INDEX_URL -u PIP_EXTRA_INDEX_URL -u PIP_TRUSTED_HOST \
        -u UV_INDEX_URL -u UV_EXTRA_INDEX_URL -u UV_INSECURE_HOST \
        uv pip compile --python-version "${version}" \
        --python-platform x86_64-manylinux_2_35 \
        --no-header --strip-extras --emit-index-url --emit-find-links \
        --index-strategy unsafe-best-match \
        "${binary_args[@]}" "${override_args[@]}" \
        --unsafe-package ray \
        --unsafe-package pip \
        --unsafe-package setuptools \
        -o "${target}" \
        "${COMPILE_PIP_SOURCES[@]}"

      sed -i -e "/^--index-url /d" -e "/@ file/d" "${target}"

      # Strip the local version segment from CPU builds (+cpu, +pt210cpu,
      # +cpu.torch.2.10). The GPU depsets also read these files, and only a
      # bare torch-scatter==2.1.2 lets their +cu.12.8.torch.2.10 pin satisfy
      # it. The public version may carry a pre/post/dev suffix (2.10.0rc1,
      # 2.10.0.post0), hence letters in the first class.
      sed -i -E 's/==([A-Za-z0-9.]+)[+][A-Za-z0-9._-]*cpu[A-Za-z0-9._-]*/==\1/g' "${target}"
    done
  )
}

test_cpp() {
  if [[ "${OSTYPE}" == darwin* ]]; then
    echo "use macos_ci.sh to run cpp tests"
    exit 1
  fi

  # C++ worker example need _GLIBCXX_USE_CXX11_ABI flag, but if we put the flag into .bazelrc, the linux ci can't pass.
  # So only set the flag in c++ worker example. More details: https://github.com/ray-project/ray/pull/18273
  echo build --cxxopt="-D_GLIBCXX_USE_CXX11_ABI=0" >> ~/.bazelrc
  bazel build --config=ci //cpp:all
  bazel run --config=ci //cpp:gen_ray_cpp_pkg

  BAZEL_EXPORT_OPTIONS=($(./ci/run/bazel_export_options))
  bazel test --config=ci "${BAZEL_EXPORT_OPTIONS[@]}" --test_strategy=exclusive //cpp:all --build_tests_only
  # run cluster mode test with external cluster
  bazel test --config=ci //cpp:cluster_mode_test --test_arg=--external_cluster=true \
    --test_arg=--ray_redis_password="1234" --test_arg=--ray_redis_username="default"
  bazel test --config=ci --test_output=all //cpp:test_python_call_cpp

  # run the cpp example, currently does not work on mac
  rm -rf ray-template
  ray cpp --generate-bazel-project-template-to ray-template
  (
    cd ray-template
    bash run.sh
  )
}

test_macos_wheels() {
  local TEST_WHEEL_RESULT=0

  "${WORKSPACE_DIR}"/ci/build/test-macos-wheels.sh || TEST_WHEEL_RESULT=$?

  if [[ "${TEST_WHEEL_RESULT}" != 0 ]]; then
    cat -- /tmp/ray/session_latest/logs/* || true
    sleep 60  # Explicitly sleep 60 seconds for logs to go through
  fi

  return "${TEST_WHEEL_RESULT}"
}

_install_npm_project() {
  if [[ "${OSTYPE}" == msys ]]; then
    # Not Windows-compatible: https://github.com/npm/cli/issues/558#issuecomment-584673763
    { echo "WARNING: Skipping NPM due to module incompatibilities with Windows"; } 2> /dev/null
  else
    npm ci
  fi
}

build_dashboard_front_end() {
  if [[ "${OSTYPE}" == msys ]]; then
    { echo "WARNING: Skipping dashboard due to NPM incompatibilities with Windows"; } 2> /dev/null
  elif [[ "${NO_DASHBOARD-}" == "1" ]]; then
    echo "Skipping dashboard build"
  else
    (
      cd ray/dashboard/client

      # skip nvm activation on buildkite linux instances.
      if [[ -z "${BUILDKITE-}" || "${OSTYPE}" != linux* ]]; then
        if [[ -d "${HOME}/.nvm" ]]; then
          set +x  # suppress set -x since it'll get very noisy here
          export TMPDIR="${TMPDIR:-/tmp}"
          . "${HOME}/.nvm/nvm.sh"
          NODE_VERSION="20"
          nvm install $NODE_VERSION
          nvm use --silent $NODE_VERSION
        fi
      fi
      _install_npm_project
      npm run build
    )
  fi
}

build_sphinx_docs() {
  install_ray

  (
    cd "${WORKSPACE_DIR}"/doc
    if [[ "${OSTYPE}" == msys ]]; then
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
    if [[ "${OSTYPE}" == msys ]]; then
      echo "WARNING: Documentation not built on Windows due to currently-unresolved issues"
    else
      make linkcheck
    fi
  )
}

_bazel_build_before_install() {
  # NOTE: Do not add build flags here. Use .bazelrc and --config instead.

  if [[ -z "${RAY_DEBUG_BUILD:-}" ]]; then
    bazel run //:gen_ray_pkg
  elif [[ "${RAY_DEBUG_BUILD}" == "asan" ]]; then
    echo "No need to build anything before install"
  elif [[ "${RAY_DEBUG_BUILD}" == "debug" ]]; then
    bazel run --config debug //:gen_ray_pkg
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

    # This is required so that pip does not pick up a cython version that is
    # too high that can break CI, especially on MacOS.
    pip install -q cython==3.0.12

    # editable_mode=compat: force setuptools (>=64) to emit a legacy
    # .egg-link / easy-install.pth editable install instead of a PEP 660
    # .pth + MetaPathFinder. Static tools (mypy, pyright) walk sys.path and
    # can't follow PEP 660 finders, so test_typing fails with
    # `Module "ray" has no attribute "init"` under strict editables.
    pip install -v -e . -c requirements_compiled.txt \
      --config-settings editable_mode=compat
  )
  (
    # For runtime_env tests, wheels are needed
    cd "${WORKSPACE_DIR}"
    pip wheel -e python -w .whl
  )
}

_validate_macos_wheels_commit_str() {
  if [[ -n "${BUILDKITE_COMMIT}" ]]; then
    EXPECTED_COMMIT="${BUILDKITE_COMMIT:-}"
  else
    EXPECTED_COMMIT="$(git rev-parse HEAD)"
  fi

  if [[ -z "$EXPECTED_COMMIT" ]]; then
    echo "Could not validate expected wheel commits: BUILDKITE_COMMIT is empty." >&2
    exit 1
  fi

  for whl in .whl/*.whl; do
    basename="${whl##*/}"

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

build_macos_wheels_and_jars() {
  if [[ "${OSTYPE}" != darwin* ]]; then
    echo "Not on macOS"
    exit 1
  fi

  _bazel_build_before_install

  # Create wheel output directory and empty contents
  # If buildkite runners are re-used, wheels from previous builds might be here, so we delete them.
  rm -rf .whl
  mkdir -p .whl

  # This command should be kept in sync with ray/python/README-building-wheels.md.
  "${WORKSPACE_DIR}"/python/build-wheel-macos.sh

  mkdir -p /tmp/artifacts
  rm -rf /tmp/artifacts/.whl
  cp -r .whl /tmp/artifacts/.whl
  chmod 755 /tmp/artifacts/.whl
  chmod 644 /tmp/artifacts/.whl/*

  _validate_macos_wheels_commit_str
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
  # Build and install ray into the system.
  # For building the wheel, see build_wheels_and_jars.
  _bazel_build_before_install
  install_ray
}

"$@"

# Pop caller's shell options (quietly)
{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null
