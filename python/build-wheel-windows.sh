#!/usr/bin/env bash

set -euxo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"
WORKSPACE_DIR="${ROOT_DIR}/.."

get_python_version() {
  python -s -c "import sys; sys.stdout.write('%s.%s' % sys.version_info[:2])"
}

is_python_version() {
  local expected result=0
  expected="$1"
  case "$(get_python_version).0." in
    "${expected}".*) ;;
    *) result=1;;
  esac
  case "$(pip --version | tr -d "\r")" in
    *" (python ${expected})") ;;
    *) result=1;;
  esac
  return "${result}"
}

refreshenv() {
  # https://gist.github.com/jayvdb/1daf8c60e20d64024f51ec333f5ce806
  powershell -NonInteractive - <<\EOF
Import-Module "$env:ChocolateyInstall\helpers\chocolateyProfile.psm1"

Update-SessionEnvironment

# Print out the list of env vars we're going to export.
# Sometimes the bash source fails, this will help with debugging.
gci env:

# Round brackets in variable names cause problems with bash
Get-ChildItem env:* | %{
  if (!($_.Name.Contains('('))) {
    $value = $_.Value
    if ($_.Name -eq 'PATH') {
      $value = $value -replace ';',':'
    }
    # Use heredocs to wrap values. This fixes problems with environment variables containing single quotes.
    # An environment variable containing the string REFRESHENV_EOF could still cause problems, but is
    # far less likely than a single quote.
    Write-Output ("export " + $_.Name + "=$`(cat <<- 'REFRESHENV_EOF'`n" + $value + "`nREFRESHENV_EOF`)")
  }
} | Out-File -Encoding ascii $env:TEMP\refreshenv.sh

EOF

  source "$TEMP/refreshenv.sh"
}

install_ray() {
  # TODO(mehrdadn): This function should be unified with the one in ci/ci.sh.
  (
    pip install wheel delvewheel


    pushd python/ray/dashboard/client
      choco install nodejs --version=22.4.1 -y
      refreshenv
      # https://stackoverflow.com/questions/69692842/error-message-error0308010cdigital-envelope-routinesunsupported
      export NODE_OPTIONS=--openssl-legacy-provider
      npm install
      npm run build
    popd

    cd "${WORKSPACE_DIR}"/python
    pip install -v -e .
  )
}

uninstall_ray() {
  pip uninstall -y ray

  # Cleanup generated thirdparty files.
  python -c $'import shutil; import sys; \nfor d in sys.argv[1:]: shutil.rmtree(d, ignore_errors=True);' \
    "${ROOT_DIR}/ray/thirdparty_files" \
    "${ROOT_DIR}/ray/_private/runtime_env/agent/thirdparty_files"
}

build_wheel_windows() {
  if [[ "${BUILD_ONE_PYTHON_ONLY:-}" == "" ]]; then
    echo "Please set BUILD_ONE_PYTHON_ONLY . Building all python versions is no longer supported."
    exit 1
  fi

  local ray_uninstall_status=0
  uninstall_ray || ray_uninstall_status=1

  local local_dir="python/dist"
  {
    echo "build --announce_rc";
    echo "build --config=ci";
    echo "startup --output_user_root=c:/raytmp";
    echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}";
  } >> ~/.bazelrc

  if [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
    echo "build --remote_upload_local_results=false" >> ~/.bazelrc
  fi

  local pyversion="${BUILD_ONE_PYTHON_ONLY}"

  git clean -q -f -f -x -d -e "${local_dir}" -e python/ray/dashboard/client
  git checkout -q -f -- .

  # Start a subshell to prevent PATH and cd from affecting our shell environment
  (
    if ! is_python_version "${pyversion}"; then
      conda install -y conda=24.1.2 python="${pyversion}"
    fi
    if ! is_python_version "${pyversion}"; then
      echo "Expected pip for Python ${pyversion} but found Python $(get_python_version) with $(pip --version); exiting..." 1>&2
      exit 1
    fi

    unset PYTHON2_BIN_PATH PYTHON3_BIN_PATH  # make sure these aren't set by some chance
    install_ray
    cd "${WORKSPACE_DIR}"/python
    # Set the commit SHA in _version.py.
    if [ -n "$BUILDKITE_COMMIT" ]; then
      sed -i.bak "s/{{RAY_COMMIT_SHA}}/$BUILDKITE_COMMIT/g" ray/_version.py && rm ray/_version.py.bak
    else
      echo "BUILDKITE_COMMIT variable not set - required to populated ray.__commit__."
      exit 1
    fi
    # build ray wheel
    python -m pip wheel -v -w dist . --no-deps
    # Pack any needed system dlls like msvcp140.dll
    delvewheel repair dist/ray-*.whl
    # build ray-cpp wheel
    RAY_INSTALL_CPP=1 python -m pip wheel -v -w dist . --no-deps
    # No extra dlls are needed, do not call delvewheel
    uninstall_ray
  )

  if [ 0 -eq "${ray_uninstall_status}" ]; then  # If Ray was previously installed, restore it
    install_ray
  fi
}

build_wheel_windows "$@"
