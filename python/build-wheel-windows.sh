#!/usr/bin/env bash

set -euxo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"
WORKSPACE_DIR="${ROOT_DIR}/.."

PY_VERSIONS=($(python -s -c "import runpy, sys; runpy.run_path(sys.argv.pop(), run_name='__api__')" python_versions "${ROOT_DIR}"/setup.py | tr -d "\r"))
PY_SCRIPT_SUBDIR=Scripts  # 'bin' for UNIX, 'Scripts' for Windows

bazel_preclean() {
  "${WORKSPACE_DIR}"/ci/travis/bazel.py preclean "mnemonic(\"Genrule\", deps(//:*))"
}

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

install_ray() {
  # TODO(mehrdadn): This function should be unified with the one in ci/travis/ci.sh.
  (
    pip install wheel

    cd "${WORKSPACE_DIR}"/python
    "${WORKSPACE_DIR}"/ci/keep_alive pip install -v -e .
  )
}

uninstall_ray() {
  pip uninstall -y ray

  python -s -c "import runpy, sys; runpy.run_path(sys.argv.pop(), run_name='__api__')" clean "${ROOT_DIR}"/setup.py
}

build_wheel_windows() {
  local ray_uninstall_status=0
  uninstall_ray || ray_uninstall_status=1

  local pyversion pyversions=()
  for pyversion in "${PY_VERSIONS[@]}"; do
    if [ "${pyversion}" = "${PYTHON-}" ]; then continue; fi  # we'll build ${PYTHON} last
    pyversions+=("${pyversion}")
  done
  pyversions+=("${PYTHON-}")  # build this last so any subsequent steps use the right version

  local local_dir="python/dist"
  for pyversion in "${pyversions[@]}"; do
    if [ -z "${pyversion}" ]; then continue; fi
    bazel_preclean
    git clean -q -f -f -x -d -e "${local_dir}" -e python/ray/dashboard/client
    git checkout -q -f -- .

    # Start a subshell to prevent PATH and cd from affecting our shell environment
    (
      if ! is_python_version "${pyversion}"; then
        local pydirs=("${RUNNER_TOOL_CACHE}/Python/${pyversion}".*/x64)
        local pydir="${pydirs[-1]}"
        pydir="$(cygpath -u "${pydir}")"  # Translate Windows path
        test -d "${pydir}"
        export PATH="${pydir}:${pydir}/${PY_SCRIPT_SUBDIR}:${PATH}"
      fi
      if ! is_python_version "${pyversion}"; then
        echo "Expected pip for Python ${pyversion} but found Python $(get_python_version) with $(pip --version); exiting..." 1>&2
        exit 1
      fi

      unset PYTHON2_BIN_PATH PYTHON3_BIN_PATH  # make sure these aren't set by some chance
      install_ray
      cd "${WORKSPACE_DIR}"/python
      python setup.py --quiet bdist_wheel
      uninstall_ray
    )
  done

  bazel_preclean
  if [ 0 -eq "${ray_uninstall_status}" ]; then  # If Ray was previously installed, restore it
    install_ray
  fi
}

build_wheel_windows "$@"
