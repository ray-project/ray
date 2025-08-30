#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 <ACTION> <ROOT_CGROUP> <BASE_CGROUP> <UNPRIV_USER>"
    echo "  ACTION       - One of {setup, teardown}."
    echo "  ROOT_CGROUP  - The root cgroup path. Assumes the cgroup already exists."
    echo "  BASE_CGROUP  - The base cgroup path. Assumes the cgroup already exists."
    echo "  UNPRIV_USER  - The name of the unprivileged user. Will create if doesn't exist."
    exit 1
}

ACTION=${1:-}
ROOT_CGROUP=${2:-}
BASE_CGROUP=${3:-}
UNPRIV_USER=${4:-}

validate_args() {
    if [[ -z "$ACTION" || -z "$ROOT_CGROUP" || -z "$BASE_CGROUP" || -z "$UNPRIV_USER" ]]; then
        echo "ERROR: Missing arguments."
        usage
    fi
}

# Helper function to move all processes from the src cgroup
# into the dest cgroup.
move_all_processes() {
  # Errexit is disabled because pids can be transient i.e.
  # you can fail to move a pid that existed when you read the file
  # but exited by the time you tried to move it.
  set +e
  local src="$1" dst="$2"
  local count=0
  while IFS= read -r pid
  do
    if echo "${pid}" > "${dst}" 2>/dev/null; then
      ((count++))
    fi
  done < <(grep -v '^ *#' "${src}")
  echo "Moved ${count} procs from ${src} to ${dst}."
  set -e
}

update_controllers() {
  local CONTROLLER_FILE=$1
  local UPDATE=$2
  if echo "${UPDATE}" > "${CONTROLLER_FILE}"; then
    echo "Updated ${UPDATE} controllers for ${CONTROLLER_FILE}"
  else
    echo "ERROR: Failed to update controllers ${UPDATE} for ${CONTROLLER_FILE}" >&2
    exit 1
  fi

}

# Setup involves the following steps:
#
#   1. Create the LEAF_CGROUP and TEST_CGROUP.
#   2. Move all processes from the ROOT_CGROUP into the LEAF_CGROUP.
#   3. Enable cpu, memory controllers on the ROOT, BASE, and TEST cgroups.
#   4. Create the UNPRIV_USER to run the tests as a non-root user.
#   5. Make UNPRIV_USER owner of the cgroup subtree starting at BASE_CGROUP.
#
# NOTE: The tests need to be run as a separate user because access control
# checks will always pass for the root user so they cannot be tested properly
# without creating an unprivileged user.
setup() {

mkdir -p "${LEAF_CGROUP}"
mkdir -p "${TEST_CGROUP}"

echo "Created LEAF_CGROUP at ${LEAF_CGROUP}."
echo "Created TEST_CGROUP at ${TEST_CGROUP}."

move_all_processes "${ROOT_CGROUP_PROCS}" "${LEAF_CGROUP_PROCS}"

if [[ -s "${ROOT_CGROUP_PROCS}" ]]; then
  echo "ERROR: Failed to move all processes out of ${ROOT_CGROUP_PROCS}."
  echo "  Expected cgroup.procs to be empty, but it's not:"
  cat "${ROOT_CGROUP_PROCS}"
  exit 1
fi

update_controllers "${ROOT_CGROUP}/cgroup.subtree_control" "+cpu +memory"
update_controllers "${BASE_CGROUP}/cgroup.subtree_control" "+cpu +memory"
update_controllers "${TEST_CGROUP}/cgroup.subtree_control" "+cpu +memory"

if ! id -u "${UNPRIV_USER}" >/dev/null 2>&1; then
  sudo useradd -m -s /usr/sbin/nologin "${UNPRIV_USER}"
  echo "Created unprivilged user ${UNPRIV_USER}."
fi

sudo chown -R "${UNPRIV_USER}":"${UNPRIV_USER}" "${BASE_CGROUP}"
sudo chmod -R u+rwx "${BASE_CGROUP}"
echo "${UNPRIV_USER} is the owner the cgroup subtree starting at ${BASE_CGROUP}"

}

# Cleanup is the reverse of setup
#   1) Delete the user we created.
#   2) Disable controllers throughout heirarchy.
#   3) Migrate all processes back into the ROOT_CGROUP.
#   4) Recursively delete all created subcgroups.
#
# This is best effort. There can be leaks. The recommended thing
# to do is to run these tests inside a container.
# Setup involves the following steps:
#
#   1. Delete the UNPRIV_USER.
#   2. Disable cpu, memory controllers on the ROOT, BASE, and TEST cgroups.
#   3. Move all processes from the LEAF_CGROUP into the ROOT_CGROUP.
#   4. Delete the TEST, LEAF, and BASE cgroups in that order.
#
# NOTE: This assumes that all C++ tests will clean up their own cgroups.
# If they do not, teardown will fail.
teardown() {

# Delete the user we created
if id -u "${UNPRIV_USER}" >/dev/null 2>&1; then
  pkill -KILL -u "${UNPRIV_USER}" 2>/dev/null || true
  deluser -f "${UNPRIV_USER}" --remove-home 2>/dev/null || true
  echo "Deleted unprivilged user ${UNPRIV_USER}."
fi

update_controllers "${TEST_CGROUP}/cgroup.subtree_control" "-cpu -memory"
update_controllers "${BASE_CGROUP}/cgroup.subtree_control" "-cpu -memory"
update_controllers "${ROOT_CGROUP}/cgroup.subtree_control" "-cpu -memory"

move_all_processes "${LEAF_CGROUP_PROCS}" "${ROOT_CGROUP_PROCS}"

rmdir "${TEST_CGROUP}"
echo "Deleted ${TEST_CGROUP}"
rmdir "${LEAF_CGROUP}"
echo "Deleted ${LEAF_CGROUP}"
rmdir "${BASE_CGROUP}"
echo "Deleted ${BASE_CGROUP}"

echo "Teardown successful."

}

validate_args

LEAF_CGROUP="${BASE_CGROUP}/leaf"
TEST_CGROUP="${BASE_CGROUP}/test"
ROOT_CGROUP_PROCS="${ROOT_CGROUP}/cgroup.procs"
LEAF_CGROUP_PROCS="${LEAF_CGROUP}/cgroup.procs"

echo "Starting integration test fixture with:"
echo "  ACTION=${ACTION}"
echo "  ROOT_CGROUP=${ROOT_CGROUP}"
echo "  BASE_CGROUP=${BASE_CGROUP}"
echo "  TEST_CGROUP=${TEST_CGROUP}"
echo "  UNPRIV_USER=${UNPRIV_USER}"

SETUP_ACTION=setup
TEARDOWN_ACTION=teardown

if [[ "${ACTION}" == "${SETUP_ACTION}" ]]; then
  echo "Running ACTION: ${SETUP_ACTION}"
  setup
elif [[ "${ACTION}" == "${TEARDOWN_ACTION}" ]]; then
  echo "Running ACTION: ${TEARDOWN_ACTION}"
  teardown
else
  echo "[ERROR]: Unknown action ${ACTION}."
  usage
fi
