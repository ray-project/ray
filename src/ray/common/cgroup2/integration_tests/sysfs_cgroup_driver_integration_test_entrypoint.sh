#!/usr/bin/env bash
set -euo pipefail

# To run this test locally, you will need to run it as the root user to be able
# to create cgroups, add users etc. It is recommended to first create a cgroup for testing
# so the tests do not interfere with your root cgroup.
#
# 1) Create a cgroup
# sudo mkdir -p /sys/fs/cgroup/testing
#
# 2) Enable rwx permissions for files in the cgroup
# sudo chmod u+rwx /sys/fs/cgroup/testing
#
# 2) Move the current process into the cgroup
# echo $$ | sudo tee /sys/fs/cgroup/testing/cgroup.procs
#
# 3) Execute the tests with sudo passing your ROOT_CGROUP
# NOTE: the "env PATH=${PATH}" is for the root user to find the bazel executable
# since it may not already be in its path.
# sudo env PATH="${PATH}" ./sysfs_cgroup_driver_integration_test_entrypoint.sh /sys/fs/cgroup/testing
#
# If cleanup fails during local testing, you can run to remove all created cgroups.
# sudo find /sys/fs/cgroup/testing -type d -depth 10 -exec rmdir {} +
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "ERROR: Cgroup integration tests can only be run on Linux."
  echo "  The current OS is $(uname)"
  exit 0
fi

BAZEL=$(which bazel)
# Defaults to /sys/fs/cgroup if not passed in as an argument.
ROOT_CGROUP="${1:-/sys/fs/cgroup}"
CURR_USER=$(whoami)

echo "Starting Cgroupv2 Integration Tests as user ${CURR_USER}"
echo "ROOT_CGROUP is ${ROOT_CGROUP}."

if ! grep -qE 'cgroup2\srw' /etc/mtab; then
  echo "Failed because cgroupv2 is not mounted on the system in read-write mode."
  echo "See the following documentation for how to enable cgroupv2 properly:"
  echo "https://kubernetes.io/docs/concepts/architecture/cgroups/#linux-distribution-cgroup-v2-support"
  exit 1
fi
if grep -qE "cgroup\sr" /etc/mtab; then
  echo "Failed because cgroupv2 and cgroupv1 is mounted on this system."
  echo "See the following documentation for how to enable cgroupv2 in properly in unified mode:"
  echo "https://kubernetes.io/docs/concepts/architecture/cgroups/#linux-distribution-cgroup-v2-support"
  exit 1
fi
if [[ ! -w ${ROOT_CGROUP} ]]; then
  echo "$(whoami) needs read and write access to ${ROOT_CGROUP} to run integration tests."
  echo "Run 'sudo chown -R ${CURR_USER} ${ROOT_CGROUP}' to fix this."
  exit 1
fi
if ! grep -qE '\scpu\s' "${ROOT_CGROUP}"/cgroup.controllers; then
  echo "Failed because the cpu controller is not available in the ${ROOT_CGROUP}/cgroup.controllers."
  echo "To enable the cpu controller, you need to add it to the parent cgroup of ${ROOT_CGROUP}."
  echo "See: https://docs.kernel.org/admin-guide/cgroup-v2.html#enabling-and-disabling."
  exit 1
fi
if ! grep -qE '\smemory\s' "${ROOT_CGROUP}"/cgroup.controllers; then
  echo "Failed because the memory controller is not available in the ${ROOT_CGROUP}/cgroup.controllers."
  echo "To enable the memory controller, you need to add it to the parent cgroup of ${ROOT_CGROUP}."
  echo "See: https://docs.kernel.org/admin-guide/cgroup-v2.html#enabling-and-disabling."
  exit 1
fi


TEST_FIXTURE_SCRIPT=src/ray/common/cgroup2/integration_tests/sysfs_cgroup_driver_integration_test_fixture.sh
BASE_CGROUP="$(mktemp -d -p "${ROOT_CGROUP}" testing.XXXXX)"
TEST_CGROUP=${BASE_CGROUP}/test
LEAF_CGROUP=${BASE_CGROUP}/leaf
UNPRIV_USER=cgroup-tester

trap 'echo "ERROR on line ${LINENO}"; cleanup' ERR INT TERM

cleanup() {
  echo "Running teardown because of an error."
  "${TEST_FIXTURE_SCRIPT}" teardown "${ROOT_CGROUP}" "${BASE_CGROUP}" "${UNPRIV_USER}"
}

# The integration tests assume that the ROOT_CGROUP exists and has read and write access.
#
# This test suite will create the following cgroup hierarchy for the tests
# starting with BASE_CGROUP.
#
#                        ROOT_CGROUP
#                             |
#                        BASE_CGROUP
#                       /           \
#                 TEST_CGROUP   LEAF_CGROUP
#
# NOTE: The test suite does not assume that ROOT_CGROUP is an actual ROOT_CGROUP. Therefore,
#   1. setup will migrate all processes from the ROOT_CGROUP -> LEAF_CGROUP
#   2. teardown will migrate all processes from the LEAF_CGROUP -> ROOT_CGROUP
#
# NOTE: BASE_CGROUP will have a randomly generated name to isolate tests from each other.
#
# The test suite assumes that
#   1. cpu, memory controllers are available on ROOT_CGROUP i.e. in the ROOT_CGROUP/cgroup.controllers file.
#   2. All processes inside the base_cgroup can be migrated into the leaf_cgroup to avoid not violating
#   the no internal processes contstraint.
#
# All C++ tests should only have access to the TEST_CGROUP and nothing outside of it.
# The C++ tests will be executed as a non-root user. Setup/teardown will need root permissions.
echo "ROOT_CGROUP is ${ROOT_CGROUP}."
echo "BASE_CGROUP for the test suite is ${BASE_CGROUP}."
echo "TEST_CGROUP for the test suite is ${TEST_CGROUP}."
echo "LEAF_CGROUP for the test suite is ${LEAF_CGROUP}."

"${TEST_FIXTURE_SCRIPT}" setup "${ROOT_CGROUP}" "${BASE_CGROUP}" "${UNPRIV_USER}"

sudo -u "${UNPRIV_USER}" CGROUP_PATH="${TEST_CGROUP}" \
  "${BAZEL}" run //src/ray/common/cgroup2/integration_tests:sysfs_cgroup_driver_integration_test

"${TEST_FIXTURE_SCRIPT}" teardown "${ROOT_CGROUP}" "${BASE_CGROUP}" "${UNPRIV_USER}"
