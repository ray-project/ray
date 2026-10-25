import os
import sys
from pathlib import Path

import pytest

# In privileged containers, we expect the following
# cgroupv1 is disabled
# cgroupv2 is enabled and mounted on /sys/fs/cgroup
# the user running tests has read and write access to the cgroup subtree
# memory and cpu controllers are enabled

_MOUNT_FILE_PATH = "/proc/mounts"
_CGROUP2_PATH = "/sys/fs/cgroup"
_CTRL_FILE = "cgroup.controllers"
_EXPECTED_CTRLS = ["memory", "cpu"]


# mount file format:
# cgroup /sys/fs/cgroup cgroup2 rw,nosuid,nodev,noexec,relatime 0 0
def test_only_cgroupv2_mounted_rw():
    found_cgroupv2 = False
    found_cgroupv1 = False
    with open(Path(_MOUNT_FILE_PATH)) as f:
        for line in f:
            c = line.split()
            found_cgroupv2 = found_cgroupv2 or (
                c[2] == "cgroup2" and c[1] == _CGROUP2_PATH and "rw" in c[3]
            )
            found_cgroupv1 = found_cgroupv1 or (c[2] == "cgroup")
    assert found_cgroupv2 and not found_cgroupv1


def test_cgroupv2_rw_for_test_user():
    assert os.access(_CGROUP2_PATH, os.R_OK) and os.access(_CGROUP2_PATH, os.W_OK)


def test_cgroupv2_controllers_enabled():
    with open(os.path.join(_CGROUP2_PATH, _CTRL_FILE)) as f:
        enabled = f.readlines()
        assert len(enabled) == 1
        enabled_ctrls = enabled[0].split()
        for expected_ctrl in _EXPECTED_CTRLS:
            assert (
                expected_ctrl in enabled_ctrls
            ), f"Expected {expected_ctrl} to be enabled for cgroups2, but it is not"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
