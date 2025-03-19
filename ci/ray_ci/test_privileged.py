import os
import pytest
import sys
import time


from pathlib import Path

# mount file format:
# cgroup2 /sys/fs/cgroup cgroup2 rw,nosuid,nodev,noexec,relatime 0 0

MOUNT_FILE_PATH = "/proc/mounts"
CGROUP2_PATH = "/sys/fs/cgroup"
CTRL_FILE = "cgroup.controllers"
EXPECTED_CTRLS = ["memory", "cpu"]


def test_only_cgroupv2_mounted_rw():
    found_cgroupv2 = False
    found_cgroupv1 = False
    with open(Path(MOUNT_FILE_PATH)) as f:
        for line in f:
            c = line.split()
            found_cgroupv2 = found_cgroupv2 or (
                "cgroup2" and c[1] == CGROUP2_PATH and "rw" in c[3]
            )
            found_cgroupv1 = found_cgroupv1 or (c[0] == "cgroup")
    time.sleep(3600)
    assert found_cgroupv2 and not found_cgroupv1


def test_cgroupv2_rw_for_test_user():
    assert os.access(CGROUP2_PATH, os.R_OK) and os.access(CGROUP2_PATH, os.W_OK)


def test_cgroupv2_controllers_enabled():
    with open(os.path.join(CGROUP2_PATH, CTRL_FILE)) as f:
        enabled = f.readlines()
        assert len(enabled) == 1
        enabled_ctrls = enabled[0].split()
        for expected_ctrl in EXPECTED_CTRLS:
            assert (
                expected_ctrl in enabled_ctrls
            ), f"Expected {expected_ctrl} to be enabled for cgroups2, but it is not"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
