import os
import platform
import shutil
import tempfile
import urllib.request
from pathlib import Path
from typing import Optional, Tuple

import pytest

from ray._common.utils import get_random_alphanumeric_string
from ray._private.test_utils import sandbox_test_enabled

_MOUNT_FILE_PATH = "/proc/mounts"
_ROOT_CGROUP = Path("/sys/fs/cgroup")


def _move_procs(src: Path, dst: Path) -> None:
    """Move PIDs listed in src/cgroup.procs into dst/cgroup.procs."""
    with open(src / "cgroup.procs", "r") as src_file, open(
        dst / "cgroup.procs", "w"
    ) as dst_file:
        for line in src_file.readlines():
            pid = line.strip()
            if not pid:
                continue
            try:
                dst_file.write(pid)
                dst_file.flush()
            except OSError:
                # PID may have exited between read and write.
                pass


def _try_enable_cgroup_nesting() -> Tuple[
    bool, Optional[Path], Optional[Path], Optional[Path]
]:
    """Mirror resource isolation integration setup for a nestable cgroup domain.

    Creates:
                        ROOT_CGROUP
                             |
                        BASE_CGROUP
                       /           \\
                 TEST_CGROUP   LEAF_CGROUP

    Moves processes ROOT -> LEAF, then enables cpu/memory on ROOT/BASE/TEST.
    Returns (success, base, test, leaf).
    """
    if platform.system() != "Linux":
        return False, None, None, None
    if not _ROOT_CGROUP.is_dir() or not os.access(_ROOT_CGROUP, os.W_OK):
        print(f"Sandbox tests: {_ROOT_CGROUP} is not writable")
        return False, None, None, None

    try:
        with open(_MOUNT_FILE_PATH, "r") as mount_file:
            lines = mount_file.readlines()
        found_cgroup_v1 = any("cgroup r" in line.strip() for line in lines)
        found_cgroup_v2 = any("cgroup2 rw" in line.strip() for line in lines)
        if not found_cgroup_v2 or found_cgroup_v1:
            print("Sandbox tests: need cgroup v2 unified rw mount")
            return False, None, None, None

        with open(_ROOT_CGROUP / "cgroup.controllers", "r") as controllers_file:
            available = set(controllers_file.readline().strip().split())
        if not {"cpu", "memory"}.issubset(available):
            print(
                "Sandbox tests: cpu/memory controllers unavailable in "
                f"{_ROOT_CGROUP}/cgroup.controllers (have {sorted(available)})"
            )
            return False, None, None, None

        base = _ROOT_CGROUP / ("testing_" + get_random_alphanumeric_string(5))
        test = base / "test"
        leaf = base / "leaf"
        os.mkdir(base)
        os.mkdir(test)
        os.mkdir(leaf)

        _move_procs(_ROOT_CGROUP, leaf)

        for path in (_ROOT_CGROUP, base, test):
            with open(path / "cgroup.subtree_control", "w") as subtree:
                subtree.write("+cpu +memory")
                subtree.flush()

        return True, base, test, leaf
    except Exception as e:
        print(f"Sandbox tests: cgroup nesting setup failed: {e}")
        return False, None, None, None


def _cleanup_cgroup_nesting(
    base: Optional[Path], test: Optional[Path], leaf: Optional[Path]
) -> None:
    """Best-effort teardown matching resource isolation cleanup order."""
    if base is None or test is None or leaf is None:
        return
    try:
        for path in (test, base, _ROOT_CGROUP):
            subtree = path / "cgroup.subtree_control"
            if subtree.exists():
                with open(subtree, "w") as subtree_file:
                    subtree_file.write("-cpu -memory")
                    subtree_file.flush()
        if leaf.exists():
            _move_procs(leaf, _ROOT_CGROUP)
        for path in (test, leaf, base):
            if path.exists():
                path.rmdir()
    except Exception as e:
        print(f"Sandbox tests: cgroup nesting teardown failed: {e}")


def pytest_runtest_setup(item):
    if not sandbox_test_enabled():
        pytest.skip("Sandbox tests are only run when TEST_SANDBOX=1")


@pytest.fixture(scope="session", autouse=True)
def configure_cgroups_and_runsc():
    """Install runsc and enable nestable cgroups like resource isolation tests.

    Privileged CI containers are a cgroup v2 leaf. We use the same Python setup
    as test_resource_isolation_integration.py so runsc can create child cgroups
    without --ignore-cgroups. Falls back to RAY_SANDBOX_IGNORE_CGROUPS=1 when
    setup is unavailable.
    """
    if not sandbox_test_enabled():
        return

    using_cgroups, base, test, leaf = _try_enable_cgroup_nesting()
    if using_cgroups:
        os.environ.pop("RAY_SANDBOX_IGNORE_CGROUPS", None)
        print(
            "Sandbox tests: cgroup nesting enabled "
            "(RAY_SANDBOX_IGNORE_CGROUPS unset)"
        )
    else:
        os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = "1"
        print(
            "Sandbox tests: using RAY_SANDBOX_IGNORE_CGROUPS=1 "
            "(cgroup nesting unavailable)"
        )

    if not shutil.which("runsc"):
        temp_bin = tempfile.mkdtemp()
        os.chmod(temp_bin, 0o755)
        runsc_path = os.path.join(temp_bin, "runsc")
        arch = (
            "aarch64"
            if platform.machine().lower() in ("aarch64", "arm64")
            else "x86_64"
        )
        url = (
            "https://storage.googleapis.com/gvisor/releases/release/latest/"
            f"{arch}/runsc"
        )
        try:
            urllib.request.urlretrieve(url, runsc_path)
            os.chmod(runsc_path, 0o755)
            os.environ["PATH"] = f"{temp_bin}:{os.environ.get('PATH', '')}"
        except Exception as e:
            pytest.skip(f"Failed to install runsc for sandbox tests: {e}")

    yield

    if using_cgroups:
        _cleanup_cgroup_nesting(base, test, leaf)
