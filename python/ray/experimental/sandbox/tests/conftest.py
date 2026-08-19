import os
import platform
import shutil
import sys
import tempfile
import urllib.request
from pathlib import Path
from typing import Optional, Tuple

import pytest

from ray._common.utils import get_random_alphanumeric_string
from ray._private.test_utils import sandbox_test_enabled

_MOUNT_FILE_PATH = "/proc/mounts"
_ROOT_CGROUP = Path("/sys/fs/cgroup")
_ARTIFACT_DIR = Path("/artifact-mount")
_STATUS_FILE = _ARTIFACT_DIR / "sandbox-cgroup-status.txt"


def _artifact_status_path() -> Optional[Path]:
    if _ARTIFACT_DIR.is_dir() and os.access(_ARTIFACT_DIR, os.W_OK):
        return _STATUS_FILE
    return None


def _write_artifact_status(lines: list[str], *, append: bool = False) -> None:
    """Write sandbox cgroup diagnostics for Buildkite artifact upload."""
    path = _artifact_status_path()
    if path is None:
        return
    mode = "a" if append else "w"
    with open(path, mode, encoding="utf-8") as status_file:
        for line in lines:
            status_file.write(line.rstrip() + "\n")
        status_file.flush()


def _write_artifact_section(lines: list[str]) -> None:
    path = _artifact_status_path()
    if path is None:
        return
    section = [f"=== {Path(sys.argv[0]).stem} ===", *lines]
    _write_artifact_status(section, append=path.exists())


def _read_subtree_control(path: Path) -> str:
    subtree = path / "cgroup.subtree_control"
    if not subtree.exists():
        return ""
    return subtree.read_text(encoding="utf-8").strip()


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
    bool, Optional[Path], Optional[Path], Optional[Path], str
]:
    """Mirror resource isolation integration setup for a nestable cgroup domain.

    Creates:
                        ROOT_CGROUP
                             |
                        BASE_CGROUP
                       /           \\
                 TEST_CGROUP   LEAF_CGROUP

    Moves processes ROOT -> LEAF, then enables cpu/memory on ROOT/BASE/TEST.
    Returns (success, base, test, leaf, status_message).
    """
    if platform.system() != "Linux":
        return False, None, None, None, "not Linux"
    if not _ROOT_CGROUP.is_dir() or not os.access(_ROOT_CGROUP, os.W_OK):
        return False, None, None, None, f"{_ROOT_CGROUP} is not writable"

    try:
        with open(_MOUNT_FILE_PATH, "r") as mount_file:
            lines = mount_file.readlines()
        found_cgroup_v1 = any("cgroup r" in line.strip() for line in lines)
        found_cgroup_v2 = any("cgroup2 rw" in line.strip() for line in lines)
        if not found_cgroup_v2 or found_cgroup_v1:
            return False, None, None, None, "need cgroup v2 unified rw mount"

        with open(_ROOT_CGROUP / "cgroup.controllers", "r") as controllers_file:
            available = set(controllers_file.readline().strip().split())
        if not {"cpu", "memory"}.issubset(available):
            return (
                False,
                None,
                None,
                None,
                "cpu/memory controllers unavailable in "
                f"{_ROOT_CGROUP}/cgroup.controllers (have {sorted(available)})",
            )

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

        return True, base, test, leaf, "enabled"
    except Exception as e:
        return False, None, None, None, f"cgroup nesting setup failed: {e}"


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


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if not sandbox_test_enabled() or call.when != "call":
        return
    if "cgroup_limits" not in item.nodeid:
        return
    if report.passed:
        _write_artifact_status(["cgroup_limits_probe=passed"], append=True)
    elif report.skipped:
        _write_artifact_status(["cgroup_limits_probe=skipped"], append=True)
    elif report.failed:
        _write_artifact_status(["cgroup_limits_probe=failed"], append=True)


def pytest_sessionfinish(session, exitstatus):
    if not sandbox_test_enabled():
        return
    _write_artifact_status([f"pytest_exitstatus={exitstatus}"], append=True)


@pytest.fixture(scope="session", autouse=True)
def configure_cgroups_and_runsc():
    """Install runsc and enable nestable cgroups like resource isolation tests.

    Privileged CI containers are a cgroup v2 leaf. We use the same Python setup
    as test_resource_isolation_integration.py so runsc can create child cgroups
    without --ignore-cgroups.
    """
    if not sandbox_test_enabled():
        return

    using_cgroups, base, test, leaf, status_message = _try_enable_cgroup_nesting()
    if not using_cgroups:
        _write_artifact_section(
            [
                "cgroup_nesting=failed",
                f"reason={status_message}",
            ]
        )
        pytest.skip(
            "Sandbox tests require nestable cgroup v2 (privileged Linux CI container): "
            f"{status_message}"
        )

    _write_artifact_section(
        [
            "cgroup_nesting=enabled",
            f"cgroup_base={base}",
            f"root_subtree_control={_read_subtree_control(_ROOT_CGROUP)}",
            f"base_subtree_control={_read_subtree_control(base)}",
        ]
    )
    print("Sandbox tests: cgroup nesting enabled")

    runsc_source = "preinstalled"
    if not shutil.which("runsc"):
        runsc_source = "downloaded"
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
            _write_artifact_status(
                [
                    "runsc_install=failed",
                    f"runsc_error={e}",
                ],
                append=True,
            )
            pytest.skip(f"Failed to install runsc for sandbox tests: {e}")

    _write_artifact_status(
        [
            f"runsc={shutil.which('runsc')}",
            f"runsc_source={runsc_source}",
        ],
        append=True,
    )

    yield

    if using_cgroups:
        _cleanup_cgroup_nesting(base, test, leaf)
