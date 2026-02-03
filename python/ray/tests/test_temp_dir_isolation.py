"""
Tests for running multiple Ray clusters on the same node using --temp-dir isolation.

This test verifies that multiple Ray clusters can run independently on the same
node when started with different --temp-dir values, and can be stopped selectively.

Usage Example:
--------------
# Start two isolated Ray clusters (each with 1 head + 1 worker)
ray start --head --temp-dir /tmp/ray_cluster_1 --port 6380
ray start --address=127.0.0.1:6380 --temp-dir /tmp/ray_cluster_1

ray start --head --temp-dir /tmp/ray_cluster_2 --port 6381
ray start --address=127.0.0.1:6381 --temp-dir /tmp/ray_cluster_2

# Stop only cluster 1
ray stop --temp-dir /tmp/ray_cluster_1
"""

import os
import shutil
import subprocess
import uuid
from typing import Dict, List, Tuple

import pytest
from click.testing import CliRunner

import ray.scripts.scripts as scripts
from ray._common.test_utils import wait_for_condition
from ray.autoscaler._private.constants import RAY_PROCESSES

import psutil


def _get_process_info(proc: psutil.Process) -> Tuple[int, str, List[str]]:
    """Get process info: (pid, name, cmdline_list)."""
    try:
        return (proc.pid, proc.name(), proc.cmdline() or [])
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def _is_ray_process(proc_name: str, proc_cmdline: List[str]) -> bool:
    """
    Check if a process is a Ray process using the same logic as ray stop.
    Uses RAY_PROCESSES from ray.autoscaler._private.constants.
    """
    cmdline_str = subprocess.list2cmdline(proc_cmdline)

    for keyword, filter_by_cmd in RAY_PROCESSES:
        if filter_by_cmd:
            # Filter by command name (first 15 chars on Linux)
            if keyword in proc_name:
                return True
        else:
            # Filter by full command line args
            if keyword in cmdline_str:
                return True
    return False


def _get_all_ray_processes() -> List[Dict]:
    """
    Get all Ray-related processes currently running.
    Uses the same process identification logic as ray stop.
    Returns list of dicts with pid, name, cmdline, temp_dir.
    """
    ray_procs = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        info = _get_process_info(proc)
        if info is None:
            continue
        pid, name, cmdline_list = info

        # Check if this is a Ray process using the same logic as ray stop
        if not _is_ray_process(name, cmdline_list):
            continue

        cmdline_str = subprocess.list2cmdline(cmdline_list)

        # Extract temp_dir if present
        temp_dir = None
        for pattern in ["--temp-dir=", "--temp_dir="]:
            if pattern in cmdline_str:
                start = cmdline_str.find(pattern) + len(pattern)
                end = cmdline_str.find(" ", start)
                temp_dir = cmdline_str[start:end] if end != -1 else cmdline_str[start:]
                break

        ray_procs.append(
            {
                "pid": pid,
                "name": name,
                "cmdline": cmdline_str,
                "temp_dir": temp_dir,
            }
        )
    return ray_procs


def _get_pids_by_temp_dir(temp_dir: str) -> List[Dict]:
    """
    Get Ray processes that have temp_dir in their command line.
    Uses the same process identification logic as ray stop.
    """
    procs = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        info = _get_process_info(proc)
        if info is None:
            continue
        pid, name, cmdline_list = info

        # Only consider Ray processes
        if not _is_ray_process(name, cmdline_list):
            continue

        # Use exact path matching for cross-platform compatibility
        # Reuse the same function from scripts.py
        if scripts.matches_temp_dir_path(cmdline_list, temp_dir):
            cmdline_str = subprocess.list2cmdline(cmdline_list)
            procs.append(
                {
                    "pid": pid,
                    "name": name,
                    "cmdline": cmdline_str,
                }
            )
    return procs


def _format_process_list(procs: List[Dict], header: str) -> str:
    """Format process list for readable output."""
    lines = [f"\n{'='*60}", header, "=" * 60]
    if not procs:
        lines.append("  (none)")
    else:
        for p in procs:
            # Truncate cmdline for readability
            cmdline = (
                p["cmdline"][:100] + "..." if len(p["cmdline"]) > 100 else p["cmdline"]
            )
            lines.append(f"  PID={p['pid']:6} | {p['name']:20} | {cmdline}")
    lines.append("")
    return "\n".join(lines)


def _all_pids_stopped(procs: List[Dict]) -> bool:
    """Check if all processes in the list are stopped."""
    for p in procs:
        if psutil.pid_exists(p["pid"]):
            return False
    return True


def _all_pids_running(procs: List[Dict]) -> bool:
    """Check if all processes in the list are still running."""
    for p in procs:
        if not psutil.pid_exists(p["pid"]):
            return False
    return True


def _die_on_error(result):
    """Print output and raise if command failed."""
    if result.exit_code != 0:
        print(f"Command failed with exit code {result.exit_code}")
        print(f"Output: {result.output}")
        if result.exception:
            raise result.exception
        raise AssertionError(f"Command failed: {result.output}")


@pytest.fixture
def cleanup_ray():
    """Fixture to ensure Ray is stopped before and after tests."""
    runner = CliRunner(env={"RAY_USAGE_STATS_PROMPT_ENABLED": "0"})
    try:
        runner.invoke(scripts.stop, ["--force"])
    except Exception:
        pass
    yield runner
    try:
        runner.invoke(scripts.stop, ["--force"])
    except Exception:
        pass


class TestTempDirIsolation:
    """Test running multiple Ray clusters with different --temp-dir values."""

    def test_two_clusters_independent_stop(self, cleanup_ray):
        """
        Test that two clusters (each with 1 head + 1 worker) can be stopped independently.

        Scenario:
        - Start cluster 1: 1 head + 1 worker with --temp-dir /tmp/ray_cluster_1
        - Start cluster 2: 1 head + 1 worker with --temp-dir /tmp/ray_cluster_2
        - Stop cluster 1 using ray stop --temp-dir
        - Verify: total_ray_processes - cluster1 - cluster2 = 0 (no orphans)
        - Verify cluster 2 is still running
        - Stop cluster 2
        - Verify all processes are stopped
        """
        runner = cleanup_ray
        suffix = uuid.uuid4().hex[:8]
        temp_dir_1 = f"/tmp/ray_cluster1_{suffix}"
        temp_dir_2 = f"/tmp/ray_cluster2_{suffix}"

        try:
            # === Start Cluster 1 (head + worker) ===
            result = runner.invoke(
                scripts.start,
                ["--head", f"--temp-dir={temp_dir_1}", "--port", "6380"],
            )
            _die_on_error(result)

            # Wait for cluster 1 head to be ready
            wait_for_condition(
                lambda: os.path.isdir(os.path.join(temp_dir_1, "session_latest")),
                timeout=30,
            )

            result = runner.invoke(
                scripts.start,
                [f"--temp-dir={temp_dir_1}", "--address=127.0.0.1:6380"],
            )
            _die_on_error(result)

            # Wait for cluster 1 to have processes
            wait_for_condition(
                lambda: len(_get_pids_by_temp_dir(temp_dir_1)) >= 2,
                timeout=30,
            )

            # === Start Cluster 2 (head + worker) ===
            result = runner.invoke(
                scripts.start,
                ["--head", f"--temp-dir={temp_dir_2}", "--port", "6381"],
            )
            _die_on_error(result)

            # Wait for cluster 2 head to be ready
            wait_for_condition(
                lambda: os.path.isdir(os.path.join(temp_dir_2, "session_latest")),
                timeout=30,
            )

            result = runner.invoke(
                scripts.start,
                [f"--temp-dir={temp_dir_2}", "--address=127.0.0.1:6381"],
            )
            _die_on_error(result)

            # Wait for cluster 2 to have processes
            wait_for_condition(
                lambda: len(_get_pids_by_temp_dir(temp_dir_2)) >= 2,
                timeout=30,
            )

            # === Verify process isolation before stopping ===
            # Get all Ray processes and categorize them
            all_ray_procs = _get_all_ray_processes()
            cluster1_procs = _get_pids_by_temp_dir(temp_dir_1)
            cluster2_procs = _get_pids_by_temp_dir(temp_dir_2)

            cluster1_pids = {p["pid"] for p in cluster1_procs}
            cluster2_pids = {p["pid"] for p in cluster2_procs}
            all_ray_pids = {p["pid"] for p in all_ray_procs}

            # Calculate orphan processes (Ray processes not belonging to either cluster)
            orphan_pids = all_ray_pids - cluster1_pids - cluster2_pids
            orphan_procs = [p for p in all_ray_procs if p["pid"] in orphan_pids]

            # Print detailed process information
            print(
                _format_process_list(
                    all_ray_procs, f"ALL RAY PROCESSES (total={len(all_ray_procs)})"
                )
            )
            print(
                _format_process_list(
                    cluster1_procs,
                    f"CLUSTER 1 PROCESSES (temp_dir={temp_dir_1}, count={len(cluster1_procs)})",
                )
            )
            print(
                _format_process_list(
                    cluster2_procs,
                    f"CLUSTER 2 PROCESSES (temp_dir={temp_dir_2}, count={len(cluster2_procs)})",
                )
            )
            print(
                _format_process_list(
                    orphan_procs,
                    f"ORPHAN PROCESSES (no temp_dir, count={len(orphan_procs)})",
                )
            )

            # Verify: total - cluster1 - cluster2 = 0
            print(f"\n{'='*60}")
            print("PROCESS ISOLATION CHECK")
            print("=" * 60)
            print(f"  Total Ray processes:    {len(all_ray_procs)}")
            print(f"  Cluster 1 processes:  - {len(cluster1_procs)}")
            print(f"  Cluster 2 processes:  - {len(cluster2_procs)}")
            print(f"  Orphan processes:     = {len(orphan_procs)}")
            print("")

            assert len(orphan_procs) == 0, (
                f"Found {len(orphan_procs)} orphan Ray processes without --temp-dir:\n"
                + _format_process_list(orphan_procs, "ORPHAN PROCESSES")
            )

            assert (
                len(cluster1_procs) >= 2
            ), f"Cluster 1 should have >= 2 processes, got {len(cluster1_procs)}"
            assert (
                len(cluster2_procs) >= 2
            ), f"Cluster 2 should have >= 2 processes, got {len(cluster2_procs)}"

            # === Stop Cluster 1 only ===
            result = runner.invoke(scripts.stop, [f"--temp-dir={temp_dir_1}"])
            _die_on_error(result)

            # Wait for cluster 1 to stop
            wait_for_condition(
                lambda: _all_pids_stopped(cluster1_procs),
                timeout=30,
            )

            # === Verify after stopping cluster 1 ===
            remaining_ray_procs = _get_all_ray_processes()
            remaining_cluster1 = _get_pids_by_temp_dir(temp_dir_1)
            remaining_cluster2 = _get_pids_by_temp_dir(temp_dir_2)

            print(
                _format_process_list(
                    remaining_ray_procs,
                    f"AFTER STOPPING CLUSTER 1 - ALL RAY PROCESSES (total={len(remaining_ray_procs)})",
                )
            )
            print(
                _format_process_list(
                    remaining_cluster1,
                    f"AFTER STOPPING CLUSTER 1 - CLUSTER 1 PROCESSES (should be 0, got={len(remaining_cluster1)})",
                )
            )
            print(
                _format_process_list(
                    remaining_cluster2,
                    f"AFTER STOPPING CLUSTER 1 - CLUSTER 2 PROCESSES (count={len(remaining_cluster2)})",
                )
            )

            # Cluster 1 should have no remaining processes
            assert len(remaining_cluster1) == 0, (
                f"Cluster 1 should have 0 processes after stop, got {len(remaining_cluster1)}:\n"
                + _format_process_list(remaining_cluster1, "LEAKED CLUSTER 1 PROCESSES")
            )

            # Cluster 2 should still be running
            assert _all_pids_running(
                cluster2_procs
            ), "Cluster 2 should still be running after stopping cluster 1"

            # Verify no orphan processes created
            remaining_ray_pids = {p["pid"] for p in remaining_ray_procs}
            remaining_cluster2_pids = {p["pid"] for p in remaining_cluster2}
            orphan_after_stop = remaining_ray_pids - remaining_cluster2_pids

            assert (
                len(orphan_after_stop) == 0
            ), f"Found {len(orphan_after_stop)} orphan processes after stopping cluster 1"

            # === Stop Cluster 2 ===
            result = runner.invoke(scripts.stop, [f"--temp-dir={temp_dir_2}"])
            _die_on_error(result)

            # Wait for cluster 2 to stop
            wait_for_condition(
                lambda: _all_pids_stopped(cluster2_procs),
                timeout=30,
            )

            # === Final verification: all processes should be stopped ===
            final_ray_procs = _get_all_ray_processes()
            print(
                _format_process_list(
                    final_ray_procs,
                    f"FINAL STATE - ALL RAY PROCESSES (should be 0, got={len(final_ray_procs)})",
                )
            )

            assert len(final_ray_procs) == 0, (
                f"All Ray processes should be stopped, but found {len(final_ray_procs)}:\n"
                + _format_process_list(final_ray_procs, "LEAKED PROCESSES")
            )

        finally:
            runner.invoke(scripts.stop, ["--force"])
            shutil.rmtree(temp_dir_1, ignore_errors=True)
            shutil.rmtree(temp_dir_2, ignore_errors=True)

    def test_stop_nonexistent_temp_dir_does_nothing(self, cleanup_ray):
        """
        Test that ray stop --temp-dir with nonexistent path does nothing.
        """
        runner = cleanup_ray
        suffix = uuid.uuid4().hex[:8]
        nonexistent_temp_dir = f"/tmp/ray_nonexistent_{suffix}"
        real_temp_dir = f"/tmp/ray_real_{suffix}"

        try:
            # Start a cluster (head + worker)
            result = runner.invoke(
                scripts.start,
                ["--head", f"--temp-dir={real_temp_dir}", "--port", "6380"],
            )
            _die_on_error(result)

            # Wait for head to be ready
            wait_for_condition(
                lambda: os.path.isdir(os.path.join(real_temp_dir, "session_latest")),
                timeout=30,
            )

            result = runner.invoke(
                scripts.start,
                [f"--temp-dir={real_temp_dir}", "--address=127.0.0.1:6380"],
            )
            _die_on_error(result)

            # Wait for cluster to have processes
            wait_for_condition(
                lambda: len(_get_pids_by_temp_dir(real_temp_dir)) >= 2,
                timeout=30,
            )

            cluster_procs = _get_pids_by_temp_dir(real_temp_dir)

            # Print process details
            print(
                _format_process_list(
                    cluster_procs, f"CLUSTER PROCESSES (temp_dir={real_temp_dir})"
                )
            )

            assert len(cluster_procs) >= 2, "Cluster should have >= 2 processes"

            # Try to stop with nonexistent temp_dir - should do nothing
            result = runner.invoke(
                scripts.stop,
                [f"--temp-dir={nonexistent_temp_dir}"],
            )
            _die_on_error(result)

            # Real cluster should still be running
            assert _all_pids_running(
                cluster_procs
            ), "Cluster should still be running after stopping nonexistent temp_dir"

            # Verify all original processes still exist
            remaining_procs = _get_pids_by_temp_dir(real_temp_dir)
            print(
                _format_process_list(
                    remaining_procs,
                    "AFTER STOP NONEXISTENT - CLUSTER PROCESSES (should be same count)",
                )
            )

            assert len(remaining_procs) == len(
                cluster_procs
            ), f"Process count changed unexpectedly: was {len(cluster_procs)}, now {len(remaining_procs)}"

        finally:
            runner.invoke(scripts.stop, ["--force"])
            shutil.rmtree(real_temp_dir, ignore_errors=True)

    def test_prefix_path_isolation(self, cleanup_ray):
        """
        Test that /tmp/ray_X and /tmp/ray_X/subdir are correctly isolated.
        ray stop --temp-dir=/tmp/ray_X should NOT stop /tmp/ray_X/subdir cluster.
        """
        runner = cleanup_ray
        suffix = uuid.uuid4().hex[:8]
        temp_dir_parent = f"/tmp/ray_prefix_{suffix}"
        temp_dir_child = f"/tmp/ray_prefix_{suffix}/subdir"

        try:
            os.makedirs(temp_dir_child, exist_ok=True)

            # Start cluster 1 (parent path)
            result = runner.invoke(
                scripts.start,
                ["--head", f"--temp-dir={temp_dir_parent}", "--port", "6382"],
            )
            _die_on_error(result)
            wait_for_condition(
                lambda: len(_get_pids_by_temp_dir(temp_dir_parent)) >= 1,
                timeout=30,
            )

            # Start cluster 2 (child path)
            result = runner.invoke(
                scripts.start,
                ["--head", f"--temp-dir={temp_dir_child}", "--port", "6383"],
            )
            _die_on_error(result)
            wait_for_condition(
                lambda: len(_get_pids_by_temp_dir(temp_dir_child)) >= 1,
                timeout=30,
            )

            cluster1_procs = _get_pids_by_temp_dir(temp_dir_parent)
            cluster2_procs = _get_pids_by_temp_dir(temp_dir_child)

            # Verify no overlap (exact matching works)
            cluster1_pids = {p["pid"] for p in cluster1_procs}
            cluster2_pids = {p["pid"] for p in cluster2_procs}
            assert (
                len(cluster1_pids & cluster2_pids) == 0
            ), "Clusters should not overlap"

            # Stop parent - child should NOT be affected
            runner.invoke(scripts.stop, [f"--temp-dir={temp_dir_parent}"])
            wait_for_condition(lambda: _all_pids_stopped(cluster1_procs), timeout=30)

            assert _all_pids_running(
                cluster2_procs
            ), "Child cluster should still run after stopping parent cluster"

        finally:
            runner.invoke(scripts.stop, ["--force"])
            shutil.rmtree(temp_dir_parent, ignore_errors=True)


class TestMatchesTempDirPath:
    """Unit tests for the matches_temp_dir_path function."""

    def test_relative_path_matches_absolute_path(self):
        """
        Test that relative paths are converted to absolute paths for matching.

        This tests the fix for the bug where:
        - ray stop --temp-dir=./ray_temp
        - os.path.exists("./ray_temp") passes
        - But matching fails because "./ray_temp" != "/home/user/ray_temp"
        """
        import tempfile

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_base:
            # Create a subdirectory
            subdir_name = "ray_temp_test"
            subdir_path = os.path.join(temp_base, subdir_name)
            os.makedirs(subdir_path)

            # Get the absolute path
            abs_path = os.path.abspath(subdir_path)

            # Simulate cmdline with absolute path (as Ray processes have)
            cmdline_with_abs = [
                "python",
                "-u",
                "some_script.py",
                f"--temp-dir={abs_path}",
            ]

            # Test 1: Absolute path should match absolute path
            assert scripts.matches_temp_dir_path(
                cmdline_with_abs, abs_path
            ), "Absolute path should match itself"

            # Test 2: Change to temp_base and use relative path
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_base)
                relative_path = f"./{subdir_name}"

                # Relative path should match the cmdline with absolute path
                assert scripts.matches_temp_dir_path(
                    cmdline_with_abs, relative_path
                ), f"Relative path '{relative_path}' should match absolute path '{abs_path}'"

                # Test with just the directory name (no ./)
                assert scripts.matches_temp_dir_path(
                    cmdline_with_abs, subdir_name
                ), f"Relative path '{subdir_name}' should match absolute path '{abs_path}'"

            finally:
                os.chdir(original_cwd)

    def test_path_with_spaces_in_setproctitle_format(self):
        """
        Test that paths containing spaces are correctly parsed in setproctitle format.

        This tests the fix for the bug where:
        - setproctitle: "ray::DashboardAgent --temp-dir=/tmp/ray dir/session"
        - Old code: path_part.split()[0] → "/tmp/ray" (wrong!)
        - Fixed code: takes everything after prefix → "/tmp/ray dir/session" (correct)
        """
        import tempfile

        # Create a directory with spaces in the name
        with tempfile.TemporaryDirectory() as temp_base:
            dir_with_spaces = os.path.join(temp_base, "ray temp dir")
            os.makedirs(dir_with_spaces)

            abs_path = os.path.abspath(dir_with_spaces)

            # Test 1: Normal cmdline format (--temp-dir=path as separate argument)
            cmdline_normal = [
                "python",
                "-u",
                "agent.py",
                f"--temp-dir={abs_path}",
            ]
            assert scripts.matches_temp_dir_path(
                cmdline_normal, abs_path
            ), "Normal cmdline with spaces in path should match"

            # Test 2: setproctitle format (entire command as single string)
            # This is how psutil.Process.cmdline() returns it for setproctitle processes
            cmdline_setproctitle = [
                f"ray::DashboardAgent --temp-dir={abs_path}",
            ]
            assert scripts.matches_temp_dir_path(
                cmdline_setproctitle, abs_path
            ), f"setproctitle format with spaces in path should match: {cmdline_setproctitle}"

            # Test 3: Verify the old bug would have failed
            # The old code did: path_part.split()[0] if " " in path_part
            # For path "/tmp/ray temp dir", this would return "/tmp/ray" (wrong!)
            wrong_path = abs_path.split()[0]  # Simulates the old bug
            assert (
                wrong_path != abs_path
            ), "Sanity check: space-split path should differ"

    def test_trailing_slash_normalization(self):
        """Test that trailing slashes are normalized correctly."""
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            # cmdline has path without trailing slash
            cmdline = ["python", f"--temp-dir={temp_dir}"]

            # Should match with trailing slash
            assert scripts.matches_temp_dir_path(
                cmdline, temp_dir + "/"
            ), "Path with trailing slash should match path without"

            # Should match without trailing slash
            assert scripts.matches_temp_dir_path(
                cmdline, temp_dir
            ), "Path without trailing slash should match"

    def test_both_prefix_formats(self):
        """Test that both --temp-dir= and --temp_dir= formats are recognized."""
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            # Test --temp-dir= format
            cmdline_hyphen = ["python", f"--temp-dir={temp_dir}"]
            assert scripts.matches_temp_dir_path(
                cmdline_hyphen, temp_dir
            ), "--temp-dir= format should match"

            # Test --temp_dir= format
            cmdline_underscore = ["python", f"--temp_dir={temp_dir}"]
            assert scripts.matches_temp_dir_path(
                cmdline_underscore, temp_dir
            ), "--temp_dir= format should match"

    def test_no_match_returns_false(self):
        """Test that non-matching paths return False."""
        cmdline = ["python", "--temp-dir=/tmp/ray_cluster_1"]

        # Different path should not match
        assert not scripts.matches_temp_dir_path(
            cmdline, "/tmp/ray_cluster_2"
        ), "Different paths should not match"

        # No --temp-dir argument should not match
        cmdline_no_temp = ["python", "script.py"]
        assert not scripts.matches_temp_dir_path(
            cmdline_no_temp, "/tmp/ray"
        ), "Cmdline without --temp-dir should not match"
