"""
Tests for stopping specific Ray clusters by GCS address.

Verifies that multiple Ray clusters on the same node (different GCS ports) can be
stopped selectively via `ray stop --address=<GCS address>`.

Example:
  ray start --head --port 6380 && ray start --address=127.0.0.1:6380
  ray start --head --port 6381 && ray start --address=127.0.0.1:6381
  ray stop --address=127.0.0.1:6380   # stops only cluster on 6380
"""

import os
import shutil
import subprocess
import sys
import uuid
from typing import Dict, List, Optional, Tuple

import pytest
from click.testing import CliRunner

import ray.scripts.scripts as scripts
from ray._common.test_utils import wait_for_condition
from ray.autoscaler._private.constants import RAY_PROCESSES

import psutil


class TestExtractGcsAddressFromCmdline:
    """Unit tests for _extract_gcs_address_from_cmdline."""

    # Strategy 1: --gcs-address=ip:port (most Ray processes)
    @pytest.mark.parametrize(
        "cmdline,expected",
        [
            (
                ["raylet", "--gcs-address=127.0.0.1:6379"],
                "127.0.0.1:6379",
            ),
            (
                ["raylet", "--gcs-address=10.0.0.1:6380", "--other-flag=val"],
                "10.0.0.1:6380",
            ),
            (
                [
                    "python",
                    "-u",
                    "log_monitor.py",
                    "--gcs-address=192.168.1.1:6379",
                ],
                "192.168.1.1:6379",
            ),
        ],
    )
    def test_gcs_address_flag(self, cmdline, expected):
        assert scripts._extract_gcs_address_from_cmdline(cmdline) == expected

    # Strategy 2: --address=ip:port (ray.util.client.server)
    @pytest.mark.parametrize(
        "cmdline,expected",
        [
            (
                [
                    "python",
                    "-m",
                    "ray.util.client.server",
                    "--address=127.0.0.1:6379",
                ],
                "127.0.0.1:6379",
            ),
            (
                [
                    "setup_worker.py",
                    "-m",
                    "ray.util.client.server",
                    "--address=10.0.0.1:6380",
                    "--host=0.0.0.0",
                ],
                "10.0.0.1:6380",
            ),
        ],
    )
    def test_address_flag_client_server(self, cmdline, expected):
        assert scripts._extract_gcs_address_from_cmdline(cmdline) == expected

    # Strategy 3: gcs_server (--node-ip-address + --gcs_server_port)
    @pytest.mark.parametrize(
        "cmdline,expected",
        [
            (
                [
                    "gcs_server",
                    "--node-ip-address=10.0.0.1",
                    "--gcs_server_port=6379",
                ],
                "10.0.0.1:6379",
            ),
            # underscore variant: --node_ip_address
            (
                [
                    "gcs_server",
                    "--node_ip_address=10.0.0.1",
                    "--gcs_server_port=6379",
                ],
                "10.0.0.1:6379",
            ),
            # only ip, no port -> None
            (["gcs_server", "--node-ip-address=10.0.0.1"], None),
            # only port, no ip -> None
            (["gcs_server", "--gcs_server_port=6379"], None),
        ],
    )
    def test_gcs_server_flags(self, cmdline, expected):
        assert scripts._extract_gcs_address_from_cmdline(cmdline) == expected

    # Strategy 4: setproctitle concatenated args
    @pytest.mark.parametrize(
        "cmdline,expected",
        [
            (
                ["ray::DashboardAgent --gcs-address=127.0.0.1:6379"],
                "127.0.0.1:6379",
            ),
            (
                ["ray::WorkerProcess --gcs-address=10.0.0.1:6380 --other-flag"],
                "10.0.0.1:6380",
            ),
            (
                ["ray::ClientServer --address=10.0.0.1:6380 --host=0.0.0.0"],
                "10.0.0.1:6380",
            ),
            (
                ["ray::GCS --node-ip-address=10.0.0.1 --gcs_server_port=6379"],
                "10.0.0.1:6379",
            ),
        ],
    )
    def test_setproctitle_concatenated(self, cmdline, expected):
        assert scripts._extract_gcs_address_from_cmdline(cmdline) == expected

    # Edge cases
    @pytest.mark.parametrize(
        "cmdline",
        [
            [],
            ["python"],
            ["python", "some_script.py", "--verbose"],
        ],
    )
    def test_returns_none_when_no_address(self, cmdline):
        assert scripts._extract_gcs_address_from_cmdline(cmdline) is None

    def test_gcs_address_takes_priority_over_address(self):
        """--gcs-address should be found before --address (Strategy 1 before 2)."""
        cmdline = [
            "raylet",
            "--gcs-address=10.0.0.1:6379",
            "--address=10.0.0.2:6380",
        ]
        assert scripts._extract_gcs_address_from_cmdline(cmdline) == "10.0.0.1:6379"


class TestNormalizeGcsAddress:
    """Unit tests for _normalize_gcs_address."""

    def test_basic_ipv4(self):
        ip, port = scripts._normalize_gcs_address("127.0.0.1:6379")
        assert port == 6379
        assert isinstance(ip, str)

    def test_localhost_resolves_same_as_127(self):
        assert scripts._normalize_gcs_address(
            "localhost:6379"
        ) == scripts._normalize_gcs_address("127.0.0.1:6379")

    def test_different_ports_differ(self):
        assert scripts._normalize_gcs_address(
            "127.0.0.1:6379"
        ) != scripts._normalize_gcs_address("127.0.0.1:6380")

    @pytest.mark.parametrize(
        "invalid",
        [
            "no_port",
            "not_ip_port",
        ],
    )
    def test_invalid_format_raises(self, invalid):
        with pytest.raises(Exception):
            scripts._normalize_gcs_address(invalid)


class TestCmdlineMatchesGcsAddress:
    """Unit tests for _cmdline_matches_gcs_address."""

    def test_matching_address(self):
        cmdline = ["raylet", "--gcs-address=127.0.0.1:6379"]
        assert scripts._cmdline_matches_gcs_address(cmdline, "127.0.0.1:6379") is True

    def test_non_matching_port(self):
        cmdline = ["raylet", "--gcs-address=127.0.0.1:6379"]
        assert scripts._cmdline_matches_gcs_address(cmdline, "127.0.0.1:6380") is False

    def test_localhost_matches_127(self):
        cmdline = ["raylet", "--gcs-address=127.0.0.1:6379"]
        assert scripts._cmdline_matches_gcs_address(cmdline, "localhost:6379") is True

    def test_no_address_in_cmdline(self):
        cmdline = ["python", "some_script.py"]
        assert scripts._cmdline_matches_gcs_address(cmdline, "127.0.0.1:6379") is False

    def test_empty_cmdline(self):
        assert scripts._cmdline_matches_gcs_address([], "127.0.0.1:6379") is False

    def test_invalid_target_address_returns_false(self):
        cmdline = ["raylet", "--gcs-address=127.0.0.1:6379"]
        assert scripts._cmdline_matches_gcs_address(cmdline, "invalid") is False

    def test_gcs_server_cmdline(self):
        """gcs_server uses --node-ip-address + --gcs_server_port."""
        cmdline = [
            "gcs_server",
            "--node-ip-address=127.0.0.1",
            "--gcs_server_port=6379",
        ]
        assert scripts._cmdline_matches_gcs_address(cmdline, "127.0.0.1:6379") is True
        assert scripts._cmdline_matches_gcs_address(cmdline, "127.0.0.1:6380") is False


# ---------------------------------------------------------------------------
# Integration test helpers
# ---------------------------------------------------------------------------


def _get_process_info(proc: psutil.Process) -> Optional[Tuple[int, str, List[str]]]:
    try:
        return (proc.pid, proc.name(), proc.cmdline() or [])
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def _is_ray_process(proc_name: str, proc_cmdline: List[str]) -> bool:
    """Same process filter as ray stop (RAY_PROCESSES)."""
    cmdline_str = subprocess.list2cmdline(proc_cmdline)
    for keyword, filter_by_cmd in RAY_PROCESSES:
        if filter_by_cmd:
            if keyword in proc_name:
                return True
        else:
            if keyword in cmdline_str:
                return True
    return False


def _get_pids_by_address(address: str) -> List[Dict]:
    """Ray processes whose GCS address normalizes to the given address."""
    target = scripts._normalize_gcs_address(address)
    procs = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        info = _get_process_info(proc)
        if info is None:
            continue
        pid, name, cmdline_list = info
        if not _is_ray_process(name, cmdline_list):
            continue
        extracted = scripts._extract_gcs_address_from_cmdline(cmdline_list)
        if extracted is None:
            continue
        try:
            if scripts._normalize_gcs_address(extracted) == target:
                procs.append(
                    {
                        "pid": pid,
                        "name": name,
                        "cmdline": subprocess.list2cmdline(cmdline_list),
                        "gcs_address": extracted,
                    }
                )
        except Exception:
            continue
    return procs


def _get_all_ray_processes() -> List[Dict]:
    """All Ray processes with optional gcs_address (for debugging)."""
    out = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        info = _get_process_info(proc)
        if info is None:
            continue
        pid, name, cmdline_list = info
        if not _is_ray_process(name, cmdline_list):
            continue
        out.append(
            {
                "pid": pid,
                "name": name,
                "cmdline": subprocess.list2cmdline(cmdline_list),
                "gcs_address": scripts._extract_gcs_address_from_cmdline(cmdline_list),
            }
        )
    return out


def _format_process_list(procs: List[Dict], header: str) -> str:
    lines = [f"\n{'='*60}", header, "=" * 60]
    for p in procs:
        cmd = p["cmdline"][:100] + "..." if len(p["cmdline"]) > 100 else p["cmdline"]
        addr = p.get("gcs_address") or "N/A"
        lines.append(
            f"  PID={p['pid']:6} | {p['name']:20} | GCS={str(addr):20} | {cmd}"
        )
    if not procs:
        lines.append("  (none)")
    lines.append("")
    return "\n".join(lines)


def _all_pids_stopped(procs: List[Dict]) -> bool:
    return all(not psutil.pid_exists(p["pid"]) for p in procs)


def _all_pids_running(procs: List[Dict]) -> bool:
    return all(psutil.pid_exists(p["pid"]) for p in procs)


def _die_on_error(result):
    """Assert success; print output and re-raise on failure (test_cli style)."""
    if result.exit_code != 0:
        print(result.output)
        if result.exception:
            raise result.exception
        raise AssertionError(result.output)


def _start_cluster(
    runner, port: int, base_dir: Optional[str] = None
) -> Tuple[str, List[Dict]]:
    """Start head + worker at 127.0.0.1:port; return (address, process list)."""
    address = f"127.0.0.1:{port}"
    env = {**os.environ, "RAY_TMPDIR": base_dir} if base_dir else None
    kw = {"env": env} if env else {}

    result = runner.invoke(
        scripts.start,
        ["--head", "--port", str(port), "--node-ip-address=127.0.0.1"],
        **kw,
    )
    _die_on_error(result)

    if base_dir:
        wait_for_condition(
            lambda: os.path.isdir(os.path.join(base_dir, "ray", "session_latest")),
            timeout=30,
        )
    else:
        wait_for_condition(
            lambda: len(_get_pids_by_address(address)) >= 1,
            timeout=30,
        )

    result = runner.invoke(scripts.start, [f"--address={address}"], **kw)
    _die_on_error(result)
    wait_for_condition(
        lambda: len(_get_pids_by_address(address)) >= 2,
        timeout=45,
    )
    return address, _get_pids_by_address(address)


@pytest.fixture
def cleanup_ray():
    """Stop all Ray before and after test; yield CliRunner."""
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


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="ray start not supported on Windows.",
)
class TestAddressIsolation:
    """Multiple clusters (different GCS addresses) and selective stop."""

    def test_two_clusters_independent_stop(self, cleanup_ray):
        """Two clusters; stop one by address; the other keeps running."""
        runner = cleanup_ray
        port_1, port_2 = 6380, 6381
        address_1 = f"127.0.0.1:{port_1}"
        address_2 = f"127.0.0.1:{port_2}"

        cluster2_base = f"/tmp/ray_c2_{uuid.uuid4().hex[:8]}"
        os.makedirs(cluster2_base, exist_ok=True)
        try:
            address_1, cluster1_procs = _start_cluster(runner, port_1)
            address_2, cluster2_procs = _start_cluster(
                runner, port_2, base_dir=cluster2_base
            )

            expected_1 = scripts._normalize_gcs_address(address_1)
            expected_2 = scripts._normalize_gcs_address(address_2)
            for p in cluster1_procs:
                assert p.get("gcs_address") is not None
                assert scripts._normalize_gcs_address(p["gcs_address"]) == expected_1
            for p in cluster2_procs:
                assert p.get("gcs_address") is not None
                assert scripts._normalize_gcs_address(p["gcs_address"]) == expected_2

            assert len(cluster1_procs) >= 2 and len(cluster2_procs) >= 2

            result = runner.invoke(scripts.stop, [f"--address={address_1}"])
            _die_on_error(result)
            wait_for_condition(lambda: _all_pids_stopped(cluster1_procs), timeout=30)

            assert len(_get_pids_by_address(address_1)) == 0
            assert _all_pids_running(cluster2_procs)

            result = runner.invoke(scripts.stop, [f"--address={address_2}"])
            _die_on_error(result)
            wait_for_condition(lambda: _all_pids_stopped(cluster2_procs), timeout=30)
            assert len(_get_pids_by_address(address_1)) == 0
            assert len(_get_pids_by_address(address_2)) == 0
        finally:
            if os.path.exists(cluster2_base):
                shutil.rmtree(cluster2_base, ignore_errors=True)

    def test_stop_nonexistent_address_shows_warning_and_existing_addresses(
        self, cleanup_ray
    ):
        """Nonexistent address: warning + list existing; no kill."""
        runner = cleanup_ray
        address, cluster_procs = _start_cluster(runner, 6380)

        result = runner.invoke(scripts.stop, ["--address=127.0.0.1:99999"])
        assert result.exit_code == 0
        assert "99999" in result.output or "No Ray processes found" in result.output
        assert "6380" in result.output
        assert _all_pids_running(cluster_procs)

        runner.invoke(scripts.stop, [f"--address={address}"])
        wait_for_condition(lambda: _all_pids_stopped(cluster_procs), timeout=30)

    def test_stop_address_normalization_localhost_and_127(self, cleanup_ray):
        """localhost:port and 127.0.0.1:port refer to the same cluster."""
        runner = cleanup_ray
        address_ip, cluster_procs = _start_cluster(runner, 6380)

        result = runner.invoke(scripts.stop, ["--address=localhost:6380"])
        _die_on_error(result)
        wait_for_condition(lambda: _all_pids_stopped(cluster_procs), timeout=30)
        assert len(_get_pids_by_address(address_ip)) == 0

    def test_stop_invalid_address_format_rejected(self, cleanup_ray):
        """Invalid address format: non-zero exit, error message, no processes killed."""
        runner = cleanup_ray
        address, cluster_procs = _start_cluster(runner, 6380)

        for invalid in ["no_port", "127.0.0.1", "not_ip_port"]:
            result = runner.invoke(scripts.stop, [f"--address={invalid}"])
            assert result.exit_code != 0
            assert "invalid" in result.output.lower()
            assert _all_pids_running(cluster_procs)

        runner.invoke(scripts.stop, [f"--address={address}"])
        wait_for_condition(lambda: _all_pids_stopped(cluster_procs), timeout=30)

    def test_stop_force_with_address(self, cleanup_ray):
        """--force + --address should SIGKILL only matching processes."""
        runner = cleanup_ray
        address, cluster_procs = _start_cluster(runner, 6380)

        result = runner.invoke(scripts.stop, ["--force", f"--address={address}"])
        _die_on_error(result)
        wait_for_condition(lambda: _all_pids_stopped(cluster_procs), timeout=15)
        assert len(_get_pids_by_address(address)) == 0

    def test_stop_address_no_ray_running(self, cleanup_ray):
        """No Ray running + --address -> graceful message, exit 0."""
        runner = cleanup_ray
        # cleanup_ray already ran ray stop --force, so no Ray processes should exist
        result = runner.invoke(scripts.stop, ["--address=127.0.0.1:6379"])
        assert result.exit_code == 0
        out_lower = result.output.lower()
        assert "no ray processes" in out_lower or "did not find" in out_lower

    def test_plain_stop_kills_all_clusters(self, cleanup_ray):
        """Plain 'ray stop' (no --address) should kill everything — regression."""
        runner = cleanup_ray
        cluster2_base = f"/tmp/ray_c2_{uuid.uuid4().hex[:8]}"
        os.makedirs(cluster2_base, exist_ok=True)
        try:
            _, procs_1 = _start_cluster(runner, 6380)
            _, procs_2 = _start_cluster(runner, 6381, base_dir=cluster2_base)

            result = runner.invoke(scripts.stop, ["--force"])
            _die_on_error(result)
            wait_for_condition(
                lambda: _all_pids_stopped(procs_1) and _all_pids_stopped(procs_2),
                timeout=30,
            )
        finally:
            shutil.rmtree(cluster2_base, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
