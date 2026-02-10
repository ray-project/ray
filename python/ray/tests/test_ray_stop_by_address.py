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
import uuid
from typing import Dict, List, Optional, Tuple

import pytest
from click.testing import CliRunner

import ray.scripts.scripts as scripts
from ray._common.test_utils import wait_for_condition
from ray.autoscaler._private.constants import RAY_PROCESSES

import psutil


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
        """Nonexistent address: warning + list existing addresses; no processes killed."""
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
