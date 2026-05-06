import builtins
import sys

import pytest
from click.testing import CliRunner

import ray.scripts.scripts as scripts
from ray._common.network_utils import build_address


def test_ray_stop_address_helpers_extract_and_match(monkeypatch, tmp_path):
    def fake_resolve_ip_for_localhost(host):
        if host in ("localhost", "127.0.0.1", "::1"):
            return "10.0.0.1"
        return host

    def fake_getaddrinfo(host, port, family, socktype):
        if host == "head-node":
            return [(family, socktype, 0, "", ("10.0.0.2", port))]
        if host == "multi-ip-head":
            return [
                (family, socktype, 0, "", ("10.0.0.3", port)),
                (family, socktype, 0, "", ("10.0.0.4", port)),
            ]
        return [(family, socktype, 0, "", (host, port))]

    monkeypatch.setattr(
        scripts.services, "resolve_ip_for_localhost", fake_resolve_ip_for_localhost
    )
    monkeypatch.setattr(scripts.socket, "getaddrinfo", fake_getaddrinfo)

    assert (
        scripts._extract_flag_value(["--gcs-address=10.0.0.1:6379"], "gcs-address")
        == "10.0.0.1:6379"
    )
    assert (
        scripts._extract_flag_value(["--address", "10.0.0.1:6379"], "address")
        == "10.0.0.1:6379"
    )
    assert (
        scripts._extract_flag_value(["--gcs-address", "--block"], "gcs-address") is None
    )
    assert (
        scripts._extract_flag_value(["--ray_address=10.0.0.1:6379"], "ray_address")
        == "10.0.0.1:6379"
    )
    assert (
        scripts._extract_java_property_value(
            ["setup_worker.py", "-Dray.address=10.0.0.1:6379"],
            "ray.address",
        )
        == "10.0.0.1:6379"
    )
    assert (
        scripts._extract_java_property_value(
            ["ray::Worker -Dray.address='10.0.0.1:6379' --x=y"],
            "ray.address",
        )
        == "10.0.0.1:6379"
    )
    assert (
        scripts._extract_flag_value(
            ["ray::DashboardAgent --gcs-address='10.0.0.1:6379' --x=y"],
            "gcs-address",
        )
        == "10.0.0.1:6379"
    )
    assert scripts._extract_gcs_address_from_cmdline(
        ["gcs_server", "--node-ip-address=::1", "--gcs_server_port=6379"]
    ) == build_address("::1", "6379")

    monkeypatch.setattr(
        scripts.ray._raylet,
        "get_port_filename",
        lambda node_id, port_name: f"{port_name}_{node_id}",
    )
    port_file = tmp_path / f"{scripts.ray._raylet.GCS_SERVER_PORT_NAME}_node-1"
    port_file.write_text("6381", encoding="utf-8")
    assert scripts._extract_gcs_address_from_cmdline(
        [
            "gcs_server",
            "--node-ip-address=10.0.0.1",
            "--gcs_server_port=0",
            f"--session-dir={tmp_path}",
            "--node-id=node-1",
        ]
    ) == build_address("10.0.0.1", "6381")

    assert scripts._cmdline_matches_gcs_address(
        ["raylet", "--gcs-address=10.0.0.1:6379"], "localhost:6379"
    )
    assert scripts._cmdline_matches_gcs_address(
        ["setup_worker.py", "--ray_address=10.0.0.1:6379"], "localhost:6379"
    )
    assert scripts._cmdline_matches_gcs_address(
        [
            "setup_worker.py",
            "-Dray.address=10.0.0.1:6379",
            "io.ray.runtime.runner.worker.DefaultWorker",
        ],
        "localhost:6379",
    )
    assert scripts._cmdline_matches_gcs_address(
        [
            sys.executable,
            "-m",
            "ray.util.client.server",
            "--address=10.0.0.1:6379",
        ],
        "localhost:6379",
    )
    assert not scripts._cmdline_matches_gcs_address(
        ["some_ray_process", "--address=10.0.0.1:6379"], "localhost:6379"
    )
    assert scripts._cmdline_matches_gcs_address(
        ["gcs_server", "--node-ip-address=10.0.0.2", "--gcs_server_port=6380"],
        "head-node:6380",
    )
    assert not scripts._cmdline_matches_gcs_address(
        ["raylet", "--gcs-address=10.0.0.2:6379"], "head-node:6380"
    )
    assert scripts._cmdline_matches_gcs_address(
        ["raylet", "--gcs-address=10.0.0.3:6379"], "multi-ip-head:6379"
    )
    assert not scripts._cmdline_matches_gcs_address(
        ["raylet", "--gcs-address=10.0.0.4:6379"], "multi-ip-head:6379"
    )
    assert scripts._address_matches_gcs_address("10.0.0.1:6379", "localhost:6379")
    assert not scripts._address_matches_gcs_address("10.0.0.2:6379", "localhost:6379")

    def raise_gaierror(host, port, family, socktype):
        raise scripts.socket.gaierror()

    monkeypatch.setattr(scripts.socket, "getaddrinfo", raise_gaierror)
    assert scripts._normalize_gcs_address("unresolvable-head:6379") == (
        {"unresolvable-head"},
        6379,
    )


def test_gcs_server_port_zero_retry(monkeypatch, tmp_path):
    monkeypatch.setattr(
        scripts.ray._raylet,
        "get_port_filename",
        lambda node_id, port_name: f"{port_name}_{node_id}",
    )
    monkeypatch.setattr(scripts.time, "sleep", lambda seconds: None)

    port_file = tmp_path / f"{scripts.ray._raylet.GCS_SERVER_PORT_NAME}_node-1"
    port_file.write_text("6381", encoding="utf-8")

    real_open = builtins.open
    open_attempts = 0

    def flaky_open(path, *args, **kwargs):
        nonlocal open_attempts
        if path == str(port_file):
            open_attempts += 1
            if open_attempts < 3:
                raise OSError()
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", flaky_open)

    assert (
        scripts._extract_gcs_address_from_cmdline(
            [
                "gcs_server",
                "--node-ip-address=10.0.0.1",
                "--gcs_server_port=0",
                f"--session-dir={tmp_path}",
                "--node-id=node-1",
            ]
        )
        == "10.0.0.1:6381"
    )
    assert open_attempts == 3


def test_gcs_server_port_zero_missing_port_file(monkeypatch, tmp_path):
    monkeypatch.setattr(
        scripts.ray._raylet,
        "get_port_filename",
        lambda node_id, port_name: f"{port_name}_{node_id}",
    )
    monkeypatch.setattr(scripts.time, "sleep", lambda seconds: None)

    assert (
        scripts._extract_gcs_address_from_cmdline(
            [
                "gcs_server",
                "--node-ip-address=10.0.0.1",
                "--gcs_server_port=0",
                f"--session-dir={tmp_path}",
                "--node-id=node-1",
            ]
        )
        is None
    )


def test_ray_stop_address_only_stops_matching_processes(monkeypatch):
    class FakeProcess:
        def __init__(self, name, cmdline):
            self._name = name
            self._cmdline = cmdline
            self.terminated = False
            self.killed = False

        def name(self):
            return self._name

        def cmdline(self):
            return self._cmdline

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True

    target_raylet = FakeProcess("raylet", ["raylet", "--gcs-address=10.0.0.1:6379"])
    other_raylet = FakeProcess("raylet", ["raylet", "--gcs-address=10.0.0.2:6379"])
    target_gcs = FakeProcess(
        "gcs_server",
        ["gcs_server", "--node-ip-address=10.0.0.1", "--gcs_server_port=6379"],
    )
    other_gcs = FakeProcess(
        "gcs_server",
        ["gcs_server", "--node-ip-address=10.0.0.2", "--gcs_server_port=6379"],
    )
    no_identity_worker = FakeProcess("ray::IDLE", ["ray::IDLE"])
    no_identity_reaper = FakeProcess("python", ["ray_process_reaper.py"])
    processes = [
        target_raylet,
        other_raylet,
        target_gcs,
        other_gcs,
        no_identity_worker,
        no_identity_reaper,
    ]

    monkeypatch.setattr(
        scripts,
        "RAY_PROCESSES",
        [
            ["raylet", True],
            ["ray::", False],
            ["ray_process_reaper.py", False],
            ["gcs_server", True],
        ],
    )
    monkeypatch.setattr(scripts.psutil, "process_iter", lambda attrs: processes)
    monkeypatch.setattr(
        scripts.services,
        "resolve_ip_for_localhost",
        lambda host: host,
    )
    monkeypatch.setattr(
        scripts.socket,
        "getaddrinfo",
        lambda host, port, family, socktype: [(family, socktype, 0, "", (host, port))],
    )

    def fake_wait_procs(procs, timeout=None, callback=None):
        procs = list(procs)
        if callback is not None:
            for proc in procs:
                callback(proc)
        return procs, []

    monkeypatch.setattr(scripts.psutil, "wait_procs", fake_wait_procs)
    monkeypatch.setattr(
        scripts.ray._private.utils,
        "read_ray_address",
        lambda: "10.0.0.2:6379",
    )
    reset_calls = []
    monkeypatch.setattr(
        scripts.ray._common.utils,
        "reset_ray_address",
        lambda: reset_calls.append(True),
    )

    runner = CliRunner()
    result = runner.invoke(
        scripts.stop, ["--address", "10.0.0.1:6379", "--grace-period", "0"]
    )

    assert result.exit_code == 0, result.output
    assert target_raylet.terminated
    assert target_gcs.terminated
    assert not other_raylet.terminated
    assert not other_gcs.terminated
    assert not other_raylet.killed
    assert not other_gcs.killed
    assert not no_identity_worker.terminated
    assert not no_identity_reaper.terminated
    assert not no_identity_worker.killed
    assert not no_identity_reaper.killed
    assert reset_calls == []


def test_ray_stop_address_resets_matching_current_cluster(monkeypatch):
    monkeypatch.setattr(scripts.psutil, "process_iter", lambda attrs: [])
    monkeypatch.setattr(scripts.services, "find_gcs_addresses", lambda: frozenset())
    monkeypatch.setattr(
        scripts.services,
        "resolve_ip_for_localhost",
        lambda host: "10.0.0.1" if host == "localhost" else host,
    )
    monkeypatch.setattr(
        scripts.socket,
        "getaddrinfo",
        lambda host, port, family, socktype: [(family, socktype, 0, "", (host, port))],
    )
    monkeypatch.setattr(
        scripts.ray._private.utils,
        "read_ray_address",
        lambda: "localhost:6379",
    )
    reset_calls = []
    monkeypatch.setattr(
        scripts.ray._common.utils,
        "reset_ray_address",
        lambda: reset_calls.append(True),
    )

    result = CliRunner().invoke(
        scripts.stop, ["--address", "10.0.0.1:6379", "--grace-period", "0"]
    )

    assert result.exit_code == 0, result.output
    assert reset_calls == [True]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
