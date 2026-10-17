import secrets
import subprocess
import sys
import time

import pytest
import requests
from click.testing import CliRunner

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.authentication_test_utils import reset_auth_token_state
from ray.cluster_utils import Cluster
from ray.dashboard.consts import PROMETHEUS_CONFIG_INPUT_PATH
from ray.dashboard.modules.metrics import install_and_start_prometheus
from ray.dashboard.modules.metrics.templates import (
    PROMETHEUS_YML_TEMPLATE,
    get_prometheus_authorization_config,
)
from ray.scripts.scripts import metrics_group


@pytest.fixture
def auth_env(monkeypatch, tmp_path):
    """Isolate HOME and the auth env vars; yields the default token path."""
    monkeypatch.setenv("HOME", str(tmp_path))
    for var in ("RAY_AUTH_MODE", "RAY_AUTH_TOKEN", "RAY_AUTH_TOKEN_PATH"):
        monkeypatch.delenv(var, raising=False)
    yield tmp_path / ".ray" / "auth_token"
    monkeypatch.undo()
    reset_auth_token_state()


def _enable_token_auth(monkeypatch):
    monkeypatch.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()


def test_prometheus_authorization_config_empty_when_auth_disabled(
    auth_env, monkeypatch
):
    monkeypatch.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    assert get_prometheus_authorization_config() == ""


def test_prometheus_authorization_config_uses_token_path_env(
    auth_env, monkeypatch, tmp_path
):
    token_file = tmp_path / "my_token"
    token_file.write_text("a" * 64)
    auth_env.parent.mkdir(parents=True)
    auth_env.write_text("b" * 64)
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token_file))
    _enable_token_auth(monkeypatch)

    assert get_prometheus_authorization_config() == (
        "  authorization:\n"
        "    type: Bearer\n"
        f"    credentials_file: '{token_file}'\n"
    )


def test_prometheus_authorization_config_uses_default_token_file(auth_env, monkeypatch):
    auth_env.parent.mkdir(parents=True)
    auth_env.write_text("b" * 64)
    _enable_token_auth(monkeypatch)

    assert f"credentials_file: '{auth_env}'" in get_prometheus_authorization_config()


def test_prometheus_authorization_config_rejects_env_only_token(auth_env, monkeypatch):
    monkeypatch.setenv("RAY_AUTH_TOKEN", "a" * 64)
    _enable_token_auth(monkeypatch)

    with pytest.raises(ValueError, match="RAY_AUTH_TOKEN_PATH"):
        get_prometheus_authorization_config()


@pytest.mark.parametrize(
    "os_type,architecture",
    [
        ("linux", "amd64"),
        ("linux", "arm64"),
        ("darwin", "amd64"),
        ("darwin", "arm64"),
        ("windows", "amd64"),
        ("windows", "arm64"),
    ],
)
def test_download_prometheus(os_type, architecture, monkeypatch):
    # set TEST_MODE_ENV_VAR to True to use requests.head instead of requests.get.
    # This will make the download faster. We just want to make sure the URL
    # exists.
    monkeypatch.setenv(install_and_start_prometheus.TEST_MODE_ENV_VAR, "True")
    downloaded, _ = install_and_start_prometheus.download_prometheus(
        os_type, architecture
    )
    assert downloaded


def test_e2e(capsys):
    install_and_start_prometheus.main()
    captured = capsys.readouterr()
    assert "Prometheus is running" in captured.out
    # Find the Prometheus process and kill it.
    # Find the PID from the output: "To stop Prometheus, use the command: 'kill 22790'"
    pid = int(captured.out.split("kill ")[1].split("'")[0])
    subprocess.run(["kill", str(pid)])


def test_shutdown_prometheus():
    install_and_start_prometheus.main()
    runner = CliRunner()
    # Sleep for a few seconds to make sure Prometheus is running
    # before we try to shut it down.
    time.sleep(5)
    result = runner.invoke(metrics_group, ["shutdown-prometheus"])
    assert result.exit_code == 0


def test_prometheus_config_content():
    # Test to make sure the content in the hardcoded file
    # (python/ray/dashboard/modules/metrics/export/prometheus/prometheus.yml) will
    # always be the same as the template (templates.py) used to generate Prometheus
    # config file when Ray startup
    PROM_DISCOVERY_FILE_PATH = "/tmp/ray/prom_metrics_service_discovery.json"
    template_content = PROMETHEUS_YML_TEMPLATE.format(
        prom_metrics_service_discovery_file_path=PROM_DISCOVERY_FILE_PATH
    )
    with open(PROMETHEUS_CONFIG_INPUT_PATH) as f:
        assert f.read() == template_content


def test_launch_prometheus_scrapes_token_auth_cluster(auth_env, monkeypatch, capsys):
    token_file = auth_env.parent.parent / "token_file"
    token_file.write_text(secrets.token_hex(32))
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token_file))
    _enable_token_auth(monkeypatch)

    cluster = Cluster()
    cluster.add_node()
    cluster.add_node()
    ray.init(address=cluster.address)
    prometheus_pid = None
    try:
        install_and_start_prometheus.main()
        out = capsys.readouterr().out
        prometheus_pid = int(out.split("kill ")[1].split("'")[0])

        def all_targets_up():
            try:
                targets = requests.get(
                    "http://localhost:9090/api/v1/targets", timeout=5
                ).json()["data"]["activeTargets"]
            except requests.exceptions.ConnectionError:
                return False
            # Two nodes, the autoscaler and the dashboard head.
            return len(targets) == 4 and all(t["health"] == "up" for t in targets)

        wait_for_condition(all_targets_up, timeout=120, retry_interval_ms=2000)
    finally:
        if prometheus_pid:
            subprocess.run(["kill", str(prometheus_pid)])
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
