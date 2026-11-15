import json
import os
import secrets
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest
import requests
import yaml
from click.testing import CliRunner

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.authentication_test_utils import reset_auth_token_state
from ray.cluster_utils import Cluster
from ray.dashboard.consts import PROMETHEUS_CONFIG_INPUT_PATH
from ray.dashboard.modules.metrics import install_and_start_prometheus, metrics_head
from ray.dashboard.modules.metrics.install_and_start_prometheus import (
    get_prometheus_authorization_config,
)
from ray.dashboard.modules.metrics.templates import PROMETHEUS_YML_TEMPLATE
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


@pytest.mark.parametrize("auth_mode", ["token", "disabled"])
def test_prometheus_authorization_config_empty_when_metrics_auth_skipped(
    auth_env, monkeypatch, auth_mode
):
    monkeypatch.setenv("RAY_AUTH_SKIP_METRICS", "1")
    monkeypatch.setenv("RAY_AUTH_MODE", auth_mode)
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


def test_prometheus_authorization_config_inlines_env_token(auth_env, monkeypatch):
    # RAY_AUTH_TOKEN wins over a token file that holds a different token.
    auth_env.parent.mkdir(parents=True)
    auth_env.write_text("b" * 64)
    monkeypatch.setenv("RAY_AUTH_TOKEN", "a" * 64)
    _enable_token_auth(monkeypatch)

    assert get_prometheus_authorization_config() == (
        "  authorization:\n" "    type: Bearer\n" f"    credentials: '{'a' * 64}'\n"
    )


@pytest.mark.skipif(
    sys.platform == "win32" or os.geteuid() == 0,
    reason="chmod can't make a file unreadable here",
)
def test_prometheus_authorization_config_rejects_unreadable_file(auth_env, monkeypatch):
    auth_env.parent.mkdir(parents=True)
    auth_env.write_text("a" * 64)
    auth_env.chmod(0)
    _enable_token_auth(monkeypatch)

    with pytest.raises(ValueError, match="RAY_AUTH_TOKEN_PATH"):
        get_prometheus_authorization_config()


@pytest.mark.parametrize("file_content", [None, "\n"], ids=["missing", "empty"])
def test_prometheus_authorization_config_rejects_missing_or_empty_file(
    auth_env, monkeypatch, file_content
):
    if file_content is not None:
        auth_env.parent.mkdir(parents=True)
        auth_env.write_text(file_content)
    _enable_token_auth(monkeypatch)

    with pytest.raises(ValueError, match="RAY_AUTH_TOKEN_PATH"):
        get_prometheus_authorization_config()


def test_prometheus_authorization_config_accepts_blank_lines_around_token(
    auth_env, monkeypatch
):
    # Ray uses the first non-blank line, and Prometheus trims the file.
    auth_env.parent.mkdir(parents=True)
    auth_env.write_text("\n  \n" + "a" * 64 + "\n\n")
    _enable_token_auth(monkeypatch)

    assert f"credentials_file: '{auth_env}'" in get_prometheus_authorization_config()


def test_prometheus_authorization_config_rejects_multiline_file(auth_env, monkeypatch):
    # Ray uses the first non-blank line, but Prometheus sends the whole file.
    auth_env.parent.mkdir(parents=True)
    auth_env.write_text("\n" + "a" * 64 + "\n# note\n")
    _enable_token_auth(monkeypatch)

    with pytest.raises(ValueError, match="only the token"):
        get_prometheus_authorization_config()


@pytest.mark.parametrize("token_source", ["env", "file"])
def test_prometheus_authorization_config_survives_quotes(
    auth_env, monkeypatch, tmp_path, token_source
):
    if token_source == "env":
        monkeypatch.setenv("RAY_AUTH_TOKEN", "it's")
        expected = {"type": "Bearer", "credentials": "it's"}
    else:
        token_file = tmp_path / "it's token"
        token_file.write_text("a" * 64)
        monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token_file))
        expected = {"type": "Bearer", "credentials_file": str(token_file)}
    _enable_token_auth(monkeypatch)

    config = install_and_start_prometheus.write_prometheus_config(
        tmp_path, get_prometheus_authorization_config()
    )

    (ray_job,) = yaml.safe_load(config.read_text())["scrape_configs"]
    assert ray_job["job_name"] == "ray"
    assert ray_job["authorization"] == expected


# None leaves RAY_AUTH_MODE unset, which means token mode.
@pytest.mark.parametrize("auth_mode", ["token", None])
def test_launch_prometheus_exits_before_download_without_token(
    auth_env, monkeypatch, auth_mode
):
    if auth_mode:
        _enable_token_auth(monkeypatch)

    def fail_download(*args, **kwargs):
        raise AssertionError("downloaded Prometheus without a token")

    monkeypatch.setattr(
        install_and_start_prometheus, "download_prometheus", fail_download
    )

    with pytest.raises(SystemExit) as exit_info:
        install_and_start_prometheus.main()
    assert exit_info.value.code == 1


def test_launch_prometheus_exits_cleanly_when_config_write_fails(
    auth_env, monkeypatch, tmp_path, caplog
):
    monkeypatch.setenv("RAY_AUTH_TOKEN", "a" * 64)
    _enable_token_auth(monkeypatch)
    prometheus_dir = tmp_path / "prom"
    # A directory where the config goes makes the write fail with an OSError.
    (prometheus_dir / "ray_prometheus.yml").mkdir(parents=True)
    monkeypatch.setattr(
        install_and_start_prometheus,
        "download_prometheus",
        lambda: (True, f"{prometheus_dir}.tar.gz"),
    )
    monkeypatch.setattr(
        install_and_start_prometheus, "install_prometheus", lambda _: True
    )

    with pytest.raises(SystemExit) as exit_info:
        install_and_start_prometheus.main()
    assert exit_info.value.code == 1
    assert "ray_prometheus.yml" in caplog.text


def test_write_prometheus_config_is_owner_only_and_replaces_old_file(tmp_path):
    victim = tmp_path / "victim"
    victim.write_text("untouched")
    (tmp_path / "ray_prometheus.yml").symlink_to(victim)

    config = install_and_start_prometheus.write_prometheus_config(
        tmp_path, "  authorization:\n    type: Bearer\n    credentials: 'x'\n"
    )

    assert config == tmp_path / "ray_prometheus.yml"
    assert not config.is_symlink()
    assert config.stat().st_mode & 0o777 == 0o600
    assert config.read_text().endswith("    credentials: 'x'\n")
    assert victim.read_text() == "untouched"


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


def _write_default_token(auth_env):
    auth_env.parent.mkdir(parents=True)
    auth_env.write_text("a" * 64)


def test_e2e(auth_env, capsys):
    _write_default_token(auth_env)
    install_and_start_prometheus.main()
    captured = capsys.readouterr()
    assert "Prometheus is running" in captured.out
    # Find the Prometheus process and kill it.
    # Find the PID from the output: "To stop Prometheus, use the command: 'kill 22790'"
    pid = int(captured.out.split("kill ")[1].split("'")[0])
    subprocess.run(["kill", str(pid)])


def test_shutdown_prometheus(auth_env):
    _write_default_token(auth_env)
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


def _write_dashboard_prometheus_config(tmp_path):
    head = SimpleNamespace(_metrics_root=str(tmp_path), session_dir=str(tmp_path))
    metrics_head.MetricsHead._create_default_prometheus_configs(head)
    return (tmp_path / "prometheus" / "prometheus.yml").read_text()


def test_dashboard_prometheus_config_has_no_authorization_block(auth_env, monkeypatch):
    auth_env.parent.mkdir(parents=True)
    auth_env.write_text("b" * 64)
    monkeypatch.setenv("RAY_AUTH_TOKEN", "b" * 64)
    _enable_token_auth(monkeypatch)

    warnings = []
    monkeypatch.setattr(metrics_head.logger, "warning", warnings.append)

    config = _write_dashboard_prometheus_config(auth_env.parent.parent)

    assert "authorization" not in config
    assert warnings == []


DISCOVERY_FILE_PATH = "/tmp/ray/prom_metrics_service_discovery.json"


@pytest.mark.parametrize("token_source", ["file", "env", "env_skip_metrics"])
def test_launch_prometheus_scrapes_token_auth_cluster(
    auth_env, monkeypatch, capsys, token_source
):
    token = secrets.token_hex(32)
    if token_source == "file":
        token_file = auth_env.parent.parent / "token_file"
        # Blank lines around the token: Ray takes the first non-blank line and
        # Prometheus trims the file, so both must still agree on the token.
        token_file.write_text(f"\n{token}\n\n")
        monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token_file))
    else:
        monkeypatch.setenv("RAY_AUTH_TOKEN", token)
    if token_source == "env_skip_metrics":
        monkeypatch.setenv("RAY_AUTH_SKIP_METRICS", "1")
    _enable_token_auth(monkeypatch)

    cluster = Cluster()
    prometheus_pid = None
    try:
        cluster.add_node()
        cluster.add_node()
        ray.init(address=cluster.address)

        # launch-prometheus reads targets through the /tmp/ray symlink, so Prometheus
        # misses later rewrites of the session file until its 5-minute refresh; start
        # it only once the file lists two nodes, the autoscaler and the dashboard head.
        def discovery_file_lists_all_targets():
            with open(DISCOVERY_FILE_PATH) as f:
                return len(json.load(f)[0]["targets"]) == 4

        wait_for_condition(discovery_file_lists_all_targets, timeout=60)
        written_configs = []
        write_prometheus_config = install_and_start_prometheus.write_prometheus_config

        def record_written_config(*args):
            written_configs.append(write_prometheus_config(*args))
            return written_configs[-1]

        monkeypatch.setattr(
            install_and_start_prometheus,
            "write_prometheus_config",
            record_written_config,
        )
        install_and_start_prometheus.main()
        out = capsys.readouterr().out
        prometheus_pid = int(out.split("kill ")[1].split("'")[0])
        if token_source == "env":
            (config,) = written_configs
            assert config.stat().st_mode & 0o777 == 0o600
            assert f"credentials: '{token}'" in config.read_text()
        if token_source == "env_skip_metrics":
            # No authorization block, so Prometheus runs the bundled config as is.
            assert written_configs == []

        def all_targets_up():
            try:
                response = requests.get(
                    "http://localhost:9090/api/v1/targets", timeout=5
                )
                if response.status_code != 200:
                    return False
                targets = response.json()["data"]["activeTargets"]
            except (requests.RequestException, ValueError, KeyError, TypeError):
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
