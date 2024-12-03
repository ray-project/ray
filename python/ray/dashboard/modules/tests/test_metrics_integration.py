import subprocess
import sys
import time

import pytest
from click.testing import CliRunner

from ray.dashboard.consts import PROMETHEUS_CONFIG_INPUT_PATH
from ray.dashboard.modules.metrics import install_and_start_prometheus
from ray.dashboard.modules.metrics.templates import PROMETHEUS_YML_TEMPLATE
from ray.scripts.scripts import metrics_group


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
