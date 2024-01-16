import pytest
import sys
import os
import ray
import runpy

from ray.dashboard.modules.metrics import install_and_start_prometheus

# def get_system_info():
#     os_type = platform.system().lower()
#     architecture = platform.machine()
#     if architecture == "x86_64":
#         architecture = "amd64"
#     return os_type, architecture


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
    # set TEST_MODE_ENV_VAR to True to use requests.head instead of requests.get
    monkeypatch.setenv(install_and_start_prometheus.TEST_MODE_ENV_VAR, "True")
    downloaded, _ = install_and_start_prometheus.download_prometheus(
        os_type, architecture
    )
    assert downloaded


def test_e2e(capsys):
    path = install_and_start_prometheus.__file__
    runpy.run_path(path)

    captured = capsys.readouterr()
    assert "Download completed." in captured.out
    assert "Prometheus has started" in captured.out


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
