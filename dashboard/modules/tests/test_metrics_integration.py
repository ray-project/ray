import subprocess
import pytest
import sys

from ray.dashboard.modules.metrics import install_and_start_prometheus


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
    path = install_and_start_prometheus.__file__
    result = subprocess.run([sys.executable, path], capture_output=True, text=True)

    assert "Download completed." in result.stdout
    assert "Prometheus has started" in result.stdout
    assert result.returncode == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
