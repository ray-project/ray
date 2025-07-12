import pytest
import subprocess
import runfiles
import platform
import sys
import unittest

_REPO_NAME = "com_github_ray_project_ray"
_runfiles = runfiles.Create()


class TestCli(unittest.TestCase):
    def test_uv_binary_exists(self):
        assert _uv_binary() is not None

    def test_uv_version(self):
        result = subprocess.run(
            [_uv_binary(), "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert result.returncode == 0
        assert "uv 0.7.20" in result.stdout.decode("utf-8")
        assert result.stderr.decode("utf-8") == ""


def _uv_binary():
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise RuntimeError(
            f"Unsupported platform/processor: {system}/{platform.processor()}"
        )
    return _runfiles.Rlocation("uv_x86_64/uv-x86_64-unknown-linux-gnu/uv")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
