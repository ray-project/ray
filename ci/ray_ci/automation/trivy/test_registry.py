import hashlib
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

from ci.ray_ci.automation.trivy import registry


def completed(stdout: str = "", returncode: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


def test_crane_failure_raises_with_stderr():
    with mock.patch.object(
        registry, "crane_binary", return_value="crane"
    ), mock.patch.object(
        registry.subprocess,
        "run",
        return_value=completed(returncode=1, stderr="MANIFEST_UNKNOWN"),
    ):
        with pytest.raises(registry.RegistryError, match="MANIFEST_UNKNOWN"):
            registry.digest("repo:missing")


@pytest.mark.parametrize("tampered", [False, True])
def test_fetch_blob_checks_the_sha256(tmp_path: Path, tampered: bool):
    payload = b"trivy db"
    expected = "sha256:" + hashlib.sha256(payload).hexdigest()

    def run(cmd, stdout, stderr, check):
        stdout.write(payload + (b"x" if tampered else b""))
        return subprocess.CompletedProcess(cmd, 0, stderr=b"")

    dest = tmp_path / "db.tgz"
    with mock.patch.object(
        registry, "crane_binary", return_value="crane"
    ), mock.patch.object(registry.subprocess, "run", side_effect=run):
        if tampered:
            with pytest.raises(registry.RegistryError, match="downloaded with digest"):
                registry.fetch_blob("repo", expected, dest)
            assert not dest.exists()
        else:
            registry.fetch_blob("repo", expected, dest)
            assert dest.read_bytes() == payload


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
