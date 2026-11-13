import hashlib
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

from ci.ray_ci.automation.trivy import registry


def completed(stdout: str = "", returncode: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


def test_pull_puts_every_ref_into_one_layout(tmp_path: Path):
    with mock.patch.object(
        registry, "crane_binary", return_value="crane"
    ), mock.patch.object(registry.subprocess, "run", return_value=completed()) as run:
        registry.pull(["repo:a", "repo:b"], tmp_path / "layout", "linux/arm64")
    assert run.call_args.args[0] == [
        "crane",
        "pull",
        "--format",
        "oci",
        "--platform",
        "linux/arm64",
        "repo:a",
        "repo:b",
        str(tmp_path / "layout"),
    ]


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


@pytest.mark.parametrize(
    ("repo", "logged_in"),
    [
        ("029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp", True),
        ("localhost:5000/citemp", False),
    ],
)
def test_login_if_ecr(repo: str, logged_in: bool):
    login = mock.Mock()
    registry.login_if_ecr(repo, login)
    assert login.called is logged_in
    if logged_in:
        login.assert_called_once_with(repo.split("/")[0])


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
