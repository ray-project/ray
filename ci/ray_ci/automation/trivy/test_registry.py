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

    def run(cmd, stdout, **kwargs):
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


def fake_crane(tmp_path: Path, script: str) -> str:
    """A stand-in crane: a shell script, run for real by subprocess."""
    path = tmp_path / "crane"
    path.write_text("#!/bin/sh\n" + script + "\n")
    path.chmod(0o755)
    return str(path)


@pytest.mark.parametrize(
    ("script", "message"),
    [
        pytest.param(r"printf 'boom \377\n' >&2; exit 1", "boom", id="non-utf8-stderr"),
        pytest.param("echo not json", "printed no JSON", id="not-json"),
        pytest.param("echo '[1]'", "no JSON object", id="not-an-object"),
        pytest.param("sleep 5", "timed out after 1s", id="stalls"),
    ],
)
def test_crane_problems_are_registry_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, script: str, message: str
):
    crane = fake_crane(tmp_path, script)
    monkeypatch.setattr(registry, "crane_binary", lambda: crane)
    monkeypatch.setattr(registry, "CALL_TIMEOUT", 1)
    with pytest.raises(registry.RegistryError, match=message):
        registry.manifest("repo:tag")


def test_a_crane_that_cannot_run_is_a_registry_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(registry, "crane_binary", lambda: str(tmp_path / "none"))
    with pytest.raises(registry.RegistryError, match="could not run"):
        registry.digest("repo:tag")


def test_a_crane_missing_from_the_runfiles_is_a_registry_error(
    monkeypatch: pytest.MonkeyPatch,
):
    runfiles = mock.Mock(**{"Rlocation.return_value": None})
    monkeypatch.setattr(registry.runfiles, "Create", lambda: runfiles)
    with pytest.raises(registry.RegistryError, match="runfiles"):
        registry.crane_binary()


def test_a_failed_blob_download_leaves_no_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    crane = fake_crane(tmp_path, "printf partial; echo boom >&2; exit 1")
    monkeypatch.setattr(registry, "crane_binary", lambda: crane)
    dest = tmp_path / "db.tgz"
    with pytest.raises(registry.RegistryError, match="boom"):
        registry.fetch_blob("repo", "sha256:" + "0" * 64, dest)
    assert not dest.exists()


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
