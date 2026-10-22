"""Fast regression tests for the dedicated Hive security runner."""

import json
import os
import signal
import subprocess
import sys
from types import SimpleNamespace
from xml.etree.ElementTree import Element, SubElement, tostring

import pytest

from ci.ray_ci.data.hive_auth import run


def results(path, *, profile="GT", damage=None):
    root = Element("testsuite")
    names = list(run.CASES[profile])
    if damage == "empty":
        names = []
    elif damage == "missing":
        names.pop()
    elif damage == "extra":
        names.append("unplanned")
    elif damage == "duplicate":
        names[-1] = names[0]
    for name in names:
        case = SubElement(root, "testcase", name=f"test_hive_auth[{name}]")
        if damage in {"failure", "error", "skipped"}:
            SubElement(case, damage)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(tostring(root))


@pytest.mark.parametrize("profile", ["G", "T", "GT", "N"])
def test_results_require_exact_profile_case_list(tmp_path, profile):
    path = tmp_path / "results.xml"
    results(path, profile=profile)
    assert run.validate_results(path, profile) == len(run.CASES[profile])


@pytest.mark.parametrize(
    "damage", ["empty", "missing", "extra", "duplicate", "failure", "error", "skipped"]
)
def test_zero_partial_skipped_or_failed_results_are_rejected(tmp_path, damage):
    path = tmp_path / "results.xml"
    results(path, damage=damage)
    with pytest.raises(ValueError):
        run.validate_results(path, "GT")


def arguments(tmp_path, **kwargs):
    return SimpleNamespace(
        hive_image="apache/hive:4.2.0",
        readiness_timeout=300,
        profile_timeout=1200,
        cleanup_timeout=60,
        prepare_only=False,
        **kwargs,
    )


@pytest.mark.parametrize(
    "failure",
    ["preflight", "start", "ready", "tests", "interrupt", "timeout", "cleanup", None],
)
def test_failure_and_interruption_preserve_exit_and_clean_only_own_project(
    tmp_path, monkeypatch, failure
):
    calls = []
    directory = tmp_path / "GT"

    def command(args, log, *, env, timeout, check=True):
        calls.append((args, env))
        assert 0 < timeout <= 1200
        assert not any(k in env for k in run.PROXIES)
        assert args[:2] == ["docker", "compose"]
        assert "ray-hive-auth-test-run-gt" in args
        stage = log.stem
        if stage == "config" and failure == "preflight":
            raise run.CommandFailed(stage, 42)
        if stage == "start" and failure == "start":
            raise run.CommandFailed(stage, 42)
        if stage == "pytest":
            if failure == "tests":
                raise run.CommandFailed(stage, 42)
            if failure == "interrupt":
                raise KeyboardInterrupt
            if failure == "timeout":
                raise subprocess.TimeoutExpired(args, timeout)
        if stage == "copy":
            results(directory / "events/results.xml")
        if stage == "cleanup" and failure in {"cleanup", "tests"}:
            raise run.CommandFailed(stage, 7)
        return 0

    def wait(*args):
        if failure == "ready":
            raise TimeoutError

    monkeypatch.setattr(run, "command", command)
    monkeypatch.setattr(run, "wait_for_probe", wait)
    monkeypatch.setenv("HTTP_PROXY", "must-be-removed")
    code, result = run.run_profile(
        arguments(tmp_path), "GT", "test-image:fixed", "test-run", directory
    )
    cleanups = [args for args, _ in calls if "down" in args]
    if failure == "preflight":
        assert not cleanups
    else:
        assert len(cleanups) == 1
        assert "--volumes" in cleanups[0]
    if failure in {"preflight", "start", "tests"}:
        assert code == 42
    elif failure == "cleanup":
        assert code == 7
    elif failure:
        assert code != 0
    else:
        assert code == 0 and result["cases"] == 2
    assert json.loads((directory / "result.json").read_text())["exit_code"] == code


def test_prepare_only_does_not_claim_functional_pass(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(run, "command", lambda args, *a, **kw: calls.append(args) or 0)
    monkeypatch.setattr(run, "wait_for_probe", lambda *a: None)
    args = arguments(tmp_path)
    args.prepare_only = True
    code, result = run.run_profile(args, "G", "runtime:fixed", "test", tmp_path / "G")
    assert code == 0 and result["state"] == "prepared_only"
    assert not any("pytest" in command for command in calls)


def test_malformed_junit_records_failure_and_still_cleans(tmp_path, monkeypatch):
    directory = tmp_path / "GT"
    calls = []

    def command(args, log, **kwargs):
        calls.append(args)
        if log.stem == "copy":
            (directory / "events").mkdir()
            (directory / "events/results.xml").write_text("<broken")
        return 0

    monkeypatch.setattr(run, "command", command)
    monkeypatch.setattr(run, "wait_for_probe", lambda *a: None)
    code, result = run.run_profile(
        arguments(tmp_path), "GT", "runtime:fixed", "test", directory
    )
    assert code != 0 and result["state"] == "failed" and result["stage"] == "results"
    assert any("down" in call for call in calls)


def test_duplicate_profile_is_rejected_before_docker(tmp_path, monkeypatch):
    monkeypatch.setattr(
        run, "command", lambda *a, **kw: pytest.fail("Docker must not run")
    )
    with pytest.raises(ValueError, match="only once"):
        run.main(["--profiles", "G", "G", "--log-dir", str(tmp_path)])


def test_existing_artifacts_are_preserved(tmp_path, monkeypatch):
    previous = tmp_path / "result.json"
    previous.write_text("previous-evidence")
    monkeypatch.setattr(
        run, "command", lambda *a, **kw: pytest.fail("Docker must not run")
    )
    with pytest.raises(ValueError, match="previous evidence is preserved"):
        run.main(["--profiles", "N", "--log-dir", str(tmp_path)])
    assert previous.read_text() == "previous-evidence"


@pytest.mark.parametrize(
    "code,pytest_code",
    [
        (0, 0),
        (1, None),
        (42, 1),
        (2, 2),
    ],
)
def test_ci_retry_preserves_exec_and_pytest_status(
    tmp_path, monkeypatch, code, pytest_code
):
    monkeypatch.setenv("BUILDKITE", "true")
    monkeypatch.setenv("BUILDKITE_RETRY_COUNT", "1")
    monkeypatch.setattr(run, "image_info", lambda *a: {})
    monkeypatch.setattr(run, "remove_owned_dangling", lambda *a: None)
    monkeypatch.setattr(run, "wait_for_probe", lambda *a: None)
    calls = []
    directory = tmp_path / "GT"

    def command(args, log, **kwargs):
        calls.append(log.stem)
        if log.stem == "pytest":
            assert args[args.index("python") + 1 :][:3] == [
                "-c",
                run.PYTEST_ENTRYPOINT,
                "/events/pytest-exit-code",
            ]
            if code:
                raise run.CommandFailed("pytest", code)
        if log.stem == "copy":
            results(directory / "events/results.xml")
        if log.stem in {"copy", "events"} and pytest_code is not None:
            artifacts = directory / (
                "events" if log.stem == "copy" else "failure-events"
            )
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "pytest-exit-code").write_text(str(pytest_code))
        return 0

    monkeypatch.setattr(run, "command", command)
    assert (
        run.main(
            [
                "--profiles",
                "GT",
                "--runtime-image",
                "runtime:fixed",
                "--log-dir",
                str(tmp_path),
            ]
        )
        == code
    )
    assert calls.count("pytest") == calls.count("cleanup") == 1
    result = json.loads((tmp_path / "result.json").read_text())
    assert result["exit_code"] == code
    assert result["profiles"][0]["exit_code"] == code
    artifacts = directory / ("events" if code == 0 else "failure-events")
    status = artifacts / "pytest-exit-code"
    if pytest_code is None:
        assert not status.exists()
        assert result["profiles"][0]["stage"] == "tests"
        assert result["profiles"][0]["error"] == "CommandFailed"
    else:
        assert int(status.read_text()) == pytest_code


@pytest.mark.parametrize(
    "source,pytest_code,code",
    [
        ("def test_result(): pass\n", 0, 0),
        ("def test_result(): assert False\n", 1, 42),
        ("def test_result(:\n", 2, 2),
    ],
)
def test_entrypoint_uses_actual_pytest_status(tmp_path, source, pytest_code, code):
    test = tmp_path / "test_result.py"
    test.write_text(source)
    status = tmp_path / "pytest-exit-code"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            run.PYTEST_ENTRYPOINT,
            str(status),
            "-q",
            "-p",
            "no:cacheprovider",
            str(test),
        ],
        cwd=tmp_path,
        env={
            **os.environ,
            "PYTEST_ADDOPTS": "",
            "PYTEST_PLUGINS": "",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        },
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == code, result.stdout + result.stderr
    assert int(status.read_text()) == pytest_code


@pytest.mark.parametrize("changed", ["id", "created", "labels", "tags", None])
def test_image_cleanup_checks_exact_identity_and_ownership(
    tmp_path, monkeypatch, changed
):
    expected = {
        "id": "sha256:owned",
        "created": "created-time",
        "tags": ["owned:run"],
        "labels": {"com.ray.hive-auth.run": "run-id"},
    }
    current = dict(expected)
    if changed:
        current[changed] = None if changed in {"labels", "tags"} else "changed"
    monkeypatch.setattr(run, "image_info", lambda *a: current)
    calls = []
    monkeypatch.setattr(run, "command", lambda args, *a, **kw: calls.append(args))
    if changed:
        with pytest.raises(ValueError, match="ownership changed"):
            run.remove_owned_image("owned:run", expected, "run-id", tmp_path, {})
        assert not calls
    else:
        run.remove_owned_image("owned:run", expected, "run-id", tmp_path, {})
        assert calls == [["docker", "image", "rm", "owned:run"]]


def test_dangling_cleanup_is_scoped_to_current_run(tmp_path, monkeypatch):
    calls = []

    def command(args, path, **kwargs):
        calls.append(args)
        if "ls" in args:
            assert "label=com.ray.hive-auth.run=run-id" in args
            path.write_text("sha256:owned\n")

    monkeypatch.setattr(run, "command", command)
    monkeypatch.setattr(
        run,
        "image_info",
        lambda *a: {
            "id": "sha256:owned",
            "created": "created-time",
            "tags": None,
            "labels": {"com.ray.hive-auth.run": "run-id"},
        },
    )
    run.remove_owned_dangling("run-id", tmp_path, {})
    assert calls[-1] == ["docker", "image", "rm", "sha256:owned"]


def test_timeout_stops_child_group_before_returning(tmp_path, monkeypatch):
    calls = []
    process = SimpleNamespace(pid=123)
    outcomes = iter(
        [subprocess.TimeoutExpired("test", 1), subprocess.TimeoutExpired("test", 5), 0]
    )

    def wait(**kwargs):
        value = next(outcomes)
        if isinstance(value, Exception):
            raise value
        return value

    process.wait = wait
    monkeypatch.setattr(run.subprocess, "Popen", lambda *a, **kw: process)
    monkeypatch.setattr(run.os, "killpg", lambda pid, sig: calls.append((pid, sig)))
    with pytest.raises(subprocess.TimeoutExpired):
        run.command(["test"], tmp_path / "process.log", env={}, timeout=1)
    assert calls == [(123, signal.SIGTERM), (123, signal.SIGKILL)]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
