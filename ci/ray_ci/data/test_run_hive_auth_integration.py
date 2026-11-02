"""Check the security runner's collection, exit and cleanup boundaries."""

import os
import signal
import subprocess
import sys
from types import SimpleNamespace
from xml.etree.ElementTree import Element, SubElement, tostring

import pytest

from ci.ray_ci.data.hive_auth import run


def results(path, profile="GT", damage=None):
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
def test_exact_case_list(tmp_path, profile):
    path = tmp_path / "results.xml"
    results(path, profile)
    assert run.validate_results(path, profile) == len(run.CASES[profile])


@pytest.mark.parametrize(
    "damage", ["empty", "missing", "extra", "duplicate", "failure", "error", "skipped"]
)
def test_invalid_results(tmp_path, damage):
    path = tmp_path / "results.xml"
    results(path, damage=damage)
    with pytest.raises(ValueError):
        run.validate_results(path, "GT")


@pytest.mark.parametrize(
    "failure,code",
    [
        (None, 0),
        ("start", 1),
        ("ready", 1),
        ("exec", 1),
        ("pytest", 42),
        ("collection", 2),
        ("interrupt", 1),
        ("timeout", 1),
        ("copy", 1),
        ("cleanup", 7),
        ("malformed", 1),
        ("status_missing", 1),
        ("status_wrong", 1),
    ],
)
def test_main_preserves_status_and_cleans_own_project(
    tmp_path, monkeypatch, failure, code
):
    calls = []
    monkeypatch.setenv("BUILDKITE_RETRY_COUNT", "1")
    monkeypatch.setenv("HTTP_PROXY", "must-be-removed")

    def wait(*args):
        if failure == "ready":
            raise TimeoutError("Not ready")

    monkeypatch.setattr(run, "wait_for_probe", wait)

    def command(args, log, *, env, timeout, check=True):
        calls.append(log.stem)
        assert 0 < timeout <= 1200
        assert not any(k in env for k in run.PROXIES)
        assert args[:2] == ["docker", "compose"]
        assert args[args.index("-p") + 1].startswith("ray-hive-auth-")
        if log.stem == failure:
            raise run.CommandFailed(failure, code)
        if log.stem == "pytest":
            assert run.PYTEST_ENTRYPOINT in args
            if failure == "interrupt":
                raise KeyboardInterrupt
            if failure == "timeout":
                raise subprocess.TimeoutExpired(args, timeout)
            if failure in {"exec", "pytest", "collection"}:
                raise run.CommandFailed("pytest", code)
        if log.stem == "copy" and failure not in {
            "start",
            "exec",
            "interrupt",
            "timeout",
        }:
            path = tmp_path / env["HIVE_AUTH_PROFILE"] / "artifacts/results.xml"
            results(path, env["HIVE_AUTH_PROFILE"])
            if failure != "status_missing":
                (path.parent / "pytest-exit-code").write_text(
                    str(
                        {"pytest": 1, "collection": 2, "status_wrong": 1}.get(
                            failure, 0
                        )
                    )
                )
            if failure == "malformed":
                path.write_text("<broken")
        if log.stem == "cleanup" and failure == "pytest":
            raise run.CommandFailed("cleanup", 7)  # Must not replace test failure 42.
        return 0

    monkeypatch.setattr(run, "command", command)
    assert (
        run.main(
            [
                "--runtime-image",
                "prepared:fixed",
                "--profiles",
                "GT",
                "N",
                "--log-dir",
                str(tmp_path),
            ]
        )
        == code
    )
    assert calls.count("start") == (2 if failure is None else 1)
    assert calls.count("cleanup") == calls.count("start")
    assert calls.count("pytest") == (
        2 if failure is None else (0 if failure in {"start", "ready"} else 1)
    )
    if failure == "exec":
        assert not (tmp_path / "GT/artifacts/pytest-exit-code").exists()


@pytest.mark.parametrize("invalid", ["duplicate", "previous", "timeout"])
def test_rejects_invalid_invocation_before_docker(tmp_path, monkeypatch, invalid):
    args = ["--runtime-image", "prepared:fixed", "--log-dir", str(tmp_path)]
    if invalid == "previous":
        (tmp_path / "evidence").write_text("preserve")
    args += ["--profiles", "G", "G"] if invalid == "duplicate" else []
    args += ["--profile-timeout", "0"] if invalid == "timeout" else []
    monkeypatch.setattr(
        run, "command", lambda *a, **kw: pytest.fail("Docker must not run")
    )
    with pytest.raises(ValueError):
        run.main(args)


@pytest.mark.parametrize(
    "source,raw,code",
    [
        ("def test_result(): pass\n", 0, 0),
        ("def test_result(): assert False\n", 1, 42),
        ("def test_result(:\n", 2, 2),
    ],
)
def test_real_pytest_status(tmp_path, source, raw, code):
    test = tmp_path / "test_result.py"
    test.write_text(source)
    status = tmp_path / "pytest-exit-code"
    log = tmp_path / "pytest.log"
    try:
        actual = run.command(
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
            log,
            timeout=30,
            env={
                **os.environ,
                "PYTEST_ADDOPTS": "",
                "PYTEST_PLUGINS": "",
                "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            },
        )
    except run.CommandFailed as exc:
        actual = exc.code
    assert actual == code, log.read_text()
    assert int(status.read_text()) == raw


def test_timeout_stops_child_group(tmp_path, monkeypatch):
    signals = []
    outcomes = iter(
        [subprocess.TimeoutExpired("test", 1), subprocess.TimeoutExpired("test", 5), 0]
    )
    process = SimpleNamespace(pid=123, returncode=-9)

    def wait(**kwargs):
        value = next(outcomes)
        if isinstance(value, Exception):
            raise value
        return value

    process.wait = wait
    monkeypatch.setattr(run.subprocess, "Popen", lambda *a, **kw: process)
    monkeypatch.setattr(run.os, "killpg", lambda pid, sig: signals.append((pid, sig)))
    with pytest.raises(subprocess.TimeoutExpired):
        run.command(["test"], tmp_path / "process.log", env={}, timeout=1)
    assert signals == [(123, signal.SIGTERM), (123, signal.SIGKILL)]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
