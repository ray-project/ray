"""Tests of ``ray.train.health.probes.nccl_ras``.

The tests of its helpers are copies of ``test_nccl_ras_callback.py``'s, which
check the same functions in ``NCCLRASCallback``'s module.
"""
import subprocess
import sys
import threading
import types

import pytest

from ray.train.health import ProbeResult
from ray.train.health.probes import nccl_ras
from ray.train.health.probes.nccl_ras import (
    FlightRecorderProbe,
    NCCLRASProbe,
    NCCLRASTextProbe,
    NvidiaSmiProbe,
    RASQueryError,
    StackTraceProbe,
    dump_flight_recorder,
    dump_stack_trace,
    parse_ras_addr,
    run_ncclras,
    run_nvidia_smi,
)

# Named like Ray Train's worker thread (``ThreadRunner``) so the test reads
# like a real dump.
TRAINING_THREAD_NAME = "TrainingThread(train_func)"


def fake_subprocess_run(monkeypatch, **run_result):
    """Script ``subprocess.run``, recording each call.

    ``run_result`` is either ``side_effect`` (raised) or the ``returncode``,
    ``stdout`` and ``stderr`` of the completed process.
    """
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        if "side_effect" in run_result:
            raise run_result["side_effect"]
        return subprocess.CompletedProcess(
            cmd,
            run_result.get("returncode", 0),
            stdout=run_result.get("stdout", ""),
            stderr=run_result.get("stderr", ""),
        )

    monkeypatch.setattr(nccl_ras.subprocess, "run", fake_run)
    return calls


@pytest.fixture
def fake_c10d(monkeypatch):
    """Put a fake ``torch._C._distributed_c10d`` in front of the real torch.

    ``dump_flight_recorder`` imports it when it runs, so these tests exercise
    the dump whether or not torch is installed.
    """
    c10d = types.ModuleType("torch._C._distributed_c10d")
    torch_c = types.ModuleType("torch._C")
    torch_c._distributed_c10d = c10d
    monkeypatch.setitem(
        sys.modules, "torch", sys.modules.get("torch") or types.ModuleType("torch")
    )
    monkeypatch.setitem(sys.modules, "torch._C", torch_c)
    monkeypatch.setitem(sys.modules, "torch._C._distributed_c10d", c10d)
    return c10d


@pytest.fixture(autouse=True)
def unanswered_sudo_check():
    """Every test starts with the passwordless-sudo check not yet run."""
    previous = nccl_ras._passwordless_sudo
    nccl_ras._passwordless_sudo = None
    yield
    nccl_ras._passwordless_sudo = previous


# ----------------------------------------------------------------------
# ncclras
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "addr,expected",
    [
        ("host:9000", ("host", 9000)),
        ("[::1]:28028", ("::1", 28028)),
    ],
)
def test_parse_ras_addr_valid(addr, expected):
    assert parse_ras_addr(addr) == expected


@pytest.mark.parametrize(
    "addr",
    ["myhost", "", "host:notaport", "[::1", "[::1]:bad"],
)
def test_parse_ras_addr_malformed_raises(addr):
    with pytest.raises(ValueError):
        parse_ras_addr(addr)


@pytest.mark.parametrize(
    "ras_addr,address_args",
    [(None, []), ("[::1]:28028", ["-h", "::1", "-p", "28028"])],
    ids=["default_address", "given_address"],
)
def test_run_ncclras_returns_the_output(monkeypatch, ras_addr, address_args):
    calls = fake_subprocess_run(monkeypatch, stdout="{}")

    assert run_ncclras("ncclras", 8.0, "json", ras_addr) == {
        "ok": True,
        "stdout": "{}",
    }
    ((cmd, kwargs),) = calls
    assert cmd == ["ncclras", "-f", "json", "-t", "8"] + address_args
    assert kwargs["timeout"] == 8.0


@pytest.mark.parametrize(
    "run_result,expected",
    [
        ({"side_effect": FileNotFoundError("ncclras")}, {"reason": "binary_not_found"}),
        (
            {"returncode": 1, "stderr": "ncclras: invalid option -- 'f'"},
            {"reason": "unsupported_f_option"},
        ),
        ({"returncode": 3, "stderr": "boom"}, {"reason": "exit_3", "stderr": "boom"}),
        (
            {"side_effect": subprocess.TimeoutExpired("ncclras", 8.0)},
            {"reason": "timed out (8.0)"},
        ),
    ],
    ids=["not_installed", "too_old", "non_zero_exit", "timed_out"],
)
def test_run_ncclras_failures_say_why(monkeypatch, run_result, expected):
    fake_subprocess_run(monkeypatch, **run_result)

    result = run_ncclras("ncclras", 8.0)

    assert result["ok"] is False
    assert {key: result.get(key) for key in expected} == expected


def _fake_ncclras(monkeypatch, results):
    """Make ``run_ncclras`` return the next of ``results``, and record its
    calls."""
    results = iter(results)
    calls = []

    def fake_run_ncclras(binary_path, timeout_s, fmt, ras_addr):
        calls.append((fmt, ras_addr))
        return next(results)

    monkeypatch.setattr(nccl_ras, "run_ncclras", fake_run_ncclras)
    return calls


def test_ncclras_probes_return_the_report_as_detail(monkeypatch):
    calls = _fake_ncclras(
        monkeypatch,
        [{"ok": True, "stdout": "{}"}, {"ok": True, "stdout": "report"}],
    )
    assert NCCLRASProbe("host:1").poll().detail == "{}"
    assert NCCLRASTextProbe("host:1").poll().detail == "report"
    assert calls == [("json", "host:1"), ("text", "host:1")]


def test_ncclras_probe_defaults_to_this_process_ras_addr(monkeypatch):
    monkeypatch.setenv("NCCL_RAS_ADDR", "host:2")
    calls = _fake_ncclras(monkeypatch, [{"ok": True, "stdout": "{}"}])
    NCCLRASProbe().poll()
    assert calls == [("json", "host:2")]


@pytest.mark.parametrize("reason", ["binary_not_found", "unsupported_f_option"])
def test_ncclras_probe_stops_querying_after_a_fatal_error(monkeypatch, reason):
    calls = _fake_ncclras(monkeypatch, [{"ok": False, "reason": reason}])
    probe = NCCLRASProbe()
    first, second = probe.poll(), probe.poll()
    assert isinstance(first.error, RASQueryError) and first.error.fatal
    assert first.error.reason == reason
    assert second.error is first.error
    assert len(calls) == 1


def test_ncclras_probe_queries_again_after_a_transient_error(monkeypatch):
    _fake_ncclras(
        monkeypatch,
        [
            {"ok": False, "reason": "exit_1", "stderr": "boom"},
            {"ok": True, "stdout": "{}"},
        ],
    )
    probe = NCCLRASProbe()
    error = probe.poll().error
    assert not error.fatal and "boom" in str(error)
    assert probe.poll().detail == "{}"


# ----------------------------------------------------------------------
# nvidia-smi
# ----------------------------------------------------------------------
def test_nvidia_smi_returns_the_report(monkeypatch):
    calls = fake_subprocess_run(monkeypatch, stdout="GPU 00000000:00:04.0\n")

    assert run_nvidia_smi(25.0) == ProbeResult(detail="GPU 00000000:00:04.0\n")
    ((cmd, kwargs),) = calls
    assert cmd == ["nvidia-smi", "-q"]
    # The driver is what might be wedged, so the call is always bounded.
    assert kwargs["timeout"] == 25.0


@pytest.mark.parametrize(
    "run_result,expected_type,expected_message",
    [
        (
            {"side_effect": FileNotFoundError("nvidia-smi")},
            FileNotFoundError,
            "nvidia-smi",
        ),
        (
            {"side_effect": subprocess.TimeoutExpired("nvidia-smi", 25.0)},
            TimeoutError,
            "timed out after 25s",
        ),
        ({"side_effect": OSError("no permission")}, OSError, "no permission"),
        (
            {"returncode": 9, "stderr": "driver/library mismatch"},
            RuntimeError,
            "driver/library",
        ),
    ],
    ids=["not_installed", "driver_stuck", "os_error", "non_zero_exit"],
)
def test_nvidia_smi_failures_say_why(
    monkeypatch, run_result, expected_type, expected_message
):
    fake_subprocess_run(monkeypatch, **run_result)

    result = run_nvidia_smi(25.0)

    assert result.detail is None
    assert isinstance(result.error, expected_type)
    assert expected_message in str(result.error)


def test_nvidia_smi_probe_returns_the_report(monkeypatch):
    fake_subprocess_run(monkeypatch, stdout="report")
    assert NvidiaSmiProbe().poll().detail == "report"


# ----------------------------------------------------------------------
# Flight Recorder
# ----------------------------------------------------------------------
def test_flight_recorder_dumps_the_nccl_buffer(fake_c10d):
    # ProcessGroupNCCL records into its own buffer; the generic one has none of
    # its collectives.
    fake_c10d._dump_nccl_trace_json = lambda *args, **kwargs: b'{"entries": [1]}'
    fake_c10d._dump_fr_trace_json = lambda *args, **kwargs: b"{}"

    assert dump_flight_recorder() == ProbeResult(detail='{"entries": [1]}')


def test_flight_recorder_falls_back_to_the_generic_buffer(fake_c10d):
    # Torch builds without NCCL don't bind the NCCL dump.
    fake_c10d._dump_fr_trace_json = lambda *args, **kwargs: b'{"entries": []}'

    assert dump_flight_recorder() == ProbeResult(detail='{"entries": []}')


def _raise_dump_failed(*args, **kwargs):
    raise RuntimeError("dump failed")


@pytest.mark.parametrize(
    "torch_c,dump_fn,expected_type,expected_message",
    [
        # A None module makes the import fail.
        (None, None, ImportError, "torch._C"),
        ("fake", _raise_dump_failed, RuntimeError, "dump failed"),
    ],
    ids=["no_torch", "dump_raises"],
)
def test_flight_recorder_dump_failures(
    monkeypatch, fake_c10d, torch_c, dump_fn, expected_type, expected_message
):
    if torch_c is None:
        monkeypatch.setitem(sys.modules, "torch._C", None)
    else:
        fake_c10d._dump_nccl_trace_json = dump_fn

    result = dump_flight_recorder()

    assert result.detail is None
    assert isinstance(result.error, expected_type)
    assert expected_message in str(result.error)


def test_flight_recorder_probe_returns_the_dump(fake_c10d):
    fake_c10d._dump_nccl_trace_json = lambda *args, **kwargs: b"{}"
    assert FlightRecorderProbe().poll().detail == "{}"


# ----------------------------------------------------------------------
# Stack traces
# ----------------------------------------------------------------------
PY_SPY_PERMISSION_DENIED = (
    "Permission Denied: Try running again with elevated permissions by going "
    "'sudo env \"PATH=$PATH\" !!'"
)
SUDO_CHECK_CMD = ["sudo", "-n", "true"]
NATIVE_STACK = "native stack"


def fake_py_spy_runs(monkeypatch, unprivileged, sudo_check=None, sudo_retry=None):
    """Script ``subprocess.run`` for the three commands of a stack dump.

    ``sudo -n true`` gets ``sudo_check``, any other ``sudo`` command gets
    ``sudo_retry`` and the plain py-spy call gets ``unprivileged``; each is a
    ``(returncode, stdout, stderr)`` triple. Returns the recorded commands.
    """
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd == SUDO_CHECK_CMD:
            result = sudo_check
        elif cmd[0] == "sudo":
            result = sudo_retry
        else:
            result = unprivileged
        returncode, stdout, stderr = result
        return subprocess.CompletedProcess(cmd, returncode, stdout, stderr)

    monkeypatch.setattr(nccl_ras.subprocess, "run", fake_run)
    return calls


def test_dump_stack_trace_returns_the_py_spy_dump(monkeypatch):
    # The native dump is what shows a rank blocked inside NCCL, so it is
    # returned verbatim rather than summarised.
    calls = fake_subprocess_run(monkeypatch, stdout=NATIVE_STACK)

    assert dump_stack_trace(25.0) == NATIVE_STACK
    ((cmd, kwargs),) = calls
    assert cmd[:2] == ["py-spy", "dump"] and "--native" in cmd
    assert kwargs["timeout"] == 25.0


@pytest.mark.parametrize(
    "run_result,expected_reason",
    [
        ({"side_effect": FileNotFoundError("py-spy")}, "py-spy not installed"),
        (
            {"side_effect": subprocess.TimeoutExpired("py-spy", 25.0)},
            "py-spy timed out",
        ),
        ({"returncode": 1}, "py-spy exited 1"),
    ],
    ids=["not_installed", "timed_out", "non_zero_exit"],
)
def test_dump_stack_trace_falls_back_to_python(
    monkeypatch, run_result, expected_reason
):
    # py-spy is an optional dependency; without it the native frames are lost
    # but the Python stacks of every thread are still worth having.
    fake_subprocess_run(monkeypatch, **run_result)

    trace = dump_stack_trace(25.0)

    assert f"py-spy unavailable: {expected_reason}" in trace
    assert "test_dump_stack_trace_falls_back_to_python" in trace


def test_dump_stack_trace_does_not_consult_sudo_when_py_spy_succeeds(monkeypatch):
    calls = fake_py_spy_runs(monkeypatch, unprivileged=(0, NATIVE_STACK, ""))

    assert dump_stack_trace(25.0) == NATIVE_STACK
    assert len(calls) == 1
    assert SUDO_CHECK_CMD not in calls


def test_dump_stack_trace_retries_with_sudo_when_ptrace_is_denied(monkeypatch):
    monkeypatch.setenv("PATH", "/opt/bin")
    calls = fake_py_spy_runs(
        monkeypatch,
        unprivileged=(1, "", PY_SPY_PERMISSION_DENIED),
        sudo_check=(0, "", ""),
        sudo_retry=(0, NATIVE_STACK, ""),
    )

    assert dump_stack_trace(25.0) == NATIVE_STACK
    py_spy_cmd, sudo_check_cmd, sudo_retry_cmd = calls
    assert sudo_check_cmd == SUDO_CHECK_CMD
    assert sudo_retry_cmd == ["sudo", "-n", "env", "PATH=/opt/bin"] + py_spy_cmd


def test_dump_stack_trace_falls_back_to_python_without_passwordless_sudo(
    monkeypatch,
):
    calls = fake_py_spy_runs(
        monkeypatch,
        unprivileged=(1, "", PY_SPY_PERMISSION_DENIED),
        sudo_check=(1, "", "sudo: a password is required"),
    )

    trace = dump_stack_trace(25.0)

    assert f"py-spy unavailable: {PY_SPY_PERMISSION_DENIED}; Python-only" in trace
    assert "test_dump_stack_trace_falls_back_to_python_without" in trace
    assert calls[1:] == [SUDO_CHECK_CMD]


def test_dump_stack_trace_checks_sudo_once_per_process(monkeypatch):
    calls = fake_py_spy_runs(
        monkeypatch,
        unprivileged=(1, "", PY_SPY_PERMISSION_DENIED),
        sudo_check=(0, "", ""),
        sudo_retry=(0, NATIVE_STACK, ""),
    )

    dump_stack_trace(25.0)
    dump_stack_trace(25.0)

    assert calls.count(SUDO_CHECK_CMD) == 1


def test_dump_stack_trace_keeps_the_sudo_retry_inside_the_budget(monkeypatch):
    BUDGET_S = 20.0
    FIRST_ATTEMPT_S = 16.0
    clock = [0.0]
    timeouts = []

    def fake_run(cmd, **kwargs):
        timeouts.append(kwargs["timeout"])
        if cmd[0] == "py-spy":
            clock[0] += FIRST_ATTEMPT_S
            return subprocess.CompletedProcess(cmd, 1, "", PY_SPY_PERMISSION_DENIED)
        return subprocess.CompletedProcess(cmd, 0, NATIVE_STACK, "")

    monkeypatch.setattr(nccl_ras.subprocess, "run", fake_run)
    monkeypatch.setattr(nccl_ras.time, "monotonic", lambda: clock[0])

    assert dump_stack_trace(BUDGET_S) == NATIVE_STACK
    first_timeout, sudo_check_timeout, sudo_retry_timeout = timeouts
    assert first_timeout == BUDGET_S
    assert sudo_check_timeout == BUDGET_S - FIRST_ATTEMPT_S
    assert sudo_retry_timeout == BUDGET_S - FIRST_ATTEMPT_S


def test_dump_stack_trace_skips_sudo_when_the_budget_is_spent(monkeypatch):
    BUDGET_S = 20.0
    clock = [0.0]
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        clock[0] += BUDGET_S
        raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])

    monkeypatch.setattr(nccl_ras.subprocess, "run", fake_run)
    monkeypatch.setattr(nccl_ras.time, "monotonic", lambda: clock[0])

    trace = dump_stack_trace(BUDGET_S)

    assert "py-spy unavailable: py-spy timed out; Python-only" in trace
    assert len(calls) == 1


def test_python_fallback_names_each_thread(monkeypatch):
    fake_subprocess_run(monkeypatch, returncode=1)
    stop = threading.Event()
    worker = threading.Thread(target=stop.wait, name=TRAINING_THREAD_NAME)
    worker.start()

    try:
        trace = dump_stack_trace(25.0)
    finally:
        stop.set()
        worker.join()

    main = threading.main_thread()
    assert f"# Thread {worker.ident} ({TRAINING_THREAD_NAME})" in trace
    assert f"# Thread {main.ident} ({main.name})" in trace


def test_stack_trace_probe_returns_the_stacks(monkeypatch):
    monkeypatch.setattr(nccl_ras, "dump_stack_trace", lambda timeout_s: "stacks")
    assert StackTraceProbe().poll().detail == "stacks"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
