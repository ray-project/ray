"""Tests of the NCCL hang detector's probes, in ``ray.train.health.nccl_ras``.

Most are copies of ``test_nccl_ras_callback.py``'s, which check the same
queries in ``NCCLRASCallback``'s module.
"""
import subprocess
import sys
import threading
import types

import pytest

from ray.train.health.nccl_ras import probes, utils
from ray.train.health.nccl_ras.probes import (
    FlightRecorderProbe,
    NCCLRASProbe,
    NCCLRASTextProbe,
    NvidiaSmiProbe,
    StackTraceProbe,
)
from ray.train.health.nccl_ras.utils import RASQueryError, parse_ras_addr, run_ncclras

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

    monkeypatch.setattr(subprocess, "run", fake_run)
    return calls


@pytest.fixture
def fake_c10d(monkeypatch):
    """Put a fake ``torch._C._distributed_c10d`` in front of the real torch.

    ``FlightRecorderProbe`` imports it when it polls, so these tests exercise
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
def unanswered_sudo_check(monkeypatch):
    """Every test starts with the passwordless-sudo check not yet run."""
    monkeypatch.setattr(utils, "_passwordless_sudo", None)


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

    assert run_ncclras("json", 8.0, ras_addr) == "{}"
    ((cmd, kwargs),) = calls
    assert cmd == ["ncclras", "-f", "json", "-t", "8"] + address_args
    assert kwargs["timeout"] == 8.0


@pytest.mark.parametrize(
    "run_result,reason,fatal",
    [
        ({"side_effect": FileNotFoundError("ncclras")}, "binary_not_found", True),
        (
            {"returncode": 1, "stderr": "ncclras: invalid option -- 'f'"},
            "unsupported_f_option",
            True,
        ),
        ({"returncode": 3, "stderr": "boom"}, "exit_3", False),
        (
            {"side_effect": subprocess.TimeoutExpired("ncclras", 8.0)},
            "timed out (8.0)",
            False,
        ),
        ({"stdout": "\n"}, "empty_output", False),
    ],
    ids=["not_installed", "too_old", "non_zero_exit", "timed_out", "empty"],
)
def test_run_ncclras_failures_say_why(monkeypatch, run_result, reason, fatal):
    fake_subprocess_run(monkeypatch, **run_result)

    with pytest.raises(RASQueryError) as error:
        run_ncclras("json", 8.0)

    assert error.value.reason == reason
    assert error.value.fatal is fatal


def _fake_ncclras(monkeypatch, results):
    """Make ``run_ncclras`` return or raise the next of ``results``, and
    record its calls."""
    results = iter(results)
    calls = []

    def fake_run_ncclras(fmt, timeout_s, ras_addr):
        calls.append((fmt, timeout_s, ras_addr))
        result = next(results)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(probes, "run_ncclras", fake_run_ncclras)
    return calls


def test_ncclras_probes_return_the_report_as_detail(monkeypatch):
    # NCCL_RAS_ADDR is read where the probe is created: on the controller,
    # which has the driver's.
    monkeypatch.setenv("NCCL_RAS_ADDR", "host:1")
    calls = _fake_ncclras(monkeypatch, ["{}", "report"])
    json_probe, text_probe = NCCLRASProbe(), NCCLRASTextProbe()
    monkeypatch.delenv("NCCL_RAS_ADDR")

    assert json_probe.poll().detail == "{}"
    assert text_probe.poll().detail == "report"
    # ncclras gets the probe's timeout_s, the callback's 8s.
    assert calls == [("json", 8.0, "host:1"), ("text", 8.0, "host:1")]


@pytest.mark.parametrize("reason", ["binary_not_found", "unsupported_f_option"])
def test_ncclras_probe_stops_querying_after_a_fatal_error(monkeypatch, reason):
    calls = _fake_ncclras(monkeypatch, [RASQueryError(reason, fatal=True)])
    probe = NCCLRASProbe()

    with pytest.raises(RASQueryError) as error:
        probe.poll()
    assert probe.poll().error is error.value
    assert len(calls) == 1


def test_ncclras_probe_queries_again_after_a_transient_error(monkeypatch):
    _fake_ncclras(monkeypatch, [RASQueryError("exit_1", stderr="boom"), "{}"])
    probe = NCCLRASProbe()

    with pytest.raises(RASQueryError, match="boom"):
        probe.poll()
    assert probe.poll().detail == "{}"


# ----------------------------------------------------------------------
# nvidia-smi
# ----------------------------------------------------------------------
def test_nvidia_smi_returns_the_report(monkeypatch):
    calls = fake_subprocess_run(monkeypatch, stdout="GPU 00000000:00:04.0\n")

    assert NvidiaSmiProbe().poll().detail == "GPU 00000000:00:04.0\n"
    ((cmd, kwargs),) = calls
    assert cmd == ["nvidia-smi", "-q"]
    # The driver is what might be wedged, so the call is always bounded.
    assert kwargs["timeout"] == NvidiaSmiProbe.timeout_s


@pytest.mark.parametrize(
    "run_result,expected_type,expected_message",
    [
        (
            {"side_effect": FileNotFoundError("nvidia-smi")},
            FileNotFoundError,
            "nvidia-smi",
        ),
        (
            {"side_effect": subprocess.TimeoutExpired("nvidia-smi", 30.0)},
            TimeoutError,
            "timed out after 30s",
        ),
        (
            {"returncode": 9, "stderr": "driver/library mismatch"},
            RuntimeError,
            "driver/library",
        ),
    ],
    ids=["not_installed", "driver_stuck", "non_zero_exit"],
)
def test_nvidia_smi_failures_say_why(
    monkeypatch, run_result, expected_type, expected_message
):
    fake_subprocess_run(monkeypatch, **run_result)

    error = NvidiaSmiProbe().poll().error
    assert isinstance(error, expected_type)
    assert expected_message in str(error)


# ----------------------------------------------------------------------
# Flight Recorder
# ----------------------------------------------------------------------
def test_flight_recorder_dumps_the_nccl_buffer(fake_c10d):
    # ProcessGroupNCCL records into its own buffer; the generic one has none of
    # its collectives.
    fake_c10d._dump_nccl_trace_json = lambda *args, **kwargs: b'{"entries": [1]}'
    fake_c10d._dump_fr_trace_json = lambda *args, **kwargs: b"{}"

    assert FlightRecorderProbe().poll().detail == '{"entries": [1]}'


def test_flight_recorder_falls_back_to_the_generic_buffer(fake_c10d):
    # Torch builds without NCCL don't bind the NCCL dump.
    fake_c10d._dump_fr_trace_json = lambda *args, **kwargs: b'{"entries": []}'

    assert FlightRecorderProbe().poll().detail == '{"entries": []}'


def test_flight_recorder_without_torch(monkeypatch, fake_c10d):
    # A None module makes the import fail.
    monkeypatch.setitem(sys.modules, "torch._C", None)
    assert isinstance(FlightRecorderProbe().poll().error, ImportError)


# ----------------------------------------------------------------------
# Stack traces
# ----------------------------------------------------------------------
def test_stack_trace_is_the_py_spy_dump(monkeypatch):
    # The native dump is what shows a rank blocked inside NCCL, so it is
    # returned verbatim rather than summarised.
    calls = fake_subprocess_run(monkeypatch, stdout="native stack")

    assert StackTraceProbe().poll().detail == "native stack"
    ((cmd, kwargs),) = calls
    assert cmd[:2] == ["py-spy", "dump"] and "--native" in cmd
    assert kwargs["timeout"] == StackTraceProbe.timeout_s


@pytest.mark.parametrize(
    "run_result,expected_reason",
    [
        ({"side_effect": FileNotFoundError("py-spy")}, "py-spy not installed"),
        (
            {"side_effect": subprocess.TimeoutExpired("py-spy", 30.0)},
            "py-spy timed out",
        ),
        ({"returncode": 1}, "py-spy exited 1"),
        ({"returncode": 1, "stderr": "Permission Denied"}, "Permission Denied"),
    ],
    ids=["not_installed", "timed_out", "non_zero_exit", "ptrace_denied_no_sudo"],
)
def test_stack_trace_falls_back_to_python(monkeypatch, run_result, expected_reason):
    # py-spy is an optional dependency; without it the native frames are lost
    # but the Python stacks of every thread are still worth having. The sudo
    # check fails too, as when sudo needs a password.
    fake_subprocess_run(monkeypatch, **run_result)

    trace = StackTraceProbe().poll().detail

    assert f"[py-spy unavailable: {expected_reason}; Python-only" in trace
    assert "test_stack_trace_falls_back_to_python" in trace


PY_SPY_PERMISSION_DENIED = "Permission Denied: Try running again with sudo"
_STACK_BUDGET_S = StackTraceProbe.timeout_s


def fake_ptrace_denied(monkeypatch, py_spy_s=0.0):
    """Script ``subprocess.run``: py-spy is denied ptrace after ``py_spy_s``,
    and every ``sudo`` command succeeds. Returns ``(command, timeout)`` of
    each call."""
    clock = [0.0]
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs["timeout"]))
        if cmd[0] == "py-spy":
            clock[0] += py_spy_s
            return subprocess.CompletedProcess(cmd, 1, "", PY_SPY_PERMISSION_DENIED)
        return subprocess.CompletedProcess(cmd, 0, "native stack", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(utils.time, "monotonic", lambda: clock[0])
    return calls


def test_stack_trace_retries_with_sudo_when_ptrace_is_denied(monkeypatch):
    # Restricted ptrace stops an unprivileged py-spy from tracing its parent.
    monkeypatch.setenv("PATH", "/opt/bin")
    calls = fake_ptrace_denied(monkeypatch)

    assert StackTraceProbe().poll().detail == "native stack"
    assert StackTraceProbe().poll().detail == "native stack"

    py_spy, sudo_check, sudo_retry, *rest = [cmd for cmd, _ in calls]
    assert sudo_check == ["sudo", "-n", "true"]
    assert sudo_retry == ["sudo", "-n", "env", "PATH=/opt/bin"] + py_spy
    # Checked once per process.
    assert rest == [py_spy, sudo_retry]


@pytest.mark.parametrize(
    "py_spy_s,timeouts",
    [
        (_STACK_BUDGET_S - 4, [_STACK_BUDGET_S, 4.0, 4.0]),
        (_STACK_BUDGET_S, [_STACK_BUDGET_S]),
    ],
    ids=["time_left", "no_time_left"],
)
def test_stack_trace_sudo_retry_stays_within_the_timeout(
    monkeypatch, py_spy_s, timeouts
):
    calls = fake_ptrace_denied(monkeypatch, py_spy_s)

    StackTraceProbe().poll()

    assert [timeout for _, timeout in calls] == timeouts


def test_python_fallback_names_each_thread(monkeypatch):
    fake_subprocess_run(monkeypatch, returncode=1)
    stop = threading.Event()
    worker = threading.Thread(target=stop.wait, name=TRAINING_THREAD_NAME)
    worker.start()

    try:
        trace = StackTraceProbe().poll().detail
    finally:
        stop.set()
        worker.join()

    main = threading.main_thread()
    assert f"# Thread {worker.ident} ({TRAINING_THREAD_NAME})" in trace
    assert f"# Thread {main.ident} ({main.name})" in trace


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
