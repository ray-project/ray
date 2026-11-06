import sys

import pytest

from ray.train.v2._internal.callbacks import nccl_ras
from ray.train.v2._internal.callbacks.nccl_ras import (
    DiagnosticResult,
    FlightRecorderProbe,
    NCCLRASProbe,
    NCCLRASTextProbe,
    NvidiaSmiProbe,
    RASQueryError,
    StackTraceProbe,
)


def _fake_ncclras(monkeypatch, results):
    """Make ``run_ncclras`` return the next of ``results``, and record its
    calls."""
    results = iter(results)
    calls = []

    def run_ncclras(binary_path, timeout_s, fmt, ras_addr):
        calls.append((fmt, ras_addr))
        return next(results)

    monkeypatch.setattr(nccl_ras, "run_ncclras", run_ncclras)
    return calls


def test_ncclras_probes_return_the_report_as_detail(monkeypatch):
    calls = _fake_ncclras(
        monkeypatch,
        [{"ok": True, "stdout": "{}"}, {"ok": True, "stdout": "report"}],
    )
    assert NCCLRASProbe("host:1").poll().detail == "{}"
    assert NCCLRASTextProbe("host:1").poll().detail == "report"
    assert calls == [("json", "host:1"), ("text", "host:1")]


def test_ncclras_probe_stops_querying_after_a_fatal_error(monkeypatch):
    calls = _fake_ncclras(monkeypatch, [{"ok": False, "reason": "binary_not_found"}])
    probe = NCCLRASProbe()
    first, second = probe.poll(), probe.poll()
    assert isinstance(first.error, RASQueryError) and first.error.fatal
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
    assert not probe.poll().error.fatal
    assert probe.poll().detail == "{}"


@pytest.mark.parametrize(
    "probe, function",
    [
        (NvidiaSmiProbe(), "run_nvidia_smi"),
        (FlightRecorderProbe(), "dump_flight_recorder"),
    ],
)
def test_diagnostic_probes_return_the_output_or_the_error(monkeypatch, probe, function):
    monkeypatch.setattr(
        nccl_ras, function, lambda *args: DiagnosticResult(value="output")
    )
    assert probe.poll().detail == "output"

    error = RuntimeError("no GPUs")
    monkeypatch.setattr(nccl_ras, function, lambda *args: DiagnosticResult(error=error))
    assert probe.poll().error is error


def test_stack_trace_probe_returns_the_stacks(monkeypatch):
    monkeypatch.setattr(nccl_ras, "dump_stack_trace", lambda timeout_s: "stacks")
    assert StackTraceProbe().poll().detail == "stacks"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
