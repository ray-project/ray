"""The controller ingest chokepoint dispatches on the WIRE format + the aggregate
mode -- never on the producer flag:

  columnar bytes + aggregate mode -> fast columnar array store
  columnar bytes + simple mode    -> reconstruct -> object store (simple/custom safe)
  cloudpickle bytes               -> decompress  -> object store

Guards the "cover all call-flows" contract: enabling RAY_SERVE_COLUMNAR_METRICS must
never silently zero out autoscaling in the default (simple) mode.
"""
from unittest.mock import MagicMock

import ray.serve._private.controller as controller_mod
from ray.serve._private import autoscaling_metrics_codec as codec
from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentHandleSource,
    DeploymentID,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TimeStampedValue,
)
from ray.serve._private.controller import ServeController
from ray.serve._private.utils import compress_metric_report

DEP = DeploymentID("D", "default")


def _replica_report():
    return ReplicaMetricReport(
        replica_id=ReplicaID("r0", DEP),
        aggregated_metrics={RUNNING_REQUESTS_KEY: 3.0},
        metrics={RUNNING_REQUESTS_KEY: [TimeStampedValue(1.0, 2.0)]},
        timestamp=100.0,
    )


def _handle_report():
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a0",
        handle_source=DeploymentHandleSource.PROXY,
        aggregated_queued_requests=1.0,
        queued_requests=[TimeStampedValue(1.0, 1.0)],
        aggregated_metrics={RUNNING_REQUESTS_KEY: {"D#r0": 3.0}},
        metrics={RUNNING_REQUESTS_KEY: {"D#r0": [TimeStampedValue(1.0, 3.0)]}},
        timestamp=100.0,
    )


def _set_mode(monkeypatch, aggregate):
    monkeypatch.setattr(
        controller_mod, "RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER", aggregate
    )
    monkeypatch.setattr(controller_mod, "RAY_SERVE_ENABLE_DIRECT_INGRESS", False)


# ---------------------------- replica ----------------------------
def test_replica_columnar_aggregate_uses_fast_store(monkeypatch):
    _set_mode(monkeypatch, aggregate=True)
    s = MagicMock()
    ServeController.record_autoscaling_metrics_from_replica(
        s, codec.encode(_replica_report())
    )
    asm = s.autoscaling_state_manager
    asm.record_columnar_metrics_for_replica.assert_called_once()
    asm.record_request_metrics_for_replica.assert_not_called()


def test_replica_columnar_simple_mode_reconstructs(monkeypatch):
    _set_mode(monkeypatch, aggregate=False)
    s = MagicMock()
    ServeController.record_autoscaling_metrics_from_replica(
        s, codec.encode(_replica_report())
    )
    asm = s.autoscaling_state_manager
    asm.record_columnar_metrics_for_replica.assert_not_called()
    asm.record_request_metrics_for_replica.assert_called_once()
    (arg,), _ = asm.record_request_metrics_for_replica.call_args
    assert isinstance(arg, ReplicaMetricReport)
    assert arg.timestamp == 100.0


def test_replica_cloudpickle_uses_object_store(monkeypatch):
    _set_mode(monkeypatch, aggregate=True)  # mode irrelevant for cloudpickle wire
    s = MagicMock()
    ServeController.record_autoscaling_metrics_from_replica(
        s, compress_metric_report(_replica_report())
    )
    asm = s.autoscaling_state_manager
    asm.record_columnar_metrics_for_replica.assert_not_called()
    asm.record_request_metrics_for_replica.assert_called_once()


# ---------------------------- handle -----------------------------
def test_handle_columnar_aggregate_uses_fast_store(monkeypatch):
    _set_mode(monkeypatch, aggregate=True)
    s = MagicMock()
    ServeController.record_autoscaling_metrics_from_handle(
        s, codec.encode(_handle_report())
    )
    asm = s.autoscaling_state_manager
    asm.record_columnar_metrics_for_handle.assert_called_once()
    asm.record_request_metrics_for_handle.assert_not_called()


def test_handle_columnar_simple_mode_reconstructs(monkeypatch):
    _set_mode(monkeypatch, aggregate=False)
    s = MagicMock()
    ServeController.record_autoscaling_metrics_from_handle(
        s, codec.encode(_handle_report())
    )
    asm = s.autoscaling_state_manager
    asm.record_columnar_metrics_for_handle.assert_not_called()
    asm.record_request_metrics_for_handle.assert_called_once()
    (arg,), _ = asm.record_request_metrics_for_handle.call_args
    assert isinstance(arg, HandleMetricReport)


def test_handle_cloudpickle_uses_object_store(monkeypatch):
    _set_mode(monkeypatch, aggregate=True)
    s = MagicMock()
    ServeController.record_autoscaling_metrics_from_handle(
        s, compress_metric_report(_handle_report())
    )
    asm = s.autoscaling_state_manager
    asm.record_columnar_metrics_for_handle.assert_not_called()
    asm.record_request_metrics_for_handle.assert_called_once()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
