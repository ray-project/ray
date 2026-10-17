"""Unit tests for the Python export event logger (no Ray cluster required)."""
import json
import sys

import pytest

from ray._private import ray_constants
from ray._private.event import export_event_logger
from ray._private.event.export_event_logger import (
    EventLogType,
    check_export_api_enabled,
    get_export_event_logger,
)
from ray.core.generated.export_event_pb2 import ExportEvent
from ray.core.generated.export_train_nccl_hang_pb2 import (
    ExportTrainNcclHangEventData,
)
from ray.core.generated.export_train_state_pb2 import ExportTrainRunEventData


@pytest.fixture
def fresh_export_loggers(monkeypatch):
    """Isolate the process-wide export logger cache, and close the file
    handlers the test creates so the next test starts from scratch."""
    monkeypatch.setattr(export_event_logger, "_export_event_logger", {})
    yield
    for adapter in export_event_logger._export_event_logger.values():
        for handler in adapter.logger.handlers[:]:
            handler.close()
            adapter.logger.removeHandler(handler)


def _read_events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def _nccl_hang_event(**overrides) -> ExportTrainNcclHangEventData:
    Data = ExportTrainNcclHangEventData
    fields = dict(
        schema_version=1,
        run_id="run",
        attempt_id="attempt",
        poll_index=44,
        timestamp_ms=1760000660123,
        detector_status="ACTIVE",
        triggers=["CONFIRMED", "SUSPECTED"],
        message="NCCL hang detected",
        communicators=[
            Data.CommunicatorState(
                communicator_id="0x8a3f1c2b",
                state="CONFIRMED",
                stalled_since_ms=1760000060123,
                stall_duration_s=600.0,
                stalled_ranks=[
                    Data.StalledRanks(
                        op="AllReduce", launch_count=622, world_ranks=[0, 1, 3]
                    ),
                    Data.StalledRanks(
                        op="AllReduce", launch_count=621, world_ranks=[2]
                    ),
                ],
            ),
            Data.CommunicatorState(
                communicator_id="0x11aa22bb",
                state="SUSPECTED",
                stalled_since_ms=1760000600123,
                stall_duration_s=60.0,
            ),
        ],
    )
    fields.update(overrides)
    return Data(**fields)


def test_nccl_hang_event_written_to_its_own_file(tmp_path, fresh_export_loggers):
    # The hang events must not land in the train state file: consumers of
    # that file only know the train run and attempt event types.
    train_state_logger = get_export_event_logger(EventLogType.TRAIN_STATE, tmp_path)
    nccl_hang_logger = get_export_event_logger(EventLogType.TRAIN_NCCL_HANG, tmp_path)
    assert train_state_logger is not nccl_hang_logger

    nccl_hang_logger.send_event(_nccl_hang_event())

    event_dir = tmp_path / "export_events"
    [event] = _read_events(event_dir / "event_EXPORT_TRAIN_NCCL_HANG.log")
    assert event["source_type"] == "EXPORT_TRAIN_NCCL_HANG"
    assert isinstance(event["timestamp"], int)
    assert (event_dir / "event_EXPORT_TRAIN_STATE.log").read_text() == ""


def test_nccl_hang_event_json_shape(tmp_path, fresh_export_loggers):
    """The JSON line is a cross-repo contract: int64 fields are strings, unset
    optional fields are omitted and empty repeated fields are printed."""
    logger = get_export_event_logger(EventLogType.TRAIN_NCCL_HANG, tmp_path)
    logger.send_event(
        _nccl_hang_event(ras_text_report="RAS report", diagnostics_dir="s3://b/hd")
    )
    logger.send_event(
        _nccl_hang_event(
            poll_index=45,
            detector_status="STOPPED",
            stop_reason="worker_group_shutdown",
            triggers=[],
            message="",
            communicators=[],
        )
    )

    active, stopped = [
        event["event_data"]
        for event in _read_events(
            tmp_path / "export_events" / "event_EXPORT_TRAIN_NCCL_HANG.log"
        )
    ]
    assert active == {
        "schema_version": 1,
        "run_id": "run",
        "attempt_id": "attempt",
        "poll_index": "44",
        "timestamp_ms": "1760000660123",
        "detector_status": "ACTIVE",
        "triggers": ["CONFIRMED", "SUSPECTED"],
        "message": "NCCL hang detected",
        "ras_text_report": "RAS report",
        "diagnostics_dir": "s3://b/hd",
        "communicators": [
            {
                "communicator_id": "0x8a3f1c2b",
                "state": "CONFIRMED",
                "stalled_since_ms": "1760000060123",
                "stall_duration_s": 600.0,
                "stalled_ranks": [
                    {
                        "op": "AllReduce",
                        "launch_count": "622",
                        "world_ranks": [0, 1, 3],
                    },
                    {"op": "AllReduce", "launch_count": "621", "world_ranks": [2]},
                ],
            },
            {
                "communicator_id": "0x11aa22bb",
                "state": "SUSPECTED",
                "stalled_since_ms": "1760000600123",
                "stall_duration_s": 60.0,
                "stalled_ranks": [],
            },
        ],
    }
    assert stopped == {
        "schema_version": 1,
        "run_id": "run",
        "attempt_id": "attempt",
        "poll_index": "45",
        "timestamp_ms": "1760000660123",
        "detector_status": "STOPPED",
        "stop_reason": "worker_group_shutdown",
        "triggers": [],
        "message": "",
        "communicators": [],
    }


def test_misrouted_event_is_flagged(tmp_path, fresh_export_loggers, monkeypatch):
    # The log type of a logger is a contract with its consumers, so writing a
    # train run event to the hang file is reported as a bug.
    errors = []
    monkeypatch.setattr(export_event_logger.global_logger, "error", errors.append)
    logger = get_export_event_logger(EventLogType.TRAIN_NCCL_HANG, tmp_path)
    logger.send_event(ExportTrainRunEventData(id="run"))
    assert len(errors) == 1 and "EXPORT_TRAIN_NCCL_HANG" in errors[0]


@pytest.mark.parametrize(
    "write_all,write_config,expected",
    [
        (False, [""], False),
        (False, ["EXPORT_TRAIN_RUN", "EXPORT_TRAIN_RUN_ATTEMPT"], False),
        (False, ["EXPORT_TRAIN_NCCL_HANG"], True),
        (False, ["EXPORT_TRAIN_RUN", "EXPORT_TRAIN_NCCL_HANG"], True),
        (True, [""], True),
    ],
)
def test_check_export_api_enabled_for_nccl_hang(
    monkeypatch, write_all, write_config, expected
):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_EXPORT_API_WRITE", write_all)
    monkeypatch.setattr(
        ray_constants, "RAY_ENABLE_EXPORT_API_WRITE_CONFIG", write_config
    )
    assert (
        check_export_api_enabled(ExportEvent.SourceType.EXPORT_TRAIN_NCCL_HANG)
        is expected
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
