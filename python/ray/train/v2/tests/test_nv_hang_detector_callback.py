"""Unit tests for the NCCL RAS hang-detection callback (no GPU required)."""
import asyncio
import contextlib
import itertools
import json
import logging
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, Optional, Union
from unittest.mock import MagicMock

import pytest

from ray.train.v2._internal.callbacks import nv_hang_detector
from ray.train.v2._internal.callbacks.nv_hang_detector import (
    NvHangDetectorCallback,
    RASReport,
    parse_ras_addr,
    parse_ras_schema,
)
from ray.train.v2._internal.constants import (
    NCCL_RAS_ACTION_ENV_VAR,
    NCCL_RAS_ACTION_FAIL,
    NCCL_RAS_ACTION_OBSERVE,
    NCCL_RAS_CONFIRM_WINDOW_S_ENV_VAR,
    NCCL_RAS_POLL_INTERVAL_S_ENV_VAR,
    TORCH_NCCL_TRACE_BUFFER_SIZE_ENV_VAR,
)
from ray.train.v2.api.exceptions import NCCLHangError

HEALTHY_RAS_JSON = """{
  "nccl_version": "2.28.9",
  "cuda_runtime_version": 12090,
  "cuda_driver_version": 13000,
  "timestamp": "2026-06-19 06:51:56",
  "communicators_count": 1,
  "communicators": [
    {
      "hash": "0x514e98cf4e44862b",
      "secondary_hash": "0x2cd6d618b4a75a5a:0xe6854f9ece96c663",
      "size": 2,
      "ranks_count": 2,
      "missing_ranks_count": 0,
      "ranks": [
        {
          "rank": 0,
          "host": "10.0.77.184",
          "pid": 8813,
          "cuda_dev": 0,
          "nvml_dev": 0,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 2
          }
        },
        {
          "rank": 1,
          "host": "10.0.77.184",
          "pid": 8814,
          "cuda_dev": 1,
          "nvml_dev": 1,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 2
          }
        }
      ],
      "missing_ranks": [

      ]
    }
  ],
  "ras": {
    "collection_time_sec": 0.000,
    "timeouts_count": 0
  }
}
"""

DEAD_RANK_RAS_JSON = """{
  "nccl_version": "2.28.9",
  "cuda_runtime_version": 12090,
  "cuda_driver_version": 13000,
  "timestamp": "2026-06-19 06:55:57",
  "communicators_count": 1,
  "communicators": [
    {
      "hash": "0xa1349768e9517ed7",
      "secondary_hash": "0x7cbcd4b24fb45306:0x83984de1c6807cf8",
      "size": 2,
      "ranks_count": 1,
      "missing_ranks_count": 1,
      "ranks": [
        {
          "rank": 0,
          "host": "10.0.77.184",
          "pid": 10018,
          "cuda_dev": 0,
          "nvml_dev": 0,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 4
          }
        }
      ],
      "missing_ranks": [
        {
          "rank": 1,
          "host": "10.0.77.184",
          "pid": 10017,
          "cuda_dev": 1,
          "nvml_dev": 1
          "status": {
            "unresponsive": true,
            "considered_dead": false
          }
        }
      ]
    }
  ],
  "ras": {
    "collection_time_sec": 0.000,
    "timeouts_count": 0
  }
}
"""

MULTI_COMM_RAS_JSON = """{
  "nccl_version": "2.28.9",
  "cuda_runtime_version": 12090,
  "cuda_driver_version": 13000,
  "timestamp": "2026-06-19 06:56:24",
  "communicators_count": 3,
  "communicators": [
    {
      "hash": "0x4e0104c2022fa2f9",
      "secondary_hash": "0x2989420b68927728:0xa3489b479e521019",
      "size": 2,
      "ranks_count": 2,
      "missing_ranks_count": 0,
      "ranks": [
        {
          "rank": 0,
          "host": "10.0.77.184",
          "pid": 10205,
          "cuda_dev": 2,
          "nvml_dev": 2,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 10
          }
        },
        {
          "rank": 1,
          "host": "10.0.77.184",
          "pid": 10207,
          "cuda_dev": 3,
          "nvml_dev": 3,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 3
          }
        }
      ],
      "missing_ranks": [

      ]
    },
    {
      "hash": "0xbeebea5449e0a7e7",
      "secondary_hash": "0x9a74279db0437c16:0x9619aa8eb56f866b",
      "size": 2,
      "ranks_count": 2,
      "missing_ranks_count": 0,
      "ranks": [
        {
          "rank": 0,
          "host": "10.0.77.184",
          "pid": 10206,
          "cuda_dev": 0,
          "nvml_dev": 0,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 10
          }
        },
        {
          "rank": 1,
          "host": "10.0.77.184",
          "pid": 10204,
          "cuda_dev": 1,
          "nvml_dev": 1,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 10
          }
        }
      ],
      "missing_ranks": [

      ]
    },
    {
      "hash": "0xd0d47b5885b3b54b",
      "secondary_hash": "0xac5cb8a1ec16897a:0xa8023b92f14293cf",
      "size": 4,
      "ranks_count": 4,
      "missing_ranks_count": 0,
      "ranks": [
        {
          "rank": 0,
          "host": "10.0.77.184",
          "pid": 10206,
          "cuda_dev": 0,
          "nvml_dev": 0,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 1
          }
        },
        {
          "rank": 1,
          "host": "10.0.77.184",
          "pid": 10204,
          "cuda_dev": 1,
          "nvml_dev": 1,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 1
          }
        },
        {
          "rank": 2,
          "host": "10.0.77.184",
          "pid": 10205,
          "cuda_dev": 2,
          "nvml_dev": 2,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 1
          }
        },
        {
          "rank": 3,
          "host": "10.0.77.184",
          "pid": 10207,
          "cuda_dev": 3,
          "nvml_dev": 3,
          "status": {
            "init_state": 0,
            "async_error": 0,
            "finalize_called": false,
            "destroy_flag": false,
            "abort_flag": false
          },
          "collective_counts": {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 1
          }
        }
      ],
      "missing_ranks": [

      ]
    }
  ],
  "ras": {
    "collection_time_sec": 0.000,
    "timeouts_count": 0
  }
}
"""

# Communicator keys are (hash, secondary_hash) tuples.
_COMM_0X4E01 = ("0x4e0104c2022fa2f9", "0x2989420b68927728:0xa3489b479e521019")
_COMM_DEAD = ("0xa1349768e9517ed7", "0x7cbcd4b24fb45306:0x83984de1c6807cf8")


def test_parse_healthy_example():
    # Every rank at the same collective count -> healthy, no mismatched comms.
    report = parse_ras_schema(HEALTHY_RAS_JSON)
    assert report is not None
    assert report.healthy is True
    assert report.mismatched_comms == set()


def test_parse_repairs_missing_comma():
    # NCCL 2.28.9 emits missing_ranks[] with no comma before the nested "status",
    # which is invalid JSON until parse_ras_schema repairs it.
    with pytest.raises(json.JSONDecodeError):
        json.loads(DEAD_RANK_RAS_JSON)

    report = parse_ras_schema(DEAD_RANK_RAS_JSON)
    assert report is not None
    assert report.comm_collective_counts[_COMM_DEAD] == {
        0: {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 4,
        }
    }
    # Only one rank present, so no skew; but the missing rank is unresponsive.
    assert report.mismatched_comms == set()
    assert report.unresponsive_comms == {_COMM_DEAD}
    assert report.comm_missing_ranks[_COMM_DEAD][1]["unresponsive"] is True
    assert report.healthy is False


def test_parse_multicomm_example():
    # Three communicators, but only 0x4e01... diverges.
    report = parse_ras_schema(MULTI_COMM_RAS_JSON)
    assert report is not None
    assert report.healthy is False
    assert report.mismatched_comms == {_COMM_0X4E01}
    counts = report.comm_collective_counts[_COMM_0X4E01]
    assert counts[1]["AllReduce"] < counts[0]["AllReduce"]
    # host/pid/cuda_dev are retained for culprit-rank attribution.
    assert report.comm_rank_info[_COMM_0X4E01][1]["cuda_dev"] == 3


@pytest.mark.parametrize(
    "status,expected",
    [
        (
            {
                "abort_flag": True,
                "finalize_called": False,
                "destroy_flag": False,
                "init_state": 0,
            },
            "ABORT",
        ),
        (
            {
                "abort_flag": False,
                "finalize_called": True,
                "destroy_flag": False,
                "init_state": 0,
            },
            "FINALIZE",
        ),
        (
            {
                "abort_flag": False,
                "finalize_called": False,
                "destroy_flag": True,
                "init_state": 0,
            },
            "FINALIZE",
        ),
        (
            {
                "abort_flag": False,
                "finalize_called": False,
                "destroy_flag": False,
                "init_state": 1,
            },
            "INIT",
        ),
        (
            {
                "abort_flag": False,
                "finalize_called": False,
                "destroy_flag": False,
                "init_state": 0,
            },
            "RUNNING",
        ),
    ],
)
def test_rank_status_mapping(status, expected):
    assert RASReport.rank_status(status) == expected


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


class _SyncExecutor:
    """Executor stand-in that runs work inline so polling stays deterministic."""

    def submit(self, fn, *args, **kwargs):
        future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except BaseException as exc:  # noqa: BLE001
            future.set_exception(exc)
        return future


class _FakeClock:
    """Deterministic monotonic clock injected into the callback for time tests."""

    def __init__(self):
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def tick(self, dt: float):
        self.t += dt


_COMM_A = ("0x2b9ffd12ea17b069", "0xaaaa")
_COMM_B = ("0xced5b798f46495a3", "0xbbbb")

# A rank's spec is either an AllReduce count (int) or an explicit op->count map.
_RankCounts = Dict[int, Union[int, Dict[str, int]]]

_ts_counter = itertools.count()


def _fresh_ts() -> str:
    """A unique timestamp per report so the stale-snapshot guard never trips."""
    return f"ts-{next(_ts_counter)}"


def create_report(
    comms: Optional[Dict[tuple, _RankCounts]] = None,
    statuses: Optional[Dict[tuple, Dict[int, str]]] = None,
    missing: Optional[Dict[tuple, Dict[int, dict]]] = None,
    timestamp: Optional[str] = None,
    infos: Optional[Dict[tuple, Dict[int, dict]]] = None,
):
    """Build a RASReport from ``{comm_key: {local_rank: counts}}`` specs.

    A rank's ``counts`` is either an ``AllReduce`` count (int) or an explicit
    ``{op_name: count}`` mapping. Ranks default to RUNNING unless overridden via
    ``statuses``. ``infos`` optionally supplies per-rank ``{host, pid, ...}``
    used by culprit attribution. Each report gets a unique timestamp unless one
    is given.
    """
    comms = comms or {}
    statuses = statuses or {}
    comm_collective_counts = {
        comm: {
            rank: ({"AllReduce": c} if isinstance(c, int) else dict(c))
            for rank, c in ranks.items()
        }
        for comm, ranks in comms.items()
    }
    comm_rank_status = {
        comm: {rank: statuses.get(comm, {}).get(rank, "RUNNING") for rank in ranks}
        for comm, ranks in comms.items()
    }
    comm_rank_info = {comm: dict(i) for comm, i in (infos or {}).items()}
    comm_missing_ranks = {comm: dict(m) for comm, m in (missing or {}).items()}
    return RASReport(
        timestamp=timestamp if timestamp is not None else _fresh_ts(),
        comm_collective_counts=comm_collective_counts,
        comm_rank_status=comm_rank_status,
        comm_rank_info=comm_rank_info,
        comm_missing_ranks=comm_missing_ranks,
    )


def create_single_comm_report(counts, comm=_COMM_A):
    """One communicator with ``{global_rank: AllReduce count}``."""
    return create_report(comms={comm: counts})


def create_healthy_report():
    """A report with no communicators, so nothing is ever mismatched."""
    return create_report()


def make_nccl_ras_callback(
    monkeypatch,
    action,
    confirm_window_s,
    reports,
    poll_interval_s=1.0,
    first_suspicion_s=1.0,
    periodic_warn_every_s=2.0,
):
    """Build a callback whose JSON RAS query yields the given sequence of reports.

    Confirmation is wall-clock based: a communicator is confirmed after it has
    been frozen for ``confirm_window_s`` seconds. Tests drive the injected
    :class:`_FakeClock` (returned) with :func:`step` to advance time
    deterministically, one poll interval per poll.
    """
    monkeypatch.setenv(NCCL_RAS_ACTION_ENV_VAR, action)
    monkeypatch.setenv(NCCL_RAS_CONFIRM_WINDOW_S_ENV_VAR, str(confirm_window_s))
    monkeypatch.setenv(NCCL_RAS_POLL_INTERVAL_S_ENV_VAR, str(poll_interval_s))
    monkeypatch.setattr(nv_hang_detector, "_FIRST_SUSPICION_S", first_suspicion_s)
    monkeypatch.setattr(
        nv_hang_detector, "_PERIODIC_WARN_EVERY_S", periodic_warn_every_s
    )

    callback = NvHangDetectorCallback()
    callback._worker_group = MagicMock()
    callback._executor = _SyncExecutor()
    clock = _FakeClock()
    callback._clock = clock

    report_iter = iter(reports)

    def fake_query(ras_format="json", max_workers=None):
        # The one-off human-readable fetch at hang time must not consume a
        # report from the JSON poll sequence.
        if ras_format == "text":
            return "NCCL RAS text report"
        return next(report_iter, None)

    callback.query_ras_on_workers = fake_query

    # Artifact persistence (stack traces, RAS logs, ...) fans out to real
    # workers / storage; stub it out so the confirmation path stays a pure
    # in-process unit test, recording each invocation. Exercised separately.
    captured = []
    callback.save_diagnostic_artifacts = (
        lambda ras_text: captured.append(True) or "/tmp/dump"
    )

    return callback, captured, clock


def make_meta_worker(node_ip: str, pid: int, hostname: Optional[str] = None):
    """A worker stub with the ActorMetadata fields the world-rank join uses."""
    worker = MagicMock()
    worker.metadata.node_ip = node_ip
    worker.metadata.pid = pid
    worker.metadata.hostname = hostname if hostname is not None else node_ip
    return worker


def step(callback, clock, interval=1.0):
    """Advance the clock one poll interval and run a single poll."""
    clock.tick(interval)
    callback.after_worker_group_poll_status(MagicMock())


def use_real_query(callback):
    """Restore the real ``query_ras_on_workers`` (the harness stubs it out).

    The worker/subprocess-boundary tests need the genuine method, not the JSON
    report stub installed by :func:`make_nccl_ras_callback`.
    """
    del callback.query_ras_on_workers


@contextlib.contextmanager
def capture_nccl_logs(caplog):
    """Capture ``nv_hang_detector`` logger output regardless of Ray's propagation config.

    Ray disables propagation on the ``ray`` ancestor logger, so ``caplog`` (which
    listens on the root logger) never sees these records. Attach its handler to
    the module logger directly.
    """
    caplog.set_level(logging.INFO, logger=nv_hang_detector.logger.name)
    nv_hang_detector.logger.addHandler(caplog.handler)
    try:
        yield
    finally:
        nv_hang_detector.logger.removeHandler(caplog.handler)


def test_observe_mode_never_raises(monkeypatch):
    # A confirmed hang in observe mode never raises, but it now DOES capture
    # diagnostics (loud log + stack dump) exactly once at confirmation.
    reports = [create_single_comm_report({1: 5, 2: 4}) for _ in range(4)]
    callback, captured_artifact_dumps, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_window_s=1, reports=reports
    )

    for _ in reports:
        step(callback, clock)  # must not raise
    # Latched: diagnostics collected once even though the comm stays frozen.
    assert len(captured_artifact_dumps) == 1


def test_fail_mode_raises_after_confirm(monkeypatch):
    # rank 2 frozen behind rank 1 with no progress on either -> a frozen comm.
    reports = [create_single_comm_report({1: 5, 2: 4}) for _ in range(3)]
    callback, captured_artifact_dumps, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    step(callback, clock)  # baseline
    step(callback, clock)  # frozen, elapsed 0 < window
    with pytest.raises(NCCLHangError) as exc_info:
        step(callback, clock)  # frozen, elapsed >= window -> raise
    assert "1 of 1 communicators" in str(exc_info.value)
    assert len(captured_artifact_dumps) == 1


@pytest.mark.parametrize(
    "other_op_advances,expect_hang",
    [(False, True), (True, False)],
    ids=["other_op_frozen", "other_op_advancing"],
)
def test_hang_requires_whole_comm_frozen(monkeypatch, other_op_advances, expect_hang):
    # Comm A runs two collectives. AllReduce is skewed (mismatched) and frozen the
    # whole time; AllGather is matched across ranks.
    #   - other_op_frozen: AllGather is also frozen -> the communicator is fully
    #     stalled -> a hang.
    #   - other_op_advancing: AllGather keeps advancing -> ranks are alive and the
    #     stale AllReduce skew must NOT be treated as a hang.
    def report(allgather):
        return create_report(
            comms={
                _COMM_A: {
                    0: {"AllReduce": 5, "AllGather": allgather},
                    1: {"AllReduce": 4, "AllGather": allgather},
                }
            }
        )

    allgather_seq = [2, 3, 4] if other_op_advances else [2, 2, 2]
    reports = [report(ag) for ag in allgather_seq]

    callback, captured_artifact_dumps, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    step(callback, clock)  # baseline
    if expect_hang:
        step(callback, clock)  # frozen, elapsed 0
        assert set(callback.comm_frozen_since) == {_COMM_A}
        with pytest.raises(NCCLHangError) as exc_info:
            step(callback, clock)  # confirm
        assert "1 of 1 communicators" in str(exc_info.value)
        assert len(captured_artifact_dumps) == 1
    else:
        for _ in reports[1:]:
            step(callback, clock)  # never a hang
        assert callback.comm_frozen_since == {}
        assert not captured_artifact_dumps


def test_healthy_resets_frozen_streak(monkeypatch):
    reports = [
        create_single_comm_report({1: 5, 2: 4}),  # baseline (mismatched)
        create_single_comm_report({1: 5, 2: 4}),  # frozen
        create_healthy_report(),  # healthy -> reset
    ]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=5, reports=reports
    )

    step(callback, clock)
    step(callback, clock)
    assert set(callback.comm_frozen_since) == {_COMM_A}
    step(callback, clock)
    assert callback.comm_frozen_since == {}


def test_advancing_never_hangs(monkeypatch):
    # Both ranks keep advancing (nonzero deltas) even though skewed, so the comm
    # is never frozen and no hang is ever counted.
    reports = [
        create_single_comm_report({1: 5, 2: 3}),
        create_single_comm_report({1: 9, 2: 5}),
        create_single_comm_report({1: 15, 2: 8}),
        create_single_comm_report({1: 20, 2: 14}),
    ]
    callback, captured_artifact_dumps, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    for _ in reports:
        step(callback, clock)
        assert callback.comm_frozen_since == {}
    assert not captured_artifact_dumps


def test_advancing_then_freeze_hangs(monkeypatch):
    # Advancing (skewed) for a while, then rank 3's op freezes and rank 2 stops
    # too -> the comm is now frozen and the streak builds to a hang.
    reports = [
        create_single_comm_report({2: 5, 3: 3}),  # baseline, advancing
        create_single_comm_report({2: 8, 3: 6}),  # advancing
        create_single_comm_report({2: 10, 3: 6}),  # rank 3 froze, rank 2 still moved
        create_single_comm_report({2: 10, 3: 6}),  # fully frozen
        create_single_comm_report({2: 10, 3: 6}),  # fully frozen -> raise
    ]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    for _ in reports[:-1]:
        step(callback, clock)
    assert set(callback.comm_frozen_since) == {_COMM_A}
    with pytest.raises(NCCLHangError):
        step(callback, clock)


def test_per_communicator_streak_is_independent(monkeypatch):
    # Communicator A freezes while B stays merely skewed but advancing. Only A
    # should be confirmed and named in the error.
    reports = [
        create_report(comms={_COMM_A: {2: 5, 3: 6}, _COMM_B: {0: 5, 1: 4}}),
        create_report(comms={_COMM_A: {2: 5, 3: 6}, _COMM_B: {0: 7, 1: 6}}),
        create_report(comms={_COMM_A: {2: 5, 3: 6}, _COMM_B: {0: 9, 1: 8}}),
    ]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    step(callback, clock)  # baseline
    step(callback, clock)  # A frozen
    assert set(callback.comm_frozen_since) == {_COMM_A}
    with pytest.raises(NCCLHangError) as exc_info:
        step(callback, clock)  # A confirmed
    # Only A is confirmed (1 of the 2 communicators), not B.
    assert "1 of 2 communicators" in str(exc_info.value)


def test_communicator_added_later(monkeypatch):
    # Comm A advances normally, then comm B appears already skewed and stays
    # frozen. B cannot be flagged on the poll it first appears (no baseline) and
    # only starts its own streak once a baseline exists.
    reports = [
        create_report(comms={_COMM_A: {0: 2, 1: 2}}),  # baseline
        create_report(comms={_COMM_A: {0: 3, 1: 3}}),  # A advances, healthy
        create_report(comms={_COMM_A: {0: 4, 1: 4}, _COMM_B: {0: 7, 1: 5}}),  # B new
        create_report(comms={_COMM_A: {0: 5, 1: 5}, _COMM_B: {0: 7, 1: 5}}),  # B frozen
        create_report(comms={_COMM_A: {0: 6, 1: 6}, _COMM_B: {0: 7, 1: 5}}),  # confirm
    ]
    callback, captured_artifact_dumps, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    step(callback, clock)  # baseline (A healthy)
    step(callback, clock)  # A advances, healthy
    assert callback.comm_frozen_since == {}
    step(callback, clock)  # B appears, no baseline yet
    assert callback.comm_frozen_since == {}
    step(callback, clock)  # B frozen
    assert set(callback.comm_frozen_since) == {_COMM_B}
    with pytest.raises(NCCLHangError) as exc_info:
        step(callback, clock)  # B confirmed
    assert "1 of 2 communicators" in str(exc_info.value)
    assert len(captured_artifact_dumps) == 1


def test_communicator_removed_over_time(monkeypatch):
    # A communicator can disappear mid-run (its process group is destroyed). Its
    # in-progress streak is dropped, and a still-present comm keeps being
    # evaluated on its own.
    reports = [
        create_report(comms={_COMM_A: {2: 5, 3: 4}, _COMM_B: {0: 2, 1: 2}}),  # base
        create_report(comms={_COMM_A: {2: 5, 3: 4}, _COMM_B: {0: 3, 1: 3}}),  # A frozen
        create_report(comms={_COMM_B: {0: 6, 1: 4}}),  # A gone; B skewed, advancing
        create_report(comms={_COMM_B: {0: 9, 1: 6}}),  # B still advancing
    ]
    callback, captured_artifact_dumps, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=5, reports=reports
    )

    step(callback, clock)  # baseline
    step(callback, clock)  # A frozen
    assert set(callback.comm_frozen_since) == {_COMM_A}
    step(callback, clock)  # A removed -> streak drops
    assert callback.comm_frozen_since == {}
    step(callback, clock)  # B keeps advancing
    assert callback.comm_frozen_since == {}
    assert not captured_artifact_dumps


def test_finalize_and_running_mix_is_flagged(monkeypatch):
    # The "a rank exited early" hang: one FINALIZE rank + a frozen RUNNING peer.
    # Requiring only >=1 RUNNING rank (not all) means this is flagged.
    reports = [
        create_report(
            comms={_COMM_A: {0: 5, 1: 4}}, statuses={_COMM_A: {1: "FINALIZE"}}
        )
        for _ in range(3)
    ]
    callback, captured, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    step(callback, clock)  # baseline
    step(callback, clock)  # frozen
    with pytest.raises(NCCLHangError):
        step(callback, clock)


def test_all_finalize_not_flagged(monkeypatch):
    # If every rank has left RUNNING, a stale skew is not a live hang.
    r = create_report(
        comms={_COMM_A: {0: 5, 1: 4}},
        statuses={_COMM_A: {0: "FINALIZE", 1: "FINALIZE"}},
    )
    assert r.mismatched_comms == set()


def test_unresponsive_rank_confirms_without_skew(monkeypatch):
    # Survivors have EQUAL counts (skew 0) but a peer is unresponsive and the
    # survivors make no progress -> a hang the skew logic alone would miss.
    missing = {_COMM_A: {2: {"unresponsive": True, "considered_dead": False}}}
    reports = [
        create_report(comms={_COMM_A: {0: 5, 1: 5}}, missing=missing) for _ in range(3)
    ]
    callback, captured, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    step(callback, clock)  # baseline
    step(callback, clock)  # frozen (unresponsive, no progress)
    assert set(callback.comm_frozen_since) == {_COMM_A}
    with pytest.raises(NCCLHangError):
        step(callback, clock)


def test_stale_timestamp_is_skipped(monkeypatch):
    # A wedged RAS agent returning an identical snapshot (same timestamp) must not
    # count toward a hang.
    r1 = create_report(comms={_COMM_A: {0: 5, 1: 4}}, timestamp="same")
    r2 = create_report(comms={_COMM_A: {0: 5, 1: 4}}, timestamp="same")
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=[r1, r2]
    )

    step(callback, clock)  # baseline r1
    assert callback.prev_report is r1
    step(callback, clock)  # r2 stale -> skipped
    assert callback.prev_report is r1
    assert callback.comm_frozen_since == {}


def test_empty_diff_not_treated_as_frozen(monkeypatch):
    # A mismatched comm whose ranks don't overlap the previous poll has no shared
    # deltas; the emptiness guard prevents a vacuous "frozen" verdict.
    reports = [
        create_report(comms={_COMM_A: {0: 5, 1: 5}}),  # baseline, ranks 0,1
        create_report(
            comms={_COMM_A: {2: 5, 3: 4}}
        ),  # ranks 2,3 mismatched, no overlap
    ]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    step(callback, clock)  # baseline
    step(callback, clock)  # no shared ranks -> not frozen
    assert callback.comm_frozen_since == {}


def test_parse_to_evaluate_roundtrip(monkeypatch):
    # Exercise the real parser output through the state machine (the halves
    # otherwise only meet in the GPU e2e suite). 0x4e01 is skewed and frozen.
    j2 = MULTI_COMM_RAS_JSON.replace("06:56:24", "06:56:25")
    j3 = MULTI_COMM_RAS_JSON.replace("06:56:24", "06:56:26")
    reports = [parse_ras_schema(j) for j in (MULTI_COMM_RAS_JSON, j2, j3)]
    assert all(r is not None for r in reports)

    callback, captured, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    step(callback, clock)  # baseline
    step(callback, clock)  # frozen
    with pytest.raises(NCCLHangError) as exc_info:
        step(callback, clock)  # confirm
    assert "1 of 3 communicators" in str(exc_info.value)


def test_throttle_skips_query(monkeypatch):
    # With a large interval, the second poll falls inside the throttle window of
    # the first, so only the first poll queries and consumes a report.
    reports = [create_single_comm_report({2: 5, 3: 3})]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_window_s=1, reports=reports
    )
    callback._poll_interval_s = 1000.0

    step(callback, clock)  # queries (last_query_time was -inf)
    first_report = callback.prev_report
    assert first_report is not None
    step(callback, clock)  # throttled (only advanced 1s < 1000s)
    assert callback.prev_report is first_report  # unchanged -> no second query


def test_degraded_skips(monkeypatch):
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_window_s=1,
        reports=[create_single_comm_report({2: 5, 3: 3})],
    )
    callback._is_ras_degraded = True
    step(callback, clock)  # must not query/raise
    assert callback.prev_report is None
    assert callback.comm_frozen_since == {}


def test_unexpected_error_disables_detection(monkeypatch):
    # A bug in the detection path must never crash training: it is logged and
    # detection is disabled (transiently) for the rest of the run.
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_window_s=1,
        reports=[create_single_comm_report({2: 5, 3: 3})],
    )

    def boom(*_args, **_kwargs):
        raise RuntimeError("detector bug")

    callback.poll_ras_report = boom
    step(callback, clock)  # must not raise
    assert callback._ras_transient_disabled is True
    # Once disabled, later polls are a no-op (guarded before poll_ras_report).
    step(callback, clock)
    assert callback.prev_report is None


def test_hang_error_propagates_when_diagnostics_fail(monkeypatch):
    # Collecting hang diagnostics must not suppress the hang failure, and a
    # real hang must not be misread as a detector bug.
    reports = [create_single_comm_report({2: 5, 3: 4}) for _ in range(3)]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )

    def boom(ras_text):
        raise RuntimeError("artifact persistence failed")

    callback.save_diagnostic_artifacts = boom

    step(callback, clock)  # baseline
    step(callback, clock)  # frozen
    with pytest.raises(NCCLHangError):
        step(callback, clock)  # still raises
    assert callback._is_ras_degraded is False
    assert callback._ras_transient_disabled is False


def test_restart_resets_transient_not_fatal(monkeypatch):
    # A worker-group (re)start clears transient degradation but leaves a fatal
    # misconfiguration latched.
    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=[]
    )
    callback._ras_transient_disabled = True
    callback._is_ras_degraded = True

    callback.after_worker_group_start(MagicMock())
    assert callback._ras_transient_disabled is False
    assert callback._is_ras_degraded is True


def test_execute_async_failure_does_not_cancel_none(monkeypatch):
    # If worker.execute_async itself raises, ref is None and we must NOT call
    # ray.cancel(None) (which would raise and escape).
    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=[]
    )
    use_real_query(callback)
    worker = MagicMock()
    worker.execute_async.side_effect = RuntimeError("dead actor")
    callback._worker_group.get_workers.return_value = [worker]

    cancels = []
    monkeypatch.setattr(nv_hang_detector.ray, "cancel", lambda ref: cancels.append(ref))

    # Must not raise, and must not have tried to cancel a None ref.
    assert callback.query_ras_on_workers("json") is None
    assert cancels == []


def test_consecutive_failures_degrade_transiently(monkeypatch):
    monkeypatch.setattr(nv_hang_detector, "_MAX_CONSECUTIVE_QUERY_FAILURES", 3)
    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=[]
    )
    use_real_query(callback)
    worker = MagicMock()
    worker.execute_async.return_value = "ref"
    callback._worker_group.get_workers.return_value = [worker]
    monkeypatch.setattr(
        nv_hang_detector.ray,
        "get",
        lambda ref, timeout=None: {"ok": False, "reason": "exit_1"},
    )

    for _ in range(2):
        assert callback.query_ras_on_workers("json") is None
        assert callback._ras_transient_disabled is False
    assert callback.query_ras_on_workers("json") is None
    assert callback._ras_transient_disabled is True


def test_query_success_resets_failure_counter(monkeypatch):
    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=[]
    )
    use_real_query(callback)
    callback._consecutive_ras_query_failures = 5
    worker = MagicMock()
    worker.execute_async.return_value = "ref"
    callback._worker_group.get_workers.return_value = [worker]
    monkeypatch.setattr(
        nv_hang_detector.ray,
        "get",
        lambda ref, timeout=None: {"ok": True, "stdout": HEALTHY_RAS_JSON},
    )

    report = callback.query_ras_on_workers("json")
    assert report is not None
    assert callback._consecutive_ras_query_failures == 0


def test_executor_shutdown_on_controller_shutdown(monkeypatch):
    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_window_s=1, reports=[]
    )
    callback._executor = ThreadPoolExecutor(max_workers=1)
    asyncio.run(callback.before_controller_shutdown())
    assert callback._executor is None


@pytest.mark.parametrize(
    "env",
    [
        {NCCL_RAS_ACTION_ENV_VAR: "not-a-mode"},
        {NCCL_RAS_CONFIRM_WINDOW_S_ENV_VAR: "0"},
        {NCCL_RAS_CONFIRM_WINDOW_S_ENV_VAR: "-1"},
    ],
)
def test_invalid_config_fails_fast(monkeypatch, env):
    # Misconfigured env vars fail fast at construction with a clear ValueError.
    monkeypatch.setenv(NCCL_RAS_ACTION_ENV_VAR, NCCL_RAS_ACTION_FAIL)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(ValueError):
        NvHangDetectorCallback()


def test_suspicion_and_periodic_messages_fail_mode(monkeypatch, caplog):
    # A frozen communicator builds a streak. The confirm window is long enough that no
    # hang is confirmed, so we can observe the escalating warnings on the way up.
    reports = [create_single_comm_report({1: 5, 2: 4}) for _ in range(5)]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=100, reports=reports
    )

    with capture_nccl_logs(caplog):
        for _ in reports:
            step(callback, clock)

    text = caplog.text
    # New-suspicion announcement names the stalled communicator.
    assert "Possible NCCL hang detected!" in text
    assert _COMM_A[0] in text
    # Periodic reminder uses the "still suspected" wording.
    assert "NCCL hang still suspected!" in text
    # Fail mode threatens to raise a NCCLHangError.
    assert "A NCCLHangError will be raised" in text
    # The RAS report is logged verbatim.
    assert "NCCL RAS text report" in text


def test_escalation_absent_in_observe_mode(monkeypatch, caplog):
    # Observe mode still surfaces the suspicion/periodic warnings but must never
    # threaten to raise an error, since it only observes.
    reports = [create_single_comm_report({1: 5, 2: 4}) for _ in range(5)]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_window_s=100, reports=reports
    )

    with capture_nccl_logs(caplog):
        for _ in reports:
            step(callback, clock)

    text = caplog.text
    assert "Possible NCCL hang detected!" in text
    assert "NCCL hang still suspected!" in text
    assert "NCCLHangError will be raised" not in text


def test_no_suspicion_warning_after_confirmation(monkeypatch, caplog):
    # With a confirm window shorter than the first-suspicion threshold, the hang
    # confirms before the suspicion warning would fire; the (now redundant)
    # "possible hang" / "still suspected" messages must be suppressed for the
    # already-confirmed communicator rather than trailing the confirmation.
    reports = [create_single_comm_report({1: 5, 2: 4}) for _ in range(6)]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        confirm_window_s=2,
        reports=reports,
        first_suspicion_s=4.0,
        periodic_warn_every_s=4.0,
    )

    with capture_nccl_logs(caplog):
        for _ in reports:
            step(callback, clock)

    text = caplog.text
    assert "NCCL hang confirmed" in text
    assert "Possible NCCL hang detected!" not in text
    assert "NCCL hang still suspected!" not in text


def test_culprit_ranks_named_in_worker_failures(monkeypatch):
    # A confirmed hang attributes the laggard rank (behind on AllReduce) with its
    # host/pid/cuda_dev, exposed via NCCLHangError.worker_failures keyed by WORLD
    # rank. RAS ranks are communicator-local: in the fixture the hung sub-comm
    # 0x4e01's laggard is its local rank 1 = pid 10207, which is world rank 3 in
    # the worker group below (ordered to match the fixture's size-4 comm).
    reports = [parse_ras_schema(j) for j in (MULTI_COMM_RAS_JSON,) * 3]
    # Give distinct timestamps so the stale-snapshot guard doesn't skip them.
    for i, r in enumerate(reports):
        r.timestamp = f"cul-{i}"
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )
    callback._worker_group.get_workers.return_value = [
        make_meta_worker("10.0.77.184", pid) for pid in (10206, 10204, 10205, 10207)
    ]

    step(callback, clock)  # baseline
    step(callback, clock)  # frozen
    with pytest.raises(NCCLHangError) as exc_info:
        step(callback, clock)  # confirm

    failures = exc_info.value.worker_failures
    # In 0x4e01, local rank 1 (AllReduce 3) lags local rank 0 (AllReduce 10) by
    # 7 launches; its (host, pid) maps to world rank 3, NOT local rank 1.
    assert set(failures) == {3}
    msg = str(failures[3])
    assert "World rank 3" in msg
    assert "rank 1 of communicator 0x4e0104c2022fa2f9" in msg
    assert "behind by 7 AllReduce" in msg
    assert "cuda:3" in msg  # the laggard's cuda_dev in the fixture
    # The culprit summary is also in the human-readable error message.
    assert "Suspected culprit rank(s)" in str(exc_info.value)


def test_culprit_ranks_include_unresponsive(monkeypatch):
    # An unresponsive rank (no skew) is still named as a culprit at confirmation.
    # RAS reports its host as a hostname here, exercising the hostname side of
    # the (host, pid) -> world rank join; the matching worker sits at index 0,
    # deliberately different from the comm-local rank 2.
    missing = {
        _COMM_A: {
            2: {
                "unresponsive": True,
                "considered_dead": False,
                "host": "gpu-9",
                "pid": 4242,
            }
        }
    }
    reports = [
        create_report(comms={_COMM_A: {0: 5, 1: 5}}, missing=missing) for _ in range(3)
    ]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )
    callback._worker_group.get_workers.return_value = [
        make_meta_worker("10.0.0.9", 4242, hostname="gpu-9"),
        make_meta_worker("10.0.0.1", 1001),
        make_meta_worker("10.0.0.2", 1002),
    ]

    step(callback, clock)  # baseline
    step(callback, clock)  # frozen
    with pytest.raises(NCCLHangError) as exc_info:
        step(callback, clock)  # confirm

    failures = exc_info.value.worker_failures
    assert set(failures) == {0}
    assert "unresponsive" in str(failures[0])
    assert "pid 4242" in str(failures[0])


def test_culprits_across_comms_do_not_collide(monkeypatch):
    # Two communicators confirm together and each one's laggard is comm-local
    # rank 1, but they are different processes. The world-rank join must name
    # both culprits rather than letting one comm's "rank 1" overwrite the
    # other's.
    host = "10.0.0.1"
    infos = {
        _COMM_A: {0: {"host": host, "pid": 100}, 1: {"host": host, "pid": 103}},
        _COMM_B: {0: {"host": host, "pid": 101}, 1: {"host": host, "pid": 102}},
    }
    reports = [
        create_report(comms={_COMM_A: {0: 5, 1: 4}, _COMM_B: {0: 9, 1: 7}}, infos=infos)
        for _ in range(3)
    ]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )
    callback._worker_group.get_workers.return_value = [
        make_meta_worker(host, pid) for pid in (100, 101, 102, 103)
    ]

    step(callback, clock)  # baseline
    step(callback, clock)  # both frozen
    with pytest.raises(NCCLHangError) as exc_info:
        step(callback, clock)  # both confirmed

    failures = exc_info.value.worker_failures
    assert set(failures) == {2, 3}
    assert "behind by 1 AllReduce" in str(failures[3])  # comm A's laggard
    assert "behind by 2 AllReduce" in str(failures[2])  # comm B's laggard


def test_unmatched_culprit_kept_in_message_only(monkeypatch):
    # A culprit whose (host, pid) matches no worker in the group (e.g. RAS saw
    # a NCCL process outside this run) is still named in the error text but
    # must not claim a world-rank key in worker_failures.
    infos = {
        _COMM_A: {
            0: {"host": "10.0.0.1", "pid": 100},
            1: {"host": "10.0.0.1", "pid": 999},
        }
    }
    reports = [
        create_report(comms={_COMM_A: {0: 5, 1: 4}}, infos=infos) for _ in range(3)
    ]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )
    callback._worker_group.get_workers.return_value = [
        make_meta_worker("10.0.0.1", 100)
    ]

    step(callback, clock)  # baseline
    step(callback, clock)  # frozen
    with pytest.raises(NCCLHangError) as exc_info:
        step(callback, clock)  # confirm

    assert exc_info.value.worker_failures == {}
    msg = str(exc_info.value)
    assert "An unidentified worker" in msg
    assert "pid 999" in msg
    assert "rank 1 of communicator" in msg


def test_worker_failures_pickle_roundtrip(monkeypatch):
    # NCCLHangError pickles worker_failures via WorkerGroupError.__reduce__; the
    # culprit exceptions must survive the trip back to the driver.
    import pickle

    infos = {
        _COMM_A: {
            0: {"host": "10.0.0.1", "pid": 100},
            1: {"host": "10.0.0.1", "pid": 101},
        }
    }
    reports = [
        create_report(comms={_COMM_A: {0: 5, 1: 4}}, infos=infos) for _ in range(3)
    ]
    callback, _, clock = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_window_s=1, reports=reports
    )
    callback._worker_group.get_workers.return_value = [
        make_meta_worker("10.0.0.1", 100),
        make_meta_worker("10.0.0.1", 101),
    ]
    step(callback, clock)
    step(callback, clock)
    with pytest.raises(NCCLHangError) as exc_info:
        step(callback, clock)

    # Local rank 1's (host, pid) maps to world rank 1 in the worker list above.
    assert set(exc_info.value.worker_failures) == {1}
    restored = pickle.loads(pickle.dumps(exc_info.value))
    assert set(restored.worker_failures) == set(exc_info.value.worker_failures)
    assert all(
        isinstance(exc, nv_hang_detector.NCCLRankHang)
        for exc in restored.worker_failures.values()
    )


def test_ring_buffer_captures_raw_json(monkeypatch):
    # Each successful JSON poll appends the raw report to the postmortem ring
    # buffer used by _persist_hang_artifacts.
    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_window_s=1, reports=[]
    )
    use_real_query(callback)
    worker = MagicMock()
    worker.execute_async.return_value = "ref"
    callback._worker_group.get_workers.return_value = [worker]
    monkeypatch.setattr(
        nv_hang_detector.ray,
        "get",
        lambda ref, timeout=None: {"ok": True, "stdout": HEALTHY_RAS_JSON},
    )

    callback.query_ras_on_workers("json")
    callback.query_ras_on_workers("json")
    assert list(callback._ras_report_history) == [HEALTHY_RAS_JSON, HEALTHY_RAS_JSON]


def test_ring_buffer_bounded(monkeypatch):
    monkeypatch.setattr(nv_hang_detector, "_RAW_REPORT_HISTORY_SIZE", 3)
    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_window_s=1, reports=[]
    )
    # reset_detection_state rebuilds the deque with the patched maxlen.
    callback.reset_detection_state()
    for _ in range(5):
        callback._ras_report_history.append("x")
    assert len(callback._ras_report_history) == 3


def test_persist_hang_artifacts_writes_files(monkeypatch, tmp_path):
    # Drive the real save_diagnostic_artifacts against a local-filesystem
    # storage context and assert the single-folder layout: per-rank stack
    # traces under stack_traces/, the text report and one raw JSON log per
    # retained poll (named by RAS timestamp) under nccl_ras/, the per-node
    # nvidia-smi dumps under nvidia_smi/, and the per-rank flight recorder
    # traces under flight_recorder/.
    import pyarrow.fs

    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_window_s=1, reports=[]
    )
    # Undo the harness stub so the real method runs.
    del callback.save_diagnostic_artifacts
    callback._ras_report_history.extend([HEALTHY_RAS_JSON, MULTI_COMM_RAS_JSON])

    storage = MagicMock()
    dest = tmp_path / "experiment"
    storage.experiment_fs_path = str(dest)
    storage.storage_filesystem = pyarrow.fs.LocalFileSystem()
    callback._worker_group._storage_context = storage
    callback.collect_nvidia_smi_per_node = lambda: {"10.0.0.1": "smi-output"}
    callback._flight_recorder_enabled = True

    def fake_collect(fn, *args, **kwargs):
        if fn is nv_hang_detector.dump_stack_trace:
            return {0: "stack-0", 1: "stack-1"}
        assert fn is nv_hang_detector.dump_flight_recorder
        return {
            0: (True, '{"entries": []}'),
            1: (
                False,
                "no flight-recorder JSON dump symbol found in this torch build",
            ),
        }

    callback.collect_from_workers = fake_collect

    folder = callback.save_diagnostic_artifacts("human readable ras report")
    base = dest / "nv_hang_detector_artifacts"
    assert folder == str(base)

    assert (base / "stack_traces" / "0.log").read_text() == "stack-0"
    assert (base / "stack_traces" / "1.log").read_text() == "stack-1"
    ras_logs = {p.name for p in (base / "nccl_ras").iterdir()}
    assert ras_logs == {
        "report.txt",
        "2026-06-19_06_51_56.log",
        "2026-06-19_06_56_24.log",
    }
    assert (base / "nccl_ras" / "2026-06-19_06_51_56.log").read_text() == (
        HEALTHY_RAS_JSON
    )
    assert (base / "nvidia_smi" / "10.0.0.1.log").read_text() == "smi-output"
    assert (base / "flight_recorder" / "0.log").read_text() == '{"entries": []}'
    assert "unavailable" in (base / "flight_recorder" / "1.log").read_text()


def test_nvidia_smi_collected_once_per_node(monkeypatch):
    # nvidia-smi describes every GPU on a node, so co-located workers must not
    # produce duplicate dumps: one probe worker per node, keyed by node IP.
    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_window_s=1, reports=[]
    )

    def make_worker(node_id, node_ip):
        worker = MagicMock()
        worker.metadata.node_id = node_id
        worker.metadata.node_ip = node_ip
        return worker

    callback._worker_group.get_workers.return_value = [
        make_worker("node-a", "10.0.0.1"),
        make_worker("node-a", "10.0.0.1"),
        make_worker("node-b", "10.0.0.2"),
    ]

    collected_workers = []

    def fake_collect(fn, *args, workers=None, **kwargs):
        collected_workers.extend(workers)
        return {i: f"smi-{i}" for i in range(len(workers))}

    callback.collect_from_workers = fake_collect
    assert callback.collect_nvidia_smi_per_node() == {
        "10.0.0.1": "smi-0",
        "10.0.0.2": "smi-1",
    }
    assert len(collected_workers) == 2


@pytest.mark.parametrize(
    "buffer_size,expected",
    [(None, False), ("0", False), ("20000", True)],
    ids=["unset", "zero", "positive"],
)
def test_flight_recorder_opt_in_via_torch_env(monkeypatch, buffer_size, expected):
    # TORCH_NCCL_TRACE_BUFFER_SIZE doubles as the opt-in flag: unset or 0 leaves
    # the flight-recorder dump disabled; a positive buffer size enables it.
    if buffer_size is not None:
        monkeypatch.setenv(TORCH_NCCL_TRACE_BUFFER_SIZE_ENV_VAR, buffer_size)
    callback, _, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_window_s=1, reports=[]
    )
    assert callback._flight_recorder_enabled is expected


def test_flight_recorder_env_var_propagates_to_workers(monkeypatch):
    # The variable must reach worker processes before the process group is
    # created for the ring buffer to be armed; Ray Train propagates it from the
    # driver through the standard env-var propagation set.
    from ray.train.v2._internal.constants import (
        ENV_VARS_TO_PROPAGATE,
        get_env_vars_to_propagate,
    )

    assert TORCH_NCCL_TRACE_BUFFER_SIZE_ENV_VAR in ENV_VARS_TO_PROPAGATE
    monkeypatch.setenv(TORCH_NCCL_TRACE_BUFFER_SIZE_ENV_VAR, "20000")
    assert get_env_vars_to_propagate()[TORCH_NCCL_TRACE_BUFFER_SIZE_ENV_VAR] == "20000"


def test_dump_flight_recorder_fail_soft():
    # Whether or not this machine's torch build has a flight-recorder JSON dump
    # symbol, the dump returns (ok, str) rather than raising -- the payload is
    # the JSON trace on success or the skip reason on failure.
    ok, payload = nv_hang_detector.dump_flight_recorder()
    assert isinstance(ok, bool)
    assert isinstance(payload, str)


def test_dump_flight_recorder_falls_back_past_a_raising_symbol(monkeypatch):
    # A dump symbol that exists but raises must not end the search: the
    # backend-agnostic `_dump_fr_trace_json` is still tried, so an unhealthy
    # NCCL-specific dump can't cost us the flight recorder trace entirely.
    dist_c10d = pytest.importorskip("torch.distributed.distributed_c10d")
    c10d = pytest.importorskip("torch._C._distributed_c10d")

    def boom():
        raise RuntimeError("nccl dump exploded")

    for module in (dist_c10d, c10d):
        monkeypatch.setattr(module, "_dump_nccl_trace_json", boom, raising=False)
        monkeypatch.setattr(
            module, "_dump_fr_trace_json", lambda: b'{"entries": []}', raising=False
        )

    ok, payload = nv_hang_detector.dump_flight_recorder()
    assert ok
    # bytes payloads are decoded for the artifact file.
    assert payload == '{"entries": []}'


def test_dump_flight_recorder_reports_every_failure(monkeypatch):
    # When every candidate symbol raises, the reason names them rather than
    # claiming no dump symbol exists in this torch build.
    dist_c10d = pytest.importorskip("torch.distributed.distributed_c10d")
    c10d = pytest.importorskip("torch._C._distributed_c10d")

    def boom():
        raise RuntimeError("dump exploded")

    for module in (dist_c10d, c10d):
        for name in ("_dump_nccl_trace_json", "_dump_fr_trace_json"):
            monkeypatch.setattr(module, name, boom, raising=False)

    ok, reason = nv_hang_detector.dump_flight_recorder()
    assert not ok
    assert "_dump_nccl_trace_json failed: dump exploded" in reason
    assert "_dump_fr_trace_json failed: dump exploded" in reason


def test_dump_nvidia_smi_fail_soft():
    # No GPU here: must return a reason string, never raise.
    out = nv_hang_detector.dump_nvidia_smi()
    assert isinstance(out, str)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
