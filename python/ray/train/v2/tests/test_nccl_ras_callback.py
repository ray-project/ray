"""Unit tests for the NCCL RAS hang-detection callback (no GPU required)."""
import json
import logging
import subprocess
import sys
import time
import types
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from unittest.mock import MagicMock

import pytest

from ray.train.v2._internal.callbacks import nccl_ras
from ray.train.v2._internal.callbacks.nccl_ras import (
    DiagnosticResult,
    NCCLRASCallback,
    RASPoller,
    RASQueryError,
    RASReport,
    dump_stack_trace,
    fan_out_to_workers,
    parse_ras_addr,
    parse_ras_schema,
    run_nvidia_smi,
)
from ray.train.v2._internal.constants import (
    HANG_DETECTOR_DIRNAME,
    NCCL_RAS_ACTION_ENV_VAR,
    NCCL_RAS_ACTION_FAIL,
    NCCL_RAS_ACTION_OBSERVE,
    NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR,
    NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR,
)
from ray.train.v2._internal.state.schema import (
    NcclCommunicatorHangState,
    NcclHangDetectorStatus,
    NcclHangStopReason,
    NcclHangTrigger,
    StalledRanks,
    TrainNcclHangEvent,
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


def test_parse_healthy_ras_example():
    # Every rank at the same op-count -> healthy, no mismatched comms.
    report = parse_ras_schema(HEALTHY_RAS_JSON)
    assert report is not None
    assert report.healthy is True
    assert report.mismatched_comms == set()


def test_parse_keeps_the_raw_output():
    # The history written at hang time is the raw `ncclras` output, so a report
    # has to carry what it was parsed from -- including the unrepaired JSON, so
    # the file shows what NCCL actually emitted.
    assert parse_ras_schema(HEALTHY_RAS_JSON).raw_json == HEALTHY_RAS_JSON
    assert parse_ras_schema(DEAD_RANK_RAS_JSON).raw_json == DEAD_RANK_RAS_JSON


def test_parse_ras_missing_comma():
    # NCCL 2.28.9 emits missing_ranks[] with no comma before the nested "status",
    # which is invalid JSON until parse_ras_schema repairs it.
    with pytest.raises(json.JSONDecodeError):
        json.loads(DEAD_RANK_RAS_JSON)

    report = parse_ras_schema(DEAD_RANK_RAS_JSON)
    assert report is not None
    assert report.comm_op_counts["0xa1349768e9517ed7"] == {
        0: {
            "Broadcast": 0,
            "Reduce": 0,
            "AllGather": 0,
            "ReduceScatter": 0,
            "AllReduce": 4,
        }
    }
    assert report.mismatched_comms == set()


def test_parse_keeps_each_ranks_process():
    # The host and pid of every rank are what joins a RAS rank to its Ray Train
    # worker, so the parser has to keep them.
    report = parse_ras_schema(HEALTHY_RAS_JSON)
    assert report.comm_rank_processes["0x514e98cf4e44862b"] == {
        0: ("10.0.77.184", 8813),
        1: ("10.0.77.184", 8814),
    }


def test_parse_multicomm_ras_example():
    # Three communicators, but only 0x4e01... diverges
    report = parse_ras_schema(MULTI_COMM_RAS_JSON)
    assert report is not None
    assert report.healthy is False
    assert report.mismatched_comms == {"0x4e0104c2022fa2f9"}
    counts = report.comm_op_counts["0x4e0104c2022fa2f9"]
    assert counts[1]["AllReduce"] < counts[0]["AllReduce"]


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


@pytest.fixture(autouse=True)
def emitted(monkeypatch) -> List[TrainNcclHangEvent]:
    """The hang events the callbacks export, in order. Nothing reaches the
    state actor (or Ray) in these unit tests."""
    events: List[TrainNcclHangEvent] = []
    monkeypatch.setattr(
        NCCLRASCallback, "_emit", lambda self, event: events.append(event)
    )
    return events


class FakePoller:
    """Stand-in for :class:`RASPoller` that hands the controller a scripted
    sequence of poll results, one per tick, and a fixed text report."""

    TEXT_REPORT = "NCCL RAS text report"

    def __init__(self, results):
        self._results = list(results)
        self.stopped = False
        self.text_queries = 0

    def start(self):
        pass

    def stop(self):
        self.stopped = True

    def next_result(self):
        return self._results.pop(0) if self._results else None

    def query(self, fmt):
        # The one-off human-readable fetch at hang time must not consume a
        # result from the scripted JSON sequence.
        assert fmt == "text"
        self.text_queries += 1
        return self.TEXT_REPORT


def make_nccl_ras_callback(
    monkeypatch,
    action,
    confirm_count,
    reports,
    first_suspicion_polls=1,
    periodic_warn_every_polls=2,
    nvidia_smi=True,
):
    """Build a callback whose poller yields the given sequence of poll results.

    The detector confirms hangs on consecutive frozen polls but is configured in
    seconds, so a 1s poll interval makes every "seconds" knob equal a poll count:
    ``confirm_count`` polls, and the escalation milestones (tuned for production
    at 60s/120s, i.e. 4 and 8 polls) shrunk to values the small
    ``confirm_count``s here actually reach.

    ``nvidia_smi`` is whether the nodes have ``nvidia-smi`` at all: without it
    the snapshot has nothing to record and returns no directory.

    No poller thread is started: a :class:`FakePoller` returns one scripted
    result per controller tick, so the tests stay deterministic.

    Returns the callback and the list its diagnostic captures record themselves
    in (the tool names that produced a directory, in capture order).
    """
    monkeypatch.setenv(NCCL_RAS_ACTION_ENV_VAR, action)
    monkeypatch.setenv(NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR, str(confirm_count))
    monkeypatch.setenv(NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, "1")
    monkeypatch.setattr(
        nccl_ras, "_FIRST_SUSPICION_AFTER_S", float(first_suspicion_polls)
    )
    monkeypatch.setattr(
        nccl_ras, "_PERIODIC_WARN_EVERY_S", float(periodic_warn_every_polls)
    )

    callback = NCCLRASCallback()
    assert callback._confirm_poll_counts == confirm_count
    callback._worker_group = MagicMock()
    callback._ras_poller = FakePoller(reports)

    captured = []

    def capture(tool, produces_dir=True):
        if not produces_dir:
            return None
        captured.append(tool)
        return f"/exp/{HANG_DETECTOR_DIRNAME}/{tool}"

    callback.dump_workers_stack_traces = lambda: capture(nccl_ras._STACK_TRACES_TOOL)
    callback.dump_nodes_nvidia_smi = lambda: capture(
        nccl_ras._NVIDIA_SMI_TOOL, produces_dir=nvidia_smi
    )
    callback.dump_ras_query_history = lambda _report=None: capture(
        nccl_ras._NCCL_RAS_TOOL
    )

    return callback, captured


_COMM_A = "0x2b9ffd12ea17b069"
_COMM_B = "0xced5b798f46495a3"

# The diagnostics a confirmed hang captures, in the order they are collected.
_CONFIRMED_HANG_DIAGNOSTICS = [
    nccl_ras._NVIDIA_SMI_TOOL,
    nccl_ras._NCCL_RAS_TOOL,
    nccl_ras._STACK_TRACES_TOOL,
]

# A rank's spec is either an AllReduce count (int) or an explicit op->count map.
_RankCounts = Dict[int, Union[int, Dict[str, int]]]


def create_report(
    comms: Optional[Dict[str, _RankCounts]] = None,
    processes: Optional[Dict[str, Dict[int, Tuple[str, int]]]] = None,
):
    """Build a RASReport from ``{comm_id: {global_rank: counts}}`` specs.

    A rank's ``counts`` is either an ``AllReduce`` count (int) or an explicit
    ``{op_name: count}`` mapping. Every rank is reported as RUNNING.
    ``processes`` is the ``{comm_id: {rank: (host, pid)}}`` RAS reports.
    """
    comms = comms or {}
    comm_op_counts = {
        comm_id: {
            rank: ({"AllReduce": c} if isinstance(c, int) else dict(c))
            for rank, c in ranks.items()
        }
        for comm_id, ranks in comms.items()
    }
    comm_rank_status = {
        comm_id: {rank: "RUNNING" for rank in ranks} for comm_id, ranks in comms.items()
    }
    return RASReport(
        timestamp="2026-06-19 00:00:00",
        comm_op_counts=comm_op_counts,
        comm_rank_status=comm_rank_status,
        comm_rank_processes=processes or {},
        raw_json="",
    )


def create_single_comm_report(counts, comm_id=_COMM_A):
    """One communicator with ``{global_rank: AllReduce count}``."""
    return create_report(comms={comm_id: counts})


def create_healthy_report():
    """A report with no communicators, so nothing is ever mismatched."""
    return create_report()


def test_observe_mode_never_raises(monkeypatch):
    # A confirmed hang in observe mode never raises, but still captures the same
    # diagnostics as fail mode: observing is worthless without them.
    reports = [create_single_comm_report({1: 5, 2: 4})] * 3
    callback, captured_diagnostics = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2, reports=reports
    )

    for _ in reports:
        callback.after_worker_group_poll_status(MagicMock())  # must not raise
    assert captured_diagnostics.count(nccl_ras._STACK_TRACES_TOOL) == 1


def test_fail_mode_raises_after_confirm(monkeypatch):
    # rank 3 frozen behind rank 2 with no progress on either -> a deadlock.
    reports = [create_single_comm_report({1: 5, 2: 4})] * 3
    callback, captured_diagnostics = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    callback.after_worker_group_poll_status(MagicMock())  # frozen -> 1/2
    with pytest.raises(NCCLHangError) as exc_info:
        callback.after_worker_group_poll_status(MagicMock())  # frozen -> 2/2 -> raise
    # The error reports how many communicators were confirmed stalled.
    message = str(exc_info.value)
    assert "1 communicator(s)" in message
    assert captured_diagnostics == _CONFIRMED_HANG_DIAGNOSTICS
    # ...and points the user at every diagnostic that was captured.
    assert "Compare the stack traces" in message
    assert "/exp/hang_detector/stack_traces" in message
    assert "ncclras_report.txt" in message
    assert "/exp/hang_detector/nccl_ras" in message


@pytest.mark.parametrize("nvidia_smi", [False, True], ids=["no_gpus", "gpus"])
def test_confirmed_hang_captures_diagnostics(monkeypatch, nvidia_smi):
    # Which diagnostics a confirmed hang collects, and what the error points the
    # user at. On a node with no `nvidia-smi` there is no hardware snapshot, and
    # the hang must still fail the run and report the stack traces.
    reports = [create_single_comm_report({1: 5, 2: 4})] * 3
    callback, captured = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_count=2,
        reports=reports,
        nvidia_smi=nvidia_smi,
    )

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    callback.after_worker_group_poll_status(MagicMock())  # frozen -> 1/2
    with pytest.raises(NCCLHangError) as exc_info:
        callback.after_worker_group_poll_status(MagicMock())  # frozen -> 2/2

    message = str(exc_info.value)
    assert "Compare the stack traces" in message
    assert "/exp/hang_detector/stack_traces" in message
    if nvidia_smi:
        # Snapshotted before the stack traces, which take far longer, so the GPU
        # reading is as close to the moment of the hang as we can make it.
        assert captured == _CONFIRMED_HANG_DIAGNOSTICS
        assert "`nvidia-smi` snapshots" in message
        assert "/exp/hang_detector/nvidia_smi" in message
    else:
        assert captured == [nccl_ras._NCCL_RAS_TOOL, nccl_ras._STACK_TRACES_TOOL]
        assert "nvidia-smi" not in message
    # The hang is the expected outcome, not a detector bug.
    assert callback._is_ras_degraded is False


@pytest.mark.parametrize(
    "other_op_advances,expect_hang",
    [(False, True), (True, False)],
    ids=["other_op_frozen", "other_op_advancing"],
)
def test_deadlock_requires_whole_comm_frozen(
    monkeypatch, other_op_advances, expect_hang
):
    # Comm A runs two ops. AllReduce is skewed (mismatched) and frozen the whole time
    #   - other_op_frozen: AllGather is also frozen, so the communicator is fully
    #     stalled -> a hang, reported on the mismatched AllReduce op.
    #   - other_op_advancing: AllGather keeps advancing, so the ranks are alive
    #     and the stale AllReduce skew must NOT be treated as a hang.
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

    callback, captured_diagnostics = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    if expect_hang:
        callback.after_worker_group_poll_status(MagicMock())  # frozen -> 1/2
        assert callback.comm_deadlock_count == {_COMM_A: 1}
        with pytest.raises(NCCLHangError) as exc_info:
            callback.after_worker_group_poll_status(MagicMock())  # 2/2 -> raise
        assert "1 communicator(s)" in str(exc_info.value)
        assert captured_diagnostics.count(nccl_ras._STACK_TRACES_TOOL) == 1
    else:
        for _ in reports[1:]:
            callback.after_worker_group_poll_status(MagicMock())  # never a hang
        assert callback.comm_deadlock_count == {}
        assert not captured_diagnostics


def test_healthy_resets_deadlock_streak(monkeypatch):
    reports = [
        create_single_comm_report({1: 5, 2: 4}),  # baseline (mismatched)
        create_single_comm_report({1: 5, 2: 4}),  # mismatched -> 1/3
        create_healthy_report(),  # healthy -> reset
    ]
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=3, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())
    callback.after_worker_group_poll_status(MagicMock())
    assert callback.comm_deadlock_count == {_COMM_A: 1}
    callback.after_worker_group_poll_status(MagicMock())
    assert callback.comm_deadlock_count == {}


def test_advancing_never_deadlocks(monkeypatch):
    # Both ranks are skewed but keep advancing (nonzero deltas), so the op is
    # never frozen and no deadlock is ever counted.
    reports = [
        create_single_comm_report({1: 5, 2: 3}),
        create_single_comm_report({1: 9, 2: 5}),
        create_single_comm_report({1: 15, 2: 8}),
        create_single_comm_report({1: 20, 2: 14}),
    ]
    callback, captured_diagnostics = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    for _ in reports:
        callback.after_worker_group_poll_status(MagicMock())
        assert callback.comm_deadlock_count == {}
    assert not captured_diagnostics


def test_advancing_then_freeze_deadlocks(monkeypatch):
    # Advancing (skewed) for a while, then rank 3's op freezes and rank 2 stops
    # too -> the op is now frozen and the deadlock streak builds to a hang.
    reports = [
        create_single_comm_report({2: 5, 3: 3}),  # baseline, advancing
        create_single_comm_report({2: 8, 3: 6}),  # advancing
        create_single_comm_report({2: 10, 3: 6}),  # rank 3 froze, rank 2 still moved
        create_single_comm_report({2: 10, 3: 6}),  # fully frozen -> 1/2
        create_single_comm_report({2: 10, 3: 6}),  # fully frozen -> 2/2 -> raise
    ]
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    for _ in reports[:-1]:
        callback.after_worker_group_poll_status(MagicMock())
    assert callback.comm_deadlock_count == {_COMM_A: 1}
    with pytest.raises(NCCLHangError):
        callback.after_worker_group_poll_status(MagicMock())


def test_per_communicator_streak_is_independent(monkeypatch):
    # Communicator A deadlocks while B stays merely skewed but advancing. Only A
    # should be confirmed and named in the error.
    reports = [
        create_report(comms={_COMM_A: {2: 5, 3: 6}, _COMM_B: {0: 5, 1: 4}}),
        create_report(comms={_COMM_A: {2: 5, 3: 6}, _COMM_B: {0: 7, 1: 6}}),
        create_report(comms={_COMM_A: {2: 5, 3: 6}, _COMM_B: {0: 9, 1: 8}}),
    ]
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    callback.after_worker_group_poll_status(MagicMock())  # A frozen -> 1/2
    assert callback.comm_deadlock_count == {_COMM_A: 1}
    with pytest.raises(NCCLHangError) as exc_info:
        callback.after_worker_group_poll_status(MagicMock())  # A frozen -> 2/2
    # Only A is confirmed (1 of the 2 communicators), not B.
    assert "1 communicator(s)" in str(exc_info.value)


def test_communicator_added_after_two_polls(monkeypatch):
    # Comm A advances normally for the first two polls, then comm B appears on the
    # third poll already skewed and stays frozen. B cannot be flagged on the poll
    # it first appears (it has no prior baseline in the diff) and only starts its
    # own confirm streak once a baseline exists.
    reports = [
        create_report(comms={_COMM_A: {0: 2, 1: 2}}),  # baseline
        create_report(comms={_COMM_A: {0: 3, 1: 3}}),  # A advances, healthy
        create_report(comms={_COMM_A: {0: 4, 1: 4}, _COMM_B: {0: 7, 1: 5}}),  # B new
        create_report(comms={_COMM_A: {0: 5, 1: 5}, _COMM_B: {0: 7, 1: 5}}),  # B 1/2
        create_report(comms={_COMM_A: {0: 6, 1: 6}, _COMM_B: {0: 7, 1: 5}}),  # B 2/2
    ]
    callback, captured_diagnostics = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())  # baseline (A healthy)
    callback.after_worker_group_poll_status(MagicMock())  # A advances, healthy
    assert callback.comm_deadlock_count == {}
    callback.after_worker_group_poll_status(MagicMock())  # B appears, no baseline yet
    assert callback.comm_deadlock_count == {}
    callback.after_worker_group_poll_status(MagicMock())  # B frozen -> 1/2
    assert callback.comm_deadlock_count == {_COMM_B: 1}
    with pytest.raises(NCCLHangError) as exc_info:
        callback.after_worker_group_poll_status(MagicMock())  # B frozen -> 2/2
    # Only B is confirmed (1 of the 2 communicators present this poll).
    assert "1 communicator(s)" in str(exc_info.value)
    assert captured_diagnostics.count(nccl_ras._STACK_TRACES_TOOL) == 1


def test_communicator_removed_over_time(monkeypatch):
    # A communicator can disappear mid-run (e.g. its process group is destroyed).
    # Its in-progress deadlock streak is dropped, and a still-present comm keeps
    # being evaluated on its own.
    reports = [
        create_report(comms={_COMM_A: {2: 5, 3: 4}, _COMM_B: {0: 2, 1: 2}}),  # base
        create_report(comms={_COMM_A: {2: 5, 3: 4}, _COMM_B: {0: 3, 1: 3}}),  # A 1/2
        create_report(comms={_COMM_B: {0: 6, 1: 4}}),  # A gone; B skewed, advancing
        create_report(comms={_COMM_B: {0: 9, 1: 6}}),  # B still advancing
    ]
    callback, captured_diagnostics = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    callback.after_worker_group_poll_status(MagicMock())  # A frozen -> 1/2
    assert callback.comm_deadlock_count == {_COMM_A: 1}
    callback.after_worker_group_poll_status(MagicMock())  # A removed -> streak drops
    assert callback.comm_deadlock_count == {}
    callback.after_worker_group_poll_status(MagicMock())  # B keeps advancing
    assert callback.comm_deadlock_count == {}
    assert not captured_diagnostics


def test_no_new_report_is_noop(monkeypatch):
    # The controller polls (~2s) far more often than the poller publishes (15s).
    # A hook that finds nothing queued must leave detection state untouched.
    reports = [create_single_comm_report({2: 5, 3: 3})]
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=1, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())  # consumes the report
    first_report = callback.prev_report
    assert first_report is not None
    callback.after_worker_group_poll_status(MagicMock())  # poller published nothing
    assert callback.prev_report is first_report


def _drain(poller, n, timeout_s=5.0):
    got = []
    deadline = time.monotonic() + timeout_s
    while len(got) < n and time.monotonic() < deadline:
        item = poller.next_result()
        if item is None:
            time.sleep(0.005)
        else:
            got.append(item)
    return got


def _join(poller, timeout_s=5.0):
    poller._thread.join(timeout=timeout_s)
    return not poller.is_alive


def make_poller(query, interval_s=0.01):
    """A real RASPoller whose ``query`` is replaced by a fake (the transport is
    exercised by the GPU e2e tests)."""
    poller = RASPoller(MagicMock(), interval_s=interval_s)
    poller.query = query
    return poller


def test_ras_poller_publishes_reports_and_stops():
    # The thread publishes each successful poll; a non-fatal failure is logged
    # and polling continues; stop() ends the loop without joining.
    reports = [create_single_comm_report({2: 5, 3: 3})] * 3
    scripted = iter(reports)

    def query(fmt):
        assert fmt == "json"
        try:
            return next(scripted)
        except StopIteration:
            raise RASQueryError("exit_1", stderr="transient")

    poller = make_poller(query)
    poller.start()
    assert poller.is_alive
    assert _drain(poller, 3) == reports
    # Scripted reports exhausted -> every poll now fails non-fatally, thread lives.
    assert poller.is_alive

    poller.stop()
    assert _join(poller)
    assert poller.next_result() is None


def test_ras_poller_fatal_error_is_published_then_exits():
    fatal = RASQueryError("binary_not_found", "binary 'ncclras' not found", fatal=True)

    def query(fmt):
        raise fatal

    poller = make_poller(query)
    poller.start()
    (item,) = _drain(poller, 1)
    assert item is fatal
    assert _join(poller)


def test_ras_poller_survives_unexpected_query_error():
    calls = []
    report = create_single_comm_report({2: 5, 3: 3})

    def query(fmt):
        calls.append(fmt)
        if len(calls) == 1:
            raise RuntimeError("detector bug")
        return report

    poller = make_poller(query)
    poller.start()
    assert _drain(poller, 1) == [report]
    poller.stop()
    assert _join(poller)


def test_ras_poller_quiet_before_first_report_and_after_stop(caplog, propagate_logs):
    # Before NCCL starts there is no RAS service, so those failures are DEBUG;
    # once reports flow, a failure is INFO; after stop() nothing is logged.
    script = [
        RASQueryError("exit_1", stderr="before nccl"),
        create_single_comm_report({0: 1, 1: 1}),
        RASQueryError("exit_1", stderr="transient"),
    ]
    poller = make_poller(None)

    def query(fmt):
        if not script:
            poller.stop()
            raise RASQueryError("exit_1", stderr="after stop")
        item = script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    poller.query = query
    with caplog.at_level(logging.DEBUG, logger=nccl_ras.logger.name):
        poller._run()

    logged = [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if "poll produced no report" in record.getMessage()
    ]
    assert [(level, "before nccl" in msg) for level, msg in logged[:1]] == [
        (logging.DEBUG, True)
    ]
    assert [(level, "transient" in msg) for level, msg in logged[1:]] == [
        (logging.INFO, True)
    ]


def test_ras_poller_query_falls_back_to_next_worker():
    workers = [MagicMock(name="w0"), MagicMock(name="w1"), MagicMock(name="w2")]
    worker_group = MagicMock()
    worker_group.get_workers.return_value = workers
    poller = RASPoller(worker_group, interval_s=1.0)

    report = create_single_comm_report({2: 5, 3: 3})
    outcomes = {
        id(workers[0]): RASQueryError("query_timeout"),
        id(workers[1]): report,
    }

    def query_worker(worker, fmt):
        outcome = outcomes[id(worker)]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    poller._query_worker = query_worker
    assert poller.query("json") is report  # w0 failed, w1 answered, w2 not asked


def test_ras_poller_query_raises_last_error_when_all_workers_fail():
    workers = [MagicMock(), MagicMock()]
    worker_group = MagicMock()
    worker_group.get_workers.return_value = workers
    poller = RASPoller(worker_group, interval_s=1.0)

    errors = iter(
        [RASQueryError("query_timeout"), RASQueryError("binary_not_found", fatal=True)]
    )

    def query_worker(worker, fmt):
        raise next(errors)

    poller._query_worker = query_worker
    with pytest.raises(RASQueryError) as excinfo:
        poller.query("json")
    assert excinfo.value.reason == "binary_not_found" and excinfo.value.fatal


def test_ras_poller_query_no_workers():
    worker_group = MagicMock()
    worker_group.get_workers.return_value = []
    poller = RASPoller(worker_group, interval_s=1.0)
    with pytest.raises(RASQueryError) as excinfo:
        poller.query("json")
    assert excinfo.value.reason == "no_workers" and not excinfo.value.fatal


def test_callback_starts_and_stops_poller(monkeypatch):
    # The worker group start owns one poller; teardown stops it and a restart
    # gets a fresh one (never the stopped instance).
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=100, reports=[]
    )
    callback._poll_interval_s = 0.01

    callback.after_worker_group_start(MagicMock())
    first = callback._ras_poller
    assert isinstance(first, RASPoller) and first.is_alive

    callback.before_worker_group_shutdown(MagicMock())
    assert callback._ras_poller is None and callback._worker_group is None
    assert _join(first)

    callback.after_worker_group_start(MagicMock())
    second = callback._ras_poller
    assert second is not first and second.is_alive
    assert callback.prev_report is None and callback.comm_deadlock_count == {}
    callback.before_worker_group_shutdown(MagicMock())
    assert _join(second)


def test_callback_degrades_on_fatal_poll_result(monkeypatch):
    fatal = RASQueryError("unsupported_f_option", "binary rejected `-f`", fatal=True)
    callback, captured = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_count=1,
        reports=[fatal, create_single_comm_report({2: 5, 3: 3})],
    )

    callback.after_worker_group_poll_status(MagicMock())  # fatal -> degrade
    assert callback._is_ras_degraded is True
    callback.after_worker_group_poll_status(MagicMock())  # no-op once degraded
    assert callback.prev_report is None and not captured

    # A degraded callback does not start a poller for a later worker group.
    callback.after_worker_group_start(MagicMock())
    assert callback._ras_poller is None


def test_degraded_skips(monkeypatch):
    callback, _ = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_count=1,
        reports=[create_single_comm_report({2: 5, 3: 3})],
    )
    callback._is_ras_degraded = True
    callback.after_worker_group_poll_status(MagicMock())  # must not query/raise
    assert callback.prev_report is None
    assert callback.comm_deadlock_count == {}


def test_unexpected_error_disables_detection(monkeypatch):
    # A bug in the detection path must never crash training: it is logged and
    # detection is disabled for the rest of the run.
    callback, _ = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_count=1,
        reports=[create_single_comm_report({2: 5, 3: 3})],
    )

    def boom(*_args, **_kwargs):
        raise RuntimeError("detector bug")

    callback._ras_poller.next_result = boom
    callback.after_worker_group_poll_status(MagicMock())  # must not raise
    assert callback._is_ras_degraded is True
    # Once degraded, later polls are a no-op (guarded before next_result).
    callback.after_worker_group_poll_status(MagicMock())
    assert callback.prev_report is None


@pytest.mark.parametrize(
    "fail_stack_traces,fail_nvidia_smi,fail_ras_history",
    [
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (True, True, True),
    ],
    ids=["stack_traces_fail", "nvidia_smi_fails", "ras_history_fails", "both_fail"],
)
def test_hang_error_propagates_when_diagnostics_fail(
    monkeypatch, fail_stack_traces, fail_nvidia_smi, fail_ras_history
):
    # Collecting hang diagnostics must not suppress the hang failure, and a real
    # hang must not be misread as a detector bug. Each diagnostic fails on its
    # own, so the one that worked is still reported to the user.
    reports = [create_single_comm_report({2: 5, 3: 4})] * 3
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    def boom(*_args, **_kwargs):
        raise RuntimeError("diagnostic failed")

    if fail_stack_traces:
        callback.dump_workers_stack_traces = boom
    if fail_nvidia_smi:
        callback.dump_nodes_nvidia_smi = boom
    if fail_ras_history:
        callback.dump_ras_query_history = boom

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    callback.after_worker_group_poll_status(MagicMock())  # frozen -> 1/2
    with pytest.raises(NCCLHangError) as exc_info:
        callback.after_worker_group_poll_status(MagicMock())  # 2/2 -> still raises

    message = str(exc_info.value)
    assert ("Compare the stack traces" in message) is not fail_stack_traces
    assert ("drifted over the polls" in message) is not fail_ras_history
    assert ("`nvidia-smi` snapshots" in message) is not fail_nvidia_smi
    assert callback._is_ras_degraded is False


@pytest.mark.parametrize(
    "env",
    [
        {NCCL_RAS_ACTION_ENV_VAR: "not-a-mode"},
        {NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR: "0"},
        {NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR: "-1"},
        {NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR: "0"},
        {NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR: "-15"},
    ],
)
def test_invalid_config_fails_fast(monkeypatch, env):
    # Misconfigured env vars fail fast at construction with a clear ValueError.
    monkeypatch.setenv(NCCL_RAS_ACTION_ENV_VAR, NCCL_RAS_ACTION_FAIL)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(ValueError):
        NCCLRASCallback()


@pytest.mark.parametrize(
    "duration_s,interval_s,expected_polls",
    [
        # Defaults: 10 minutes at a 15s poll interval.
        (None, None, 40),
        # A shorter window is confirmed from proportionally fewer samples
        ("60", "15", 4),
        ("100", "15", 7),
    ],
)
def test_confirm_duration_converts_to_poll_count(
    monkeypatch, duration_s, interval_s, expected_polls
):
    # The public knob is a duration; the detector confirms on consecutive frozen
    # polls, so the duration is converted once at construction.
    if duration_s is not None:
        monkeypatch.setenv(NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR, duration_s)
    if interval_s is not None:
        monkeypatch.setenv(NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, interval_s)

    callback = NCCLRASCallback()
    assert callback._confirm_poll_counts == expected_polls
    # Escalation milestones stay below the confirmation streak so a short window
    # still warns before it fails.
    assert 1 <= callback._suspicion_polls <= max(1, expected_polls - 1)
    assert callback._periodic_warn_polls >= 1


def test_suspicion_and_periodic_messages_fail_mode(monkeypatch, caplog, propagate_logs):
    # A frozen communicator builds a streak. confirm_count is high enough that no
    # hang is confirmed, so we can observe the escalating warnings on the way up.
    # First-suspicion fires at 1 poll, periodic reminder every 2 (helper defaults).
    reports = [create_single_comm_report({1: 5, 2: 4})] * 4
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=100, reports=reports
    )

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for _ in reports:
            callback.after_worker_group_poll_status(MagicMock())

    text = caplog.text
    # New-suspicion announcement describes the stalled communicator.
    assert (
        "Possible NCCL hang detected by the Ray Train hang detector callback!" in text
    )
    assert f"Communicator {_COMM_A} over" in text
    # Periodic reminder uses the "still suspected" wording.
    assert "NCCL hang still suspected!" in text
    # Fail mode threatens to raise a NCCLHangError.
    assert "A NCCLHangError will be raised" in text
    # The RAS report is introduced so users know where it comes from, and is
    # logged only once for the whole suspected hang.
    assert text.count(FakePoller.TEXT_REPORT) == 1
    assert "NCCL's own `ncclras` report of the suspected hang" in text


def test_periodic_warning_never_precedes_the_first_suspicion(
    monkeypatch, caplog, propagate_logs
):
    # A periodic interval shorter than the first-suspicion threshold must not
    # say "still suspected" before anything was suspected, nor both at once.
    reports = [create_single_comm_report({1: 5, 2: 4})] * 8
    callback, _ = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_count=100,
        reports=reports,
        first_suspicion_polls=4,
        periodic_warn_every_polls=2,
    )

    warnings = []
    with caplog.at_level(logging.WARNING, logger=nccl_ras.logger.name):
        for _ in reports:
            caplog.clear()
            callback.after_worker_group_poll_status(MagicMock())
            warnings.append([r.getMessage().split("!")[0] for r in caplog.records])

    # Frozen streaks 1..7 follow the baseline poll.
    assert warnings == [
        [],
        [],
        [],
        [],
        ["Possible NCCL hang detected by the Ray Train hang detector callback"],
        [],
        ["NCCL hang still suspected"],
        [],
    ]


def test_ras_text_report_is_logged_again_for_a_new_hang(
    monkeypatch, caplog, propagate_logs
):
    stalled = create_single_comm_report({1: 5, 2: 4})
    reports = [stalled, stalled, create_healthy_report(), stalled, stalled]
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=100, reports=reports
    )

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for _ in reports:
            callback.after_worker_group_poll_status(MagicMock())

    assert caplog.text.count(FakePoller.TEXT_REPORT) == 2


# Per-poll states for the text report scenarios below: A and B are mismatched
# and frozen at fixed counts; "A moved" is A mismatched but progressing.
_A_STUCK = {_COMM_A: {1: 5, 2: 4}}
_A_MOVED = {_COMM_A: {1: 6, 2: 5}}
_B_IDLE = {_COMM_B: {1: 3, 2: 3}}
_B_STUCK = {_COMM_B: {1: 4, 2: 3}}


@pytest.mark.parametrize(
    "polls,text_polls",
    [
        # A is suspected at poll 1 and B, a second communicator, at poll 3
        # while A is still stalled: B's suspicion gets its own report.
        (
            [
                {**_A_STUCK, **_B_IDLE},
                {**_A_STUCK, **_B_IDLE},
                {**_A_STUCK, **_B_STUCK},
                {**_A_STUCK, **_B_STUCK},
                {**_A_STUCK, **_B_STUCK},
            ],
            [1, 3],
        ),
        # A and B stall on the same poll: one report covers both.
        ([{**_A_STUCK, **_B_STUCK}] * 4, [1]),
        # A recovers with no fully healthy poll in between (it is still
        # mismatched, just progressing), then stalls again: a new suspicion.
        ([_A_STUCK, _A_STUCK, _A_MOVED, _A_MOVED, _A_MOVED], [1, 3]),
        # A recovers through a healthy poll, then B stalls.
        ([_A_STUCK, _A_STUCK, {}, _B_STUCK, _B_STUCK], [1, 4]),
    ],
    ids=[
        "second_comm_later",
        "two_comms_same_poll",
        "recover_mismatched",
        "recover_healthy",
    ],
)
def test_ras_text_report_is_logged_once_per_suspicion(
    monkeypatch, caplog, propagate_logs, polls, text_polls
):
    reports = [create_report(comms) for comms in polls]
    callback, _ = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        confirm_count=100,
        reports=reports,
        periodic_warn_every_polls=1,
    )

    logged_at = []
    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for poll in range(len(reports)):
            caplog.clear()
            callback.after_worker_group_poll_status(MagicMock())
            if FakePoller.TEXT_REPORT in caplog.text:
                logged_at.append(poll)

    assert logged_at == text_polls


def test_ras_text_report_fetch_is_retried_while_suspected(
    monkeypatch, caplog, propagate_logs
):
    # A failed fetch at the first suspicion is retried on the next warning
    # rather than lost until the hang is confirmed.
    reports = [create_single_comm_report({1: 5, 2: 4})] * 4
    callback, _ = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        confirm_count=100,
        reports=reports,
        periodic_warn_every_polls=1,
    )
    fetches = iter([None, FakePoller.TEXT_REPORT, FakePoller.TEXT_REPORT])
    callback.fetch_ras_human_report = lambda: next(fetches)

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for _ in reports:
            callback.after_worker_group_poll_status(MagicMock())

    assert caplog.text.count(FakePoller.TEXT_REPORT) == 1


def test_escalation_absent_in_observe_mode(monkeypatch, caplog, propagate_logs):
    # Observe mode still surfaces the suspicion/periodic warnings but must never
    # threaten to raise an error, since it only observes.
    reports = [create_single_comm_report({1: 5, 2: 4})] * 4
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=100, reports=reports
    )

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for _ in reports:
            callback.after_worker_group_poll_status(MagicMock())

    text = caplog.text
    assert (
        "Possible NCCL hang detected by the Ray Train hang detector callback!" in text
    )
    assert "NCCL hang still suspected!" in text
    assert "NCCLHangError will be raised" not in text


@pytest.fixture
def uploads(monkeypatch):
    """Capture each upload as ``(fs_path, {filename: contents})``."""
    calls = []

    def fake_upload(local_dir, filesystem, fs_path):
        calls.append(
            (fs_path, {p.name: p.read_text() for p in Path(local_dir).iterdir()})
        )

    monkeypatch.setattr(nccl_ras, "_upload_to_fs_path", fake_upload)
    return calls


def make_diagnostics_callback(workers=None, experiment_fs_path="/exp"):
    """A callback wired to a worker group, to exercise the real dump methods."""
    callback = NCCLRASCallback()
    worker_group = MagicMock()
    worker_group.get_workers.return_value = (
        [make_worker(rank) for rank in (0, 1)] if workers is None else workers
    )
    worker_group._storage_context.experiment_fs_path = experiment_fs_path
    callback._worker_group = worker_group
    return callback


def test_ras_history_retains_the_confirm_window_plus_a_margin(monkeypatch):
    # The buffer has to reach back past the confirmation window, otherwise the
    # saved history only ever shows the communicator already stalled.
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=3, reports=[]
    )
    assert (
        callback.ras_history.maxlen
        == callback._confirm_poll_counts + nccl_ras._RAS_HISTORY_MARGIN_POLLS
        > callback._confirm_poll_counts
    )


def test_ras_history_records_every_poll_and_evicts_the_oldest(monkeypatch):
    # Every successful poll is recorded, healthy or not, and the buffer keeps
    # only the most recent maxlen of them.
    reports = [create_single_comm_report({1: count}) for count in range(20)]
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=100, reports=reports
    )
    monkeypatch.setattr(callback, "ras_history", deque(maxlen=3))

    for _ in reports:
        callback.after_worker_group_poll_status(MagicMock())

    assert list(callback.ras_history) == reports[-3:]


def test_ras_history_ignores_polls_that_produced_no_report(monkeypatch):
    # A poll the controller has no report for (the poller had nothing new)
    # must not advance the history or its file numbering.
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=100, reports=[]
    )

    callback.after_worker_group_poll_status(MagicMock())

    assert len(callback.ras_history) == 0


def test_ras_history_resets_with_the_worker_group(monkeypatch):
    # A new worker group is a new set of communicators, so the old group's polls
    # say nothing about it and the file numbering restarts.
    reports = [create_single_comm_report({1: 5})] * 2
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=100, reports=reports
    )

    for _ in reports:
        callback.after_worker_group_poll_status(MagicMock())
    assert len(callback.ras_history) == 2

    callback.reset_detection_state()
    assert len(callback.ras_history) == 0


def test_ras_history_uploads_one_file_per_retained_poll(uploads):
    # Each retained poll is written verbatim under a filename that carries its
    # poll number (so the files read in poll order even once the buffer has
    # wrapped) and the RAS timestamp, with the text report alongside them.
    callback = make_diagnostics_callback()
    callback.ras_history.extend(
        parse_ras_schema(ras_json)
        for ras_json in (HEALTHY_RAS_JSON, DEAD_RANK_RAS_JSON, MULTI_COMM_RAS_JSON)
    )

    fs_path = callback.dump_ras_query_history("human readable report")

    assert fs_path == "/exp/hang_detector/nccl_ras"
    ((uploaded_path, files),) = uploads
    assert uploaded_path == fs_path
    assert sorted(files) == [
        "ncclras_2026-06-19-06-51-56.json",
        "ncclras_2026-06-19-06-55-57.json",
        "ncclras_2026-06-19-06-56-24.json",
        "ncclras_report.txt",
    ]
    # The polls are the raw `ncclras` output, not what the detector parsed out
    # of it, so hosts, pids and missing ranks survive into the history.
    assert files["ncclras_2026-06-19-06-51-56.json"] == HEALTHY_RAS_JSON
    assert files["ncclras_report.txt"] == "human readable report"


def test_ras_history_upload_without_a_text_report(uploads):
    # The text report is a separate query that can fail while the history the
    # controller already holds is still worth writing.
    callback = make_diagnostics_callback()
    callback.ras_history.append(parse_ras_schema(HEALTHY_RAS_JSON))

    callback.dump_ras_query_history(None)

    ((_, files),) = uploads
    assert list(files) == ["ncclras_2026-06-19-06-51-56.json"]


def test_ras_history_upload_skipped_when_empty(uploads):
    # Nothing polled yet (the hang was confirmed on another signal): there is no
    # empty directory to leave behind in the user's experiment directory.
    assert make_diagnostics_callback().dump_ras_query_history() is None
    assert uploads == []


def make_stack_dump_callback(
    monkeypatch: pytest.MonkeyPatch,
    ranks: List[int],
    launch_errors: Optional[Dict[int, Exception]] = None,
    results: Optional[Dict[int, Union[str, Exception]]] = None,
):
    """A callback whose workers answer a stack dump with scripted outcomes.

    Args:
        monkeypatch: Used to stub out ``ray.wait``/``ray.get``.
        ranks: The world rank of each worker in the group.
        launch_errors: ``{rank: exception}`` raised by ``execute_async``.
        results: ``{rank: str | Exception}`` -- what ``ray.get`` returns for that
            rank's dump, or raises. A rank left out never becomes ready, which is
            how a dump that outlived the wait timeout looks.

    Returns:
        The callback and the ``{rank: worker}`` mapping it fans out to.
    """
    launch_errors = launch_errors or {}
    results = results or {}

    workers = {}
    for rank in ranks:
        worker = MagicMock()
        worker.distributed_context.world_rank = rank
        if rank in launch_errors:
            worker.execute_async.side_effect = launch_errors[rank]
        else:
            # Stands in for the ObjectRef the dump is launched with; the
            # rank it carries is what the stubbed ray.wait/ray.get key off.
            worker.execute_async.return_value = MagicMock(rank=rank)
        workers[rank] = worker

    callback = make_diagnostics_callback()
    callback._worker_group.get_workers.return_value = list(workers.values())

    def fake_wait(refs, num_returns, timeout):
        ready = [ref for ref in refs if ref.rank in results]
        return ready, [ref for ref in refs if ref not in ready]

    def fake_get(ref):
        result = results[ref.rank]
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(nccl_ras.ray, "wait", fake_wait)
    monkeypatch.setattr(nccl_ras.ray, "get", fake_get)
    return callback, workers


def test_stack_traces_upload_one_file_per_rank(monkeypatch, uploads):
    # The whole group is dumped in parallel and each rank's trace lands in its
    # own file, named by world rank so it can be matched to the RAS report.
    callback, workers = make_stack_dump_callback(
        monkeypatch,
        ranks=[0, 1, 2],
        results={rank: f"trace of rank {rank}" for rank in (0, 1, 2)},
    )

    fs_path = callback.dump_workers_stack_traces()

    assert fs_path == "/exp/hang_detector/stack_traces"
    ((uploaded_path, files),) = uploads
    assert uploaded_path == fs_path
    assert files == {f"rank_{rank}.log": f"trace of rank {rank}" for rank in (0, 1, 2)}
    # py-spy has to give up before the controller stops waiting, otherwise the
    # wait times out with nothing to show for it.
    for worker in workers.values():
        worker.execute_async.assert_called_once_with(
            nccl_ras.dump_stack_trace, nccl_ras._STACK_DUMP_TIMEOUT_S - 1
        )


def test_stack_traces_record_a_rank_that_could_not_be_launched(monkeypatch, uploads):
    # A worker that is unreachable (already dead, actor call refused) still gets
    # a file, so a rank missing from the diagnostics is never silent.
    callback, _ = make_stack_dump_callback(
        monkeypatch,
        ranks=[0, 1],
        launch_errors={1: RuntimeError("actor unavailable")},
        results={0: "trace of rank 0"},
    )

    callback.dump_workers_stack_traces()

    ((_, files),) = uploads
    assert files["rank_0.log"] == "trace of rank 0"
    assert files["rank_1.log"] == "actor unavailable"


def test_stack_traces_record_a_rank_that_timed_out(
    monkeypatch, uploads, caplog, propagate_logs
):
    # The hung rank is the interesting one, so a dump that never comes back must
    # not hold up (or drop) the traces the other ranks did return.
    callback, _ = make_stack_dump_callback(
        monkeypatch, ranks=[0, 1], results={0: "trace of rank 0"}
    )

    with caplog.at_level(logging.WARNING, logger=nccl_ras.logger.name):
        callback.dump_workers_stack_traces()

    ((_, files),) = uploads
    assert files["rank_0.log"] == "trace of rank 0"
    assert files["rank_1.log"] == (
        f"timed out after {nccl_ras._STACK_DUMP_TIMEOUT_S:.0f}s"
    )
    assert "dump_stack_trace on rank 1 did not finish" in caplog.text


def test_stack_traces_record_a_rank_whose_dump_failed(monkeypatch, uploads):
    # The dump was launched and returned, but as an error (the worker died while
    # dumping): report that against the rank rather than losing every trace.
    callback, _ = make_stack_dump_callback(
        monkeypatch,
        ranks=[0, 1],
        results={0: "trace of rank 0", 1: RuntimeError("worker died")},
    )

    callback.dump_workers_stack_traces()

    ((_, files),) = uploads
    assert files["rank_1.log"] == "worker died"


def test_stack_traces_skipped_without_workers(monkeypatch, uploads):
    # The group can be empty by the time the hang is confirmed (already torn
    # down): there is no empty directory to leave in the experiment directory.
    callback, _ = make_stack_dump_callback(monkeypatch, ranks=[])

    assert callback.dump_workers_stack_traces() is None
    assert uploads == []


def test_dump_stack_trace_falls_back_to_python_when_py_spy_is_missing(monkeypatch):
    # py-spy is an optional dependency; without it the native frames are lost but
    # the Python stacks of every thread are still worth having.
    def no_py_spy(*args, **kwargs):
        raise FileNotFoundError("py-spy")

    monkeypatch.setattr(nccl_ras.subprocess, "run", no_py_spy)

    trace = nccl_ras.dump_stack_trace(1.0)

    assert "py-spy unavailable: py-spy not installed" in trace
    assert "test_dump_stack_trace_falls_back_to_python_when_py_spy_is_missing" in trace


def test_dump_stack_trace_returns_the_py_spy_dump(monkeypatch):
    # The native dump is what shows a rank blocked inside NCCL, so it is returned
    # verbatim rather than summarised.
    monkeypatch.setattr(
        nccl_ras.subprocess,
        "run",
        lambda *args, **kwargs: MagicMock(
            returncode=0, stdout="native stack", stderr=""
        ),
    )

    assert nccl_ras.dump_stack_trace(1.0) == "native stack"


def test_capture_diagnostic_swallows_failures(caplog, propagate_logs):
    # A diagnostic that fails must never take the run's hang handling with it.
    def boom():
        raise RuntimeError("upload failed")

    with caplog.at_level(logging.ERROR, logger=nccl_ras.logger.name):
        assert NCCLRASCallback.capture_diagnostic("worker stack traces", boom) is None
    assert "worker stack traces" in caplog.text


def make_worker(rank, node_ip="10.0.0.1"):
    """A train worker stand-in; the diagnostics only need its rank and node."""
    return MagicMock(
        distributed_context=MagicMock(world_rank=rank),
        metadata=MagicMock(node_ip=node_ip),
    )


@pytest.fixture
def fan_out(monkeypatch):
    """Script what each worker does when a diagnostic fans out to it.

    Returns a namespace holding a ``worker(rank, ...)`` factory and the
    ``ray.wait`` mock the fan-out used. A worker returns ``value``, or instead
    fails its launch (``launch_error``), never finishes so the fan-out times out
    (``ready=False``), or fails when its finished call is collected
    (``get_error``).
    """
    pending, values, get_errors = set(), {}, {}

    def worker(
        rank, value=None, launch_error=None, get_error=None, ready=True, **kwargs
    ):
        worker = make_worker(rank, **kwargs)
        if launch_error is not None:
            worker.execute_async.side_effect = launch_error
            return worker
        # The mock's return value stands in for the call's ObjectRef.
        ref = worker.execute_async.return_value
        values[ref] = value
        if not ready:
            pending.add(ref)
        if get_error is not None:
            get_errors[ref] = get_error
        return worker

    def wait(refs, num_returns, timeout):
        assert num_returns == len(refs)
        return (
            [ref for ref in refs if ref not in pending],
            [ref for ref in refs if ref in pending],
        )

    def get(ref, timeout=None):
        if ref in get_errors:
            raise get_errors[ref]
        return values[ref]

    wait_mock = MagicMock(side_effect=wait)
    monkeypatch.setattr(nccl_ras.ray, "wait", wait_mock)
    monkeypatch.setattr(nccl_ras.ray, "get", get)
    return types.SimpleNamespace(worker=worker, wait=wait_mock)


def test_fan_out_collects_every_worker(fan_out):
    workers = [fan_out.worker(7, value="stack-7"), fan_out.worker(4, value="stack-4")]

    dumps = fan_out_to_workers(workers, dump_stack_trace, 25.0, timeout_s=30.0)

    # A dump is keyed by the worker's world rank, not its position in the list.
    assert {rank: dump.value for rank, dump in dumps.items()} == {
        7: "stack-7",
        4: "stack-4",
    }
    assert all(dump.error is None for dump in dumps.values())
    workers[0].execute_async.assert_called_once_with(dump_stack_trace, 25.0)
    # One wait for the whole fan-out, sharing the budget between the workers.
    assert fan_out.wait.call_args.kwargs["timeout"] == 30.0


@pytest.mark.parametrize(
    "broken,expected_type,expected_message",
    [
        (
            {"launch_error": RuntimeError("actor is dead")},
            RuntimeError,
            "actor is dead",
        ),
        ({"ready": False}, TimeoutError, "timed out after 30s"),
        ({"get_error": RuntimeError("worker exited")}, RuntimeError, "worker exited"),
    ],
    ids=["launch_failed", "timed_out", "collect_failed"],
)
def test_fan_out_records_per_rank_failures(
    fan_out, broken, expected_type, expected_message
):
    # One unreachable rank must not cost us the ranks that did answer: it gets an
    # error saying why, which is what ends up in that rank's file.
    workers = [fan_out.worker(0, **broken), fan_out.worker(1, value="stack-1")]

    dumps = fan_out_to_workers(workers, dump_stack_trace, 25.0, timeout_s=30.0)

    assert dumps[0].value is None
    assert isinstance(dumps[0].error, expected_type)
    assert expected_message in str(dumps[0].error)
    assert dumps[1].value == "stack-1" and dumps[1].error is None


def test_fan_out_keeps_a_returned_diagnostic_result(fan_out):
    # A worker-side function that reports its own failure must not be wrapped
    # as a successful value.
    error = RuntimeError("no GPUs")
    workers = [fan_out.worker(0, value=DiagnosticResult(error=error))]

    dumps = fan_out_to_workers(workers, run_nvidia_smi, 25.0, timeout_s=30.0)

    assert dumps[0].value is None and dumps[0].error is error


def fake_nvidia_smi(monkeypatch, **run_result):
    """Script the ``nvidia-smi`` subprocess, recording how it was invoked."""
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


def test_nvidia_smi_returns_the_report(monkeypatch):
    calls = fake_nvidia_smi(monkeypatch, stdout="GPU 00000000:00:04.0\n")

    assert run_nvidia_smi(25.0) == DiagnosticResult(value="GPU 00000000:00:04.0\n")
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
    fake_nvidia_smi(monkeypatch, **run_result)

    result = run_nvidia_smi(25.0)

    assert result.value is None
    assert isinstance(result.error, expected_type)
    assert expected_message in str(result.error)


def scripted_fan_out(monkeypatch, dumps):
    """Replace the worker fan-out with fixed dumps, recording how it was called."""
    calls = []

    def fake_fan_out(workers, fn, *fn_args, timeout_s):
        calls.append((workers, fn, fn_args, timeout_s))
        return dumps

    monkeypatch.setattr(nccl_ras, "fan_out_to_workers", fake_fan_out)
    return calls


def test_nvidia_smi_uploads_one_file_per_node(monkeypatch, uploads):
    # Two ranks share a node and a third is on its own: `nvidia-smi` reports
    # every GPU on a node, so the shared node must only be queried once.
    workers = [
        make_worker(0, node_ip="10.0.0.1"),
        make_worker(1, node_ip="10.0.0.1"),
        make_worker(2, node_ip="10.0.0.2"),
    ]
    callback = make_diagnostics_callback(workers)
    calls = scripted_fan_out(
        monkeypatch,
        {
            0: DiagnosticResult(value="node 1 GPUs"),
            2: DiagnosticResult(value="node 2 GPUs"),
        },
    )

    fs_path = callback.dump_nodes_nvidia_smi()

    assert fs_path == "/exp/hang_detector/nvidia_smi"
    assert uploads == [
        (
            fs_path,
            {"node_10.0.0.1.log": "node 1 GPUs", "node_10.0.0.2.log": "node 2 GPUs"},
        )
    ]
    ((queried, fn, fn_args, timeout_s),) = calls
    assert [worker.metadata.node_ip for worker in queried] == ["10.0.0.1", "10.0.0.2"]
    assert (fn, fn_args, timeout_s) == (
        nccl_ras.run_nvidia_smi,
        (nccl_ras._NVIDIA_SMI_TIMEOUT_S - 1,),
        nccl_ras._NVIDIA_SMI_TIMEOUT_S,
    )


@pytest.mark.parametrize(
    "bad_dump,expected_reason",
    [
        (
            DiagnosticResult(error=TimeoutError("timed out after 30s")),
            "timed out after 30s",
        ),
        (
            DiagnosticResult(error=RuntimeError("`nvidia-smi -q` exited 9")),
            "`nvidia-smi -q` exited 9",
        ),
    ],
    ids=["fan_out_failed", "nvidia_smi_failed"],
)
def test_nvidia_smi_failed_node_gets_placeholder(
    monkeypatch, uploads, bad_dump, expected_reason
):
    # A driver that will not answer is the hardware failure being looked for, so
    # the reason is the evidence and is written out like any other snapshot.
    workers = [make_worker(0, node_ip="10.0.0.1"), make_worker(1, node_ip="10.0.0.2")]
    callback = make_diagnostics_callback(workers)
    scripted_fan_out(
        monkeypatch,
        {0: bad_dump, 1: DiagnosticResult(value="GPUs")},
    )

    callback.dump_nodes_nvidia_smi()

    ((_, files),) = uploads
    assert expected_reason in files["node_10.0.0.1.log"]
    assert files["node_10.0.0.2.log"] == "GPUs"


def test_nvidia_smi_report_reaches_the_uploaded_file(monkeypatch, uploads):
    # The whole worker-side path: what `nvidia-smi` prints has to arrive intact
    # in the node's file.
    fake_nvidia_smi(monkeypatch, stdout="Driver Version : 580.65.06\n")
    callback = make_diagnostics_callback([make_worker(0, node_ip="10.0.0.1")])

    def local_fan_out(workers, fn, *fn_args, timeout_s):
        return {
            worker.distributed_context.world_rank: fn(*fn_args) for worker in workers
        }

    monkeypatch.setattr(nccl_ras, "fan_out_to_workers", local_fan_out)

    callback.dump_nodes_nvidia_smi()

    ((_, files),) = uploads
    assert files["node_10.0.0.1.log"] == "Driver Version : 580.65.06\n"


def test_stack_traces_upload_per_rank(monkeypatch, uploads):
    callback = make_diagnostics_callback()
    calls = scripted_fan_out(
        monkeypatch,
        {
            0: DiagnosticResult(value="stack 0"),
            1: DiagnosticResult(error=TimeoutError("timed out after 30s")),
        },
    )

    fs_path = callback.dump_workers_stack_traces()

    assert fs_path == "/exp/hang_detector/stack_traces"
    assert uploads == [
        (
            fs_path,
            {
                "rank_0.log": "stack 0",
                "rank_1.log": "timed out after 30s",
            },
        )
    ]
    ((_, fn, fn_args, timeout_s),) = calls
    assert (fn, fn_args, timeout_s) == (
        nccl_ras.dump_stack_trace,
        (nccl_ras._STACK_DUMP_TIMEOUT_S - 1,),
        nccl_ras._STACK_DUMP_TIMEOUT_S,
    )


def make_train_worker(world_rank: int, node_ip: str, pid: int) -> MagicMock:
    worker = MagicMock()
    worker.distributed_context.world_rank = world_rank
    worker.metadata.node_ip = node_ip
    worker.metadata.pid = pid
    return worker


@pytest.mark.parametrize(
    "comm_ranks,train_rank_of,expected",
    [
        ([0], {0: 2}, "train rank 2"),
        ([0, 1, 2], {0: 3, 1: 0, 2: 1}, "train ranks 0-1, 3"),
        (range(1000), {r: r for r in range(1000)}, "train ranks 0-999"),
        ([0, 1], {}, "comm ranks 0-1 (no matching train worker)"),
        ([0, 4], {0: 1}, "train rank 1, comm rank 4 (no matching train worker)"),
        # Past _MAX_RANKS_LOGGED runs the rest are counted, not named.
        (
            range(20),
            {r: 2 * r for r in range(20)},
            "train ranks 0, 2, 4, 6, 8, 10, 12, 14, and 12 more",
        ),
    ],
)
def test_format_ranks(comm_ranks, train_rank_of, expected):
    assert nccl_ras.format_ranks(comm_ranks, train_rank_of) == expected


def test_match_train_ranks_matches_node_and_pid():
    # Pids repeat across nodes, so the node IP decides which worker it is.
    workers = [
        make_train_worker(0, "10.0.0.1", 100),
        make_train_worker(1, "10.0.0.2", 100),
        make_train_worker(2, "10.0.0.2", 200),
    ]
    processes = {
        0: ("10.0.0.2", 100),
        # RAS on another interface: the pid alone is enough when it is unique...
        1: ("192.168.0.2", 200),
        # ...but not when two nodes have a worker with that pid.
        2: ("192.168.0.2", 100),
        3: ("10.0.0.3", 300),
    }

    assert nccl_ras.translate_ras_ranks_to_train(workers, processes) == {0: 1, 1: 2}


def make_translating_callback(monkeypatch, action, reports, workers):
    callback, _ = make_nccl_ras_callback(
        monkeypatch, action, confirm_count=2, reports=reports
    )
    callback._worker_group.get_workers.return_value = workers
    return callback


# Comm ranks 0 and 1 are Ray Train ranks 2 and 3 on two nodes.
_PAIR_PROCESSES = {_COMM_A: {0: ("10.0.0.1", 100), 1: ("10.0.0.2", 200)}}
_PAIR_WORKERS = [
    make_train_worker(2, "10.0.0.1", 100),
    make_train_worker(3, "10.0.0.2", 200),
]


def test_confirmed_hang_names_the_train_ranks(monkeypatch):
    reports = [create_report({_COMM_A: {0: 5, 1: 4}}, processes=_PAIR_PROCESSES)] * 3
    callback = make_translating_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, reports, _PAIR_WORKERS
    )

    callback.after_worker_group_poll_status(MagicMock())
    callback.after_worker_group_poll_status(MagicMock())
    with pytest.raises(NCCLHangError) as exc_info:
        callback.after_worker_group_poll_status(MagicMock())

    # Comm ranks are translated to train ranks, without blaming either side.
    assert (
        f"Communicator {_COMM_A} over train ranks 2-3, no progress for 2s. "
        "AllReduce launches: 5 by train rank 2; 4 by train rank 3"
    ) in str(exc_info.value)
    assert exc_info.value.worker_failures == {}


def test_only_mismatched_ops_are_described(monkeypatch):
    report = create_single_comm_report(
        {
            0: {"AllGather": 3, "AllReduce": 9, "Broadcast": 1},
            1: {"AllGather": 2, "AllReduce": 9, "Broadcast": 1},
            2: {"AllGather": 3, "AllReduce": 7, "Broadcast": 1},
        }
    )
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2, reports=[]
    )
    callback._worker_group.get_workers.return_value = []

    line = callback.describe_stalled_comms(report, [_COMM_A])
    assert "AllGather launches: 3 by comm ranks 0, 2" in line
    assert "AllReduce launches: 9 by comm ranks 0-1" in line
    assert "Broadcast" not in line


def test_suspicion_warnings_name_the_train_ranks(monkeypatch, caplog, propagate_logs):
    reports = [create_report({_COMM_A: {0: 5, 1: 4}}, processes=_PAIR_PROCESSES)] * 4
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=100, reports=reports
    )
    callback._worker_group.get_workers.return_value = _PAIR_WORKERS

    with caplog.at_level(logging.WARNING, logger=nccl_ras.logger.name):
        for _ in reports:
            callback.after_worker_group_poll_status(MagicMock())

    warnings = [record.getMessage() for record in caplog.records]
    assert len(warnings) == 2
    for message in warnings:
        assert "5 by train rank 2; 4 by train rank 3" in message


def test_unmatched_rank_falls_back_to_the_ras_process(monkeypatch):
    # A rank RAS knows but Ray Train doesn't (e.g. a user-spawned subprocess)
    # is still named by where RAS says it runs.
    reports = [
        create_report(
            {_COMM_A: {0: 5, 1: 4}},
            processes={_COMM_A: {0: ("10.0.0.1", 100), 1: ("10.0.0.9", 999)}},
        )
    ] * 3
    callback = make_translating_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, reports, _PAIR_WORKERS
    )

    callback.after_worker_group_poll_status(MagicMock())
    callback.after_worker_group_poll_status(MagicMock())
    with pytest.raises(NCCLHangError) as exc_info:
        callback.after_worker_group_poll_status(MagicMock())

    assert "4 by comm rank 1 (no matching train worker)" in str(exc_info.value)


def test_describing_ranks_never_raises(monkeypatch):
    # Missing log detail must never disable detection.
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2, reports=[]
    )
    callback._worker_group.get_workers.side_effect = RuntimeError("boom")
    report = create_single_comm_report({0: 5, 1: 4})

    assert callback.describe_stalled_comms(report, [_COMM_A]) == ""


# ---------------------------------------------------------------------------
# Exported hang events
# ---------------------------------------------------------------------------

SUS = NcclHangTrigger.SUSPECTED
CON = NcclHangTrigger.CONFIRMED
REC = NcclHangTrigger.RECOVERED
ACTIVE = NcclHangDetectorStatus.ACTIVE
STOPPED = NcclHangDetectorStatus.STOPPED


class FakeClock:
    """Wall clock for the detector: each poll is taken 1 s after the last."""

    def __init__(self):
        self.now = 1000.0

    def time(self):
        return self.now


def make_exporting_callback(monkeypatch, action, reports, **kwargs):
    """A callback at the production escalation ratios (suspicion at 4 polls,
    a periodic warning every 8, confirmation at 40) with run ids set and a
    clock that :func:`poll` advances by the 1 s poll interval."""
    kwargs.setdefault("confirm_count", 40)
    kwargs.setdefault("first_suspicion_polls", 4)
    kwargs.setdefault("periodic_warn_every_polls", 8)
    callback, _ = make_nccl_ras_callback(monkeypatch, action, reports=reports, **kwargs)
    callback._run_id = "run"
    callback._attempt_id = "attempt"
    callback._worker_group._storage_context.experiment_fs_path = "s3://bucket/run"
    callback.clock = FakeClock()
    monkeypatch.setattr(
        nccl_ras,
        "time",
        types.SimpleNamespace(time=callback.clock.time, monotonic=time.monotonic),
    )
    return callback


def poll(callback, times):
    for _ in range(times):
        if hasattr(callback, "clock"):
            callback.clock.now += 1.0
        callback.after_worker_group_poll_status(MagicMock())


def summary(events):
    """``(poll_index, status, triggers, [(comm, state, stall_duration_s)])``."""
    return [
        (
            e.poll_index,
            e.detector_status,
            e.triggers,
            [(c.communicator_id, c.state, c.stall_duration_s) for c in e.communicators],
        )
        for e in events
    ]


def assert_poll_indices_increase(events):
    indices = [e.poll_index for e in events]
    assert indices == sorted(set(indices)) and indices[0] >= 1


A_SUS = NcclCommunicatorHangState.SUSPECTED
A_CON = NcclCommunicatorHangState.CONFIRMED
A_REC = NcclCommunicatorHangState.RECOVERED


@pytest.mark.parametrize("action", [NCCL_RAS_ACTION_OBSERVE, NCCL_RAS_ACTION_FAIL])
def test_stall_that_confirms(monkeypatch, emitted, action):
    # Poll 1 is the baseline, poll p has p - 1 frozen polls: suspected at 5,
    # still suspected at 13, 21, 29, 37, confirmed at 41, and in observe mode
    # the periodic warnings go on at 45, 53, 61 with the comm still CONFIRMED.
    reports = [create_single_comm_report({0: 5, 1: 4})] * 61
    callback = make_exporting_callback(monkeypatch, action, reports)

    if action == NCCL_RAS_ACTION_FAIL:
        poll(callback, 40)
        with pytest.raises(NCCLHangError):
            poll(callback, 1)
        callback.before_worker_group_shutdown(MagicMock())
    else:
        poll(callback, 61)

    expected = [
        (5, ACTIVE, [SUS], [(_COMM_A, A_SUS, 4.0)]),
        (13, ACTIVE, [SUS], [(_COMM_A, A_SUS, 12.0)]),
        (21, ACTIVE, [SUS], [(_COMM_A, A_SUS, 20.0)]),
        (29, ACTIVE, [SUS], [(_COMM_A, A_SUS, 28.0)]),
        (37, ACTIVE, [SUS], [(_COMM_A, A_SUS, 36.0)]),
        (41, ACTIVE, [CON], [(_COMM_A, A_CON, 40.0)]),
    ]
    if action == NCCL_RAS_ACTION_FAIL:
        # Exported before the raise; STOPPED when the worker group shuts down.
        expected.append((42, STOPPED, [], [(_COMM_A, A_CON, 40.0)]))
    else:
        expected += [
            (45, ACTIVE, [SUS], [(_COMM_A, A_CON, 44.0)]),
            (53, ACTIVE, [SUS], [(_COMM_A, A_CON, 52.0)]),
            (61, ACTIVE, [SUS], [(_COMM_A, A_CON, 60.0)]),
        ]
    assert summary(emitted) == expected
    assert_poll_indices_increase(emitted)

    for event in emitted:
        assert (event.run_id, event.attempt_id) == ("run", "attempt")
        [comm] = event.communicators
        assert comm.stalled_since_ms == 1_001_000  # the baseline poll
        assert (
            event.timestamp_ms - comm.stalled_since_ms == comm.stall_duration_s * 1000
        )
        assert comm.stalled_ranks == [
            StalledRanks(op="AllReduce", launch_count=5, world_ranks=[]),
            StalledRanks(op="AllReduce", launch_count=4, world_ranks=[]),
        ]
    # The report is attached on the polls that fetched it: the first suspicion
    # and the confirmation (fetched once, for the history and the event).
    assert [e.ras_text_report for e in emitted] == [FakePoller.TEXT_REPORT] + [
        None
    ] * 4 + [FakePoller.TEXT_REPORT] + [None] * (len(emitted) - 6)
    assert callback._ras_poller is None or callback._ras_poller.text_queries == 2

    confirmed = emitted[5]
    assert confirmed.diagnostics_dir == f"s3://bucket/run/{HANG_DETECTOR_DIRNAME}"
    assert confirmed.message.startswith("NCCL hang detected")
    assert "\n" not in confirmed.message
    assert [e.diagnostics_dir is not None for e in emitted].count(True) == 1
    assert emitted[0].message.startswith("Possible NCCL hang detected")
    assert emitted[1].message.startswith("NCCL hang still suspected")
    assert all(e.stop_reason is None for e in emitted if e.detector_status == ACTIVE)
    if action == NCCL_RAS_ACTION_FAIL:
        assert emitted[-1].stop_reason == NcclHangStopReason.WORKER_GROUP_SHUTDOWN


def test_fail_mode_retry_starts_a_new_attempt(monkeypatch, emitted):
    reports = [create_single_comm_report({0: 5, 1: 4})] * 3
    callback = make_exporting_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        reports,
        confirm_count=2,
        first_suspicion_polls=1,
    )
    poll(callback, 2)
    with pytest.raises(NCCLHangError):
        poll(callback, 1)
    callback.before_worker_group_shutdown(MagicMock())
    assert [(e.poll_index, e.detector_status, e.triggers) for e in emitted] == [
        (2, ACTIVE, [SUS]),
        (3, ACTIVE, [CON]),
        (4, STOPPED, []),
    ]

    # The retry is a new attempt that stays healthy: no events, and its polls
    # are numbered from 1 again.
    worker_group = MagicMock()
    worker_group._train_run_context.run_id = "run"
    worker_group.get_worker_group_context.return_value.run_attempt_id = "attempt_2"
    monkeypatch.setattr(
        nccl_ras, "RASPoller", lambda *_: FakePoller([create_healthy_report()] * 5)
    )
    callback.after_worker_group_start(worker_group)
    poll(callback, 5)
    callback.before_worker_group_shutdown(worker_group)
    assert len(emitted) == 3
    assert callback._poll_index == 5


def test_stall_is_dated_from_the_poll_before_its_first_frozen_poll(
    monkeypatch, emitted
):
    callback = make_exporting_callback(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        [create_single_comm_report({0: 5, 1: 4})] * 5,
    )
    poll(callback, 5)

    [suspected] = emitted
    [comm] = suspected.communicators
    assert suspected.timestamp_ms == 1_005_000
    assert comm.stalled_since_ms == 1_001_000
    assert comm.stall_duration_s == 4.0


def test_confirmed_without_diagnostics_has_no_directory(monkeypatch, emitted):
    reports = [create_single_comm_report({0: 5, 1: 4})] * 3
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2, reports=reports
    )
    for capture in (
        "dump_workers_stack_traces",
        "dump_nodes_nvidia_smi",
        "dump_ras_query_history",
    ):
        setattr(callback, capture, lambda *_: None)
    poll(callback, 3)
    assert emitted[-1].triggers == [CON] and emitted[-1].diagnostics_dir is None


@pytest.mark.parametrize("progress", ["healthy", "advancing"])
def test_stall_that_recovers(monkeypatch, emitted, progress):
    stalled = create_single_comm_report({0: 5, 1: 4})
    resumed = (
        create_healthy_report()
        if progress == "healthy"
        else create_single_comm_report({0: 6, 1: 5})
    )
    reports = [stalled] * 14 + [resumed] * 3
    callback = make_exporting_callback(monkeypatch, NCCL_RAS_ACTION_OBSERVE, reports)

    poll(callback, 17)
    callback.before_worker_group_shutdown(MagicMock())

    # Shown once as RECOVERED with its final duration (it was last seen frozen
    # at poll 14); nothing after, not even STOPPED.
    assert summary(emitted) == [
        (5, ACTIVE, [SUS], [(_COMM_A, A_SUS, 4.0)]),
        (13, ACTIVE, [SUS], [(_COMM_A, A_SUS, 12.0)]),
        (15, ACTIVE, [REC], [(_COMM_A, A_REC, 13.0)]),
    ]
    recovered = emitted[-1]
    assert recovered.communicators[0].stalled_since_ms == 1_001_000
    assert recovered.communicators[0].stalled_ranks == []
    assert recovered.ras_text_report is None
    assert "resumed making progress" in recovered.message


def test_stall_below_the_suspicion_threshold_exports_nothing(monkeypatch, emitted):
    stalled = create_single_comm_report({0: 5, 1: 4})
    reports = [stalled] * 4 + [create_healthy_report()]
    callback = make_exporting_callback(monkeypatch, NCCL_RAS_ACTION_OBSERVE, reports)
    poll(callback, 5)
    callback.before_worker_group_shutdown(MagicMock())
    assert emitted == []


def test_new_episode_after_recovery_is_suspected_again(monkeypatch, emitted):
    # A communicator confirmed in observe mode that recovers and stalls again
    # starts a new episode: it is suspected again, with a new start time.
    stalled = create_single_comm_report({0: 5, 1: 4})
    stalled_later = create_single_comm_report({0: 9, 1: 8})
    reports = [stalled] * 3 + [stalled_later] * 2
    callback = make_exporting_callback(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        reports,
        confirm_count=2,
        first_suspicion_polls=1,
    )
    poll(callback, 5)

    assert summary(emitted) == [
        (2, ACTIVE, [SUS], [(_COMM_A, A_SUS, 1.0)]),
        (3, ACTIVE, [CON], [(_COMM_A, A_CON, 2.0)]),
        (4, ACTIVE, [REC], [(_COMM_A, A_REC, 2.0)]),
        (5, ACTIVE, [SUS], [(_COMM_A, A_SUS, 1.0)]),
    ]
    assert emitted[3].communicators[0].stalled_since_ms == 1_004_000
    assert emitted[3].ras_text_report == FakePoller.TEXT_REPORT


def test_failed_report_fetch_moves_the_report_to_the_next_warning(monkeypatch, emitted):
    reports = [create_single_comm_report({0: 5, 1: 4})] * 21
    callback = make_exporting_callback(monkeypatch, NCCL_RAS_ACTION_OBSERVE, reports)
    fetches = iter([None, FakePoller.TEXT_REPORT])
    callback.fetch_ras_human_report = lambda: next(fetches)

    poll(callback, 21)

    assert [(e.poll_index, e.ras_text_report) for e in emitted] == [
        (5, None),
        (13, FakePoller.TEXT_REPORT),
        (21, None),
    ]


@pytest.mark.parametrize("action", [NCCL_RAS_ACTION_OBSERVE, NCCL_RAS_ACTION_FAIL])
def test_staggered_stalls_confirm_and_suspect_on_one_poll(
    monkeypatch, emitted, caplog, propagate_logs, action
):
    # Comm A stalls from the first diff; comm B progresses for 4 more polls,
    # so it first becomes suspected on the very poll A is confirmed. Both are
    # reported in that one event, and B's suspicion is logged.
    def report(i):
        b_count = 4 + min(i, 4)
        return create_report(
            {_COMM_A: {0: 5, 1: 4}, _COMM_B: {0: b_count + 1, 1: b_count}}
        )

    reports = [report(i) for i in range(13)]
    callback = make_exporting_callback(
        monkeypatch, action, reports, confirm_count=8, periodic_warn_every_polls=8
    )
    with caplog.at_level(logging.WARNING, logger=nccl_ras.logger.name):
        if action == NCCL_RAS_ACTION_FAIL:
            poll(callback, 8)
            with pytest.raises(NCCLHangError):
                poll(callback, 1)
        else:
            poll(callback, 13)

    expected = [
        (5, ACTIVE, [SUS], [(_COMM_A, A_SUS, 4.0)]),
        (9, ACTIVE, [CON, SUS], [(_COMM_A, A_CON, 8.0), (_COMM_B, A_SUS, 4.0)]),
    ]
    if action == NCCL_RAS_ACTION_OBSERVE:
        # B is confirmed in turn, A staying CONFIRMED. (A's periodic warning
        # falls on this poll too, but a confirming poll never logs one.)
        expected.append(
            (13, ACTIVE, [CON], [(_COMM_A, A_CON, 12.0), (_COMM_B, A_CON, 8.0)])
        )
    assert summary(emitted) == expected

    confirm_poll = emitted[1]
    # One report on the poll, fetched once for the confirmation and B.
    assert confirm_poll.ras_text_report == FakePoller.TEXT_REPORT
    assert callback._ras_poller.text_queries == len(expected)
    assert confirm_poll.message.startswith("NCCL hang detected")
    assert "Possible NCCL hang detected" in confirm_poll.message
    assert f"Communicator {_COMM_B} over" in caplog.text
    # No "will be raised in 0 seconds" for a suspicion on the confirming poll.
    assert "raised in 0 seconds" not in caplog.text


def test_one_report_per_poll_for_communicators_suspected_together(monkeypatch, emitted):
    reports = [create_report({_COMM_A: {0: 5, 1: 4}, _COMM_B: {0: 2, 1: 1}})] * 5
    callback = make_exporting_callback(monkeypatch, NCCL_RAS_ACTION_OBSERVE, reports)
    poll(callback, 5)

    [event] = emitted
    assert [c.communicator_id for c in event.communicators] == [_COMM_A, _COMM_B]
    assert event.ras_text_report == FakePoller.TEXT_REPORT
    assert callback._ras_poller.text_queries == 1


def test_worker_group_shutdown_exports_stopped(monkeypatch, emitted):
    # Comm A is past the suspicion threshold, comm B is frozen below it and
    # comm C is healthy: only A is open at the stop.
    def report(i):
        b_count = 4 + min(i, 3)
        return create_report(
            {
                _COMM_A: {0: 5, 1: 4},
                _COMM_B: {0: b_count + 1, 1: b_count},
                "0xc": {0: 1, 1: 1},
            }
        )

    reports = [report(i) for i in range(6)]
    callback = make_exporting_callback(monkeypatch, NCCL_RAS_ACTION_OBSERVE, reports)
    poll(callback, 6)
    assert callback.comm_deadlock_count == {_COMM_A: 5, _COMM_B: 2}
    emitted.clear()

    callback.clock.now += 0.5
    callback.before_worker_group_shutdown(MagicMock())

    [stopped] = emitted
    assert summary(emitted) == [(7, STOPPED, [], [(_COMM_A, A_SUS, 5.5)])]
    assert stopped.stop_reason == NcclHangStopReason.WORKER_GROUP_SHUTDOWN
    assert stopped.timestamp_ms == 1_006_500
    assert stopped.ras_text_report is None and stopped.diagnostics_dir is None
    # The last report's stalled ranks are kept.
    assert stopped.communicators[0].stalled_ranks
    # At most once per attempt.
    callback.before_worker_group_shutdown(MagicMock())
    assert len(emitted) == 1


def test_fatal_poll_result_exports_stopped(monkeypatch, emitted):
    fatal = RASQueryError("binary_not_found", fatal=True)
    reports = [create_single_comm_report({0: 5, 1: 4})] * 5 + [fatal]
    callback = make_exporting_callback(monkeypatch, NCCL_RAS_ACTION_OBSERVE, reports)
    poll(callback, 6)

    assert callback._is_ras_degraded is True
    assert summary(emitted) == [
        (5, ACTIVE, [SUS], [(_COMM_A, A_SUS, 4.0)]),
        (6, STOPPED, [], [(_COMM_A, A_SUS, 5.0)]),
    ]
    assert emitted[-1].stop_reason == NcclHangStopReason.DETECTOR_DISABLED
    # Nothing more when the worker group shuts down.
    callback.before_worker_group_shutdown(MagicMock())
    assert len(emitted) == 2


def test_unexpected_error_exports_stopped(monkeypatch, emitted):
    reports = [create_single_comm_report({0: 5, 1: 4})] * 6
    callback = make_exporting_callback(monkeypatch, NCCL_RAS_ACTION_OBSERVE, reports)
    poll(callback, 5)

    def boom(*_args, **_kwargs):
        raise RuntimeError("detector bug")

    callback.evaluate_comm_mismatch = boom
    poll(callback, 1)

    assert callback._is_ras_degraded is True
    assert [(e.poll_index, e.detector_status) for e in emitted] == [
        (5, ACTIVE),
        (7, STOPPED),
    ]
    assert emitted[-1].stop_reason == NcclHangStopReason.DETECTOR_DISABLED


@pytest.mark.parametrize("action", [NCCL_RAS_ACTION_OBSERVE, NCCL_RAS_ACTION_FAIL])
def test_failing_export_never_affects_detection(monkeypatch, action):
    def broken_emit(self, event):
        raise RuntimeError("state actor unavailable")

    monkeypatch.setattr(NCCLRASCallback, "_emit", broken_emit)
    reports = [create_single_comm_report({0: 5, 1: 4})] * 3 + [create_healthy_report()]
    callback, captured = make_nccl_ras_callback(
        monkeypatch, action, confirm_count=2, reports=reports, first_suspicion_polls=1
    )

    poll(callback, 2)
    if action == NCCL_RAS_ACTION_FAIL:
        with pytest.raises(NCCLHangError):
            poll(callback, 1)
    else:
        poll(callback, 2)  # confirm, then recover
    callback.before_worker_group_shutdown(MagicMock())

    assert callback._is_ras_degraded is False
    assert captured == _CONFIRMED_HANG_DIAGNOSTICS


def test_exported_stalled_ranks_are_train_world_ranks(monkeypatch, emitted):
    # Comm ranks 0 and 1 are train ranks 2 and 3; comm rank 2 has no train
    # worker, so it's named in the log but left out of the event.
    processes = {_COMM_A: {**_PAIR_PROCESSES[_COMM_A], 2: ("10.0.0.9", 9)}}
    reports = [create_report({_COMM_A: {0: 5, 1: 4, 2: 5}}, processes=processes)] * 2
    callback = make_translating_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, reports, _PAIR_WORKERS
    )
    poll(callback, 2)

    [suspected] = emitted
    assert suspected.communicators[0].stalled_ranks == [
        StalledRanks(op="AllReduce", launch_count=5, world_ranks=[2]),
        StalledRanks(op="AllReduce", launch_count=4, world_ranks=[3]),
    ]


def test_worker_group_start_reads_the_run_and_attempt_ids(monkeypatch):
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=100, reports=[]
    )
    monkeypatch.setattr(nccl_ras, "RASPoller", lambda *_: FakePoller([]))
    worker_group = MagicMock()
    worker_group._train_run_context.run_id = "run_1"
    worker_group.get_worker_group_context.return_value.run_attempt_id = "attempt_1"

    callback.after_worker_group_start(worker_group)
    assert (callback._run_id, callback._attempt_id) == ("run_1", "attempt_1")

    # A restart is a new attempt of the same run.
    worker_group.get_worker_group_context.return_value.run_attempt_id = "attempt_2"
    callback.before_worker_group_shutdown(worker_group)
    callback.after_worker_group_start(worker_group)
    assert (callback._run_id, callback._attempt_id) == ("run_1", "attempt_2")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
