"""Unit tests for the NCCL RAS hang-detection callback (no GPU required)."""
import json
import logging
import sys
import time
from typing import Dict, Optional, Union
from unittest.mock import MagicMock

import pytest

from ray.train.v2._internal.callbacks import nccl_ras
from ray.train.v2._internal.callbacks.nccl_ras import (
    NCCLRASCallback,
    RASPoller,
    RASQueryError,
    RASReport,
    parse_ras_addr,
    parse_ras_schema,
)
from ray.train.v2._internal.constants import (
    NCCL_RAS_ACTION_ENV_VAR,
    NCCL_RAS_ACTION_FAIL,
    NCCL_RAS_ACTION_OBSERVE,
    NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR,
    NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR,
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
):
    """Build a callback whose poller yields the given sequence of poll results.

    The detector confirms hangs on consecutive frozen polls but is configured in
    seconds, so a 1s poll interval makes every "seconds" knob equal a poll count:
    ``confirm_count`` polls, and the escalation milestones (tuned for production
    at 60s/120s, i.e. 4 and 8 polls) shrunk to values the small
    ``confirm_count``s here actually reach.

    No poller thread is started: a :class:`FakePoller` returns one scripted
    result per controller tick, so the tests stay deterministic.
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
    callback.dump_workers_stack_traces = lambda: captured.append(True) or "/tmp/dump"

    return callback, captured


_COMM_A = "0x2b9ffd12ea17b069"
_COMM_B = "0xced5b798f46495a3"

# A rank's spec is either an AllReduce count (int) or an explicit op->count map.
_RankCounts = Dict[int, Union[int, Dict[str, int]]]


def create_report(comms: Optional[Dict[str, _RankCounts]] = None):
    """Build a RASReport from ``{comm_id: {global_rank: counts}}`` specs.

    A rank's ``counts`` is either an ``AllReduce`` count (int) or an explicit
    ``{op_name: count}`` mapping. Every rank is reported as RUNNING.
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
    )


def create_single_comm_report(counts, comm_id=_COMM_A):
    """One communicator with ``{global_rank: AllReduce count}``."""
    return create_report(comms={comm_id: counts})


def create_healthy_report():
    """A report with no communicators, so nothing is ever mismatched."""
    return create_report()


def test_observe_mode_never_raises(monkeypatch):
    # A confirmed hang in observe mode never raises. Observe mode currently does
    # not collect stack-trace diagnostics either (only the fail action does).
    reports = [create_single_comm_report({1: 5, 2: 4})] * 3
    callback, captured_stack_traces = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2, reports=reports
    )

    for _ in reports:
        callback.after_worker_group_poll_status(MagicMock())  # must not raise
    assert len(captured_stack_traces) == 1


def test_fail_mode_raises_after_confirm(monkeypatch):
    # rank 3 frozen behind rank 2 with no progress on either -> a deadlock.
    reports = [create_single_comm_report({1: 5, 2: 4})] * 3
    callback, captured_stack_traces = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    callback.after_worker_group_poll_status(MagicMock())  # frozen -> 1/2
    with pytest.raises(NCCLHangError) as exc_info:
        callback.after_worker_group_poll_status(MagicMock())  # frozen -> 2/2 -> raise
    # The error reports how many communicators were confirmed stalled.
    assert "1 of 1 communicators" in str(exc_info.value)
    assert len(captured_stack_traces) == 1


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

    callback, captured_stack_trace = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    if expect_hang:
        callback.after_worker_group_poll_status(MagicMock())  # frozen -> 1/2
        assert callback.comm_deadlock_count == {_COMM_A: 1}
        with pytest.raises(NCCLHangError) as exc_info:
            callback.after_worker_group_poll_status(MagicMock())  # 2/2 -> raise
        assert "1 of 1 communicators" in str(exc_info.value)
        assert len(captured_stack_trace) == 1
    else:
        for _ in reports[1:]:
            callback.after_worker_group_poll_status(MagicMock())  # never a hang
        assert callback.comm_deadlock_count == {}
        assert not captured_stack_trace


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
    callback, captured_stack_trace = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    for _ in reports:
        callback.after_worker_group_poll_status(MagicMock())
        assert callback.comm_deadlock_count == {}
    assert not captured_stack_trace


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
    assert "1 of 2 communicators" in str(exc_info.value)


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
    callback, captured_stack_trace = make_nccl_ras_callback(
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
    assert "1 of 2 communicators" in str(exc_info.value)
    assert len(captured_stack_trace) == 1


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
    callback, captured_stack_trace = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    callback.after_worker_group_poll_status(MagicMock())  # A frozen -> 1/2
    assert callback.comm_deadlock_count == {_COMM_A: 1}
    callback.after_worker_group_poll_status(MagicMock())  # A removed -> streak drops
    assert callback.comm_deadlock_count == {}
    callback.after_worker_group_poll_status(MagicMock())  # B keeps advancing
    assert callback.comm_deadlock_count == {}
    assert not captured_stack_trace


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
    poller = RASPoller(MagicMock(), "ncclras", interval_s=interval_s)
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


def test_ras_poller_query_falls_back_to_next_worker():
    workers = [MagicMock(name="w0"), MagicMock(name="w1"), MagicMock(name="w2")]
    worker_group = MagicMock()
    worker_group.get_workers.return_value = workers
    poller = RASPoller(worker_group, "ncclras", interval_s=1.0)

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
    poller = RASPoller(worker_group, "ncclras", interval_s=1.0)

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
    poller = RASPoller(worker_group, "ncclras", interval_s=1.0)
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


def test_hang_error_propagates_when_diagnostics_fail(monkeypatch):
    # Collecting hang diagnostics (stack dump) must not suppress the hang
    # failure, and a real hang must not be misread as a detector bug.
    reports = [create_single_comm_report({2: 5, 3: 4})] * 3
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=reports
    )

    def boom(*_args, **_kwargs):
        raise RuntimeError("stack dump failed")

    callback.dump_workers_stack_traces = boom

    callback.after_worker_group_poll_status(MagicMock())  # baseline
    callback.after_worker_group_poll_status(MagicMock())  # frozen -> 1/2
    with pytest.raises(NCCLHangError):
        callback.after_worker_group_poll_status(MagicMock())  # 2/2 -> still raises
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
    # New-suspicion announcement names the stalled communicator in a parenthetical.
    assert "Possible NCCL hang detected!" in text
    assert f"({_COMM_A}" in text
    # Periodic reminder uses the "still suspected" wording.
    assert "NCCL hang still suspected!" in text
    # Fail mode threatens to raise a NCCLHangError.
    assert "A NCCLHangError will be raised" in text
    # The RAS report is logged verbatim, without a "NCCL RAS report" label.
    assert FakePoller.TEXT_REPORT in text
    assert "NCCL RAS report:" not in text


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
    assert "Possible NCCL hang detected!" in text
    assert "NCCL hang still suspected!" in text
    assert "NCCLHangError will be raised" not in text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
