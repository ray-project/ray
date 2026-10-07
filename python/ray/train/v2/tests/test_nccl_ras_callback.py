"""Unit tests for the NCCL RAS hang-detection callback (no GPU required)."""
import json
import logging
import subprocess
import sys
import time
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from unittest.mock import MagicMock

import pytest

from ray.train.v2._internal.callbacks import nccl_ras
from ray.train.v2._internal.callbacks.nccl_ras import (
    DiagnosticResult,
    NCCLRASCallback,
    RASPoller,
    RASQueryError,
    RASReport,
    dump_flight_recorder,
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
    TORCH_FR_BUFFER_SIZE_ENV_VAR,
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


# ---------------------------------------------------------------------------
# Detection: the callback driven by a scripted poller
# ---------------------------------------------------------------------------


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


# The callback's diagnostic dump methods, keyed by tool, in the order a
# confirmed hang captures them.
_DIAGNOSTIC_METHODS = {
    nccl_ras._FLIGHT_RECORDER_TOOL: "dump_workers_flight_recorder",
    nccl_ras._NVIDIA_SMI_TOOL: "dump_nodes_nvidia_smi",
    nccl_ras._NCCL_RAS_TOOL: "dump_ras_query_history",
    nccl_ras._STACK_TRACES_TOOL: "dump_workers_stack_traces",
}
_ALL_DIAGNOSTICS = list(_DIAGNOSTIC_METHODS)

# What a stubbed diagnostic does: True uploads a directory, False uploads
# nothing (returns None), and an exception is raised from the capture.
_DiagnosticOutcome = Union[bool, Exception]


def make_nccl_ras_callback(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    confirm_count: int,
    reports: List[Union[RASReport, RASQueryError]],
    first_suspicion_polls: int = 1,
    periodic_warn_every_polls: int = 2,
    diagnostics: Optional[Dict[str, _DiagnosticOutcome]] = None,
) -> Tuple[NCCLRASCallback, List[Tuple[str, tuple]]]:
    """Build a callback whose poller yields the given sequence of poll results.

    The detector confirms hangs on consecutive frozen polls but is configured in
    seconds, so a 1s poll interval makes every "seconds" knob equal a poll count.

    No poller thread is started: a :class:`FakePoller` returns one scripted
    result per controller tick, so the tests stay deterministic.

    Args:
        monkeypatch: Sets the callback's env vars and escalation milestones.
        action: ``fail`` or ``observe``.
        confirm_count: Consecutive frozen polls that confirm a hang.
        reports: The poll results the fake poller hands out, one per tick.
        first_suspicion_polls: Frozen polls before the first warning (60s in
            production, shrunk so the small ``confirm_count``s here reach it).
        periodic_warn_every_polls: Frozen polls between reminders (120s in
            production).
        diagnostics: Each tool's stubbed outcome (default: every tool uploads
            a directory). The real dump methods are tested on their own.

    Returns:
        The callback and the ``(tool, args)`` calls its diagnostic stubs
        received, in capture order.
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

    diagnostics = diagnostics or {}
    calls: List[Tuple[str, tuple]] = []

    def stub(tool):
        outcome = diagnostics.get(tool, True)

        def capture(*args):
            calls.append((tool, args))
            if isinstance(outcome, Exception):
                raise outcome
            return f"/exp/{HANG_DETECTOR_DIRNAME}/{tool}" if outcome else None

        return capture

    for tool, method in _DIAGNOSTIC_METHODS.items():
        setattr(callback, method, stub(tool))

    return callback, calls


def captured_tools(calls):
    return [tool for tool, _ in calls]


def tick(callback):
    """One controller poll-loop iteration."""
    callback.after_worker_group_poll_status(MagicMock())


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


def create_two_op_report(allgather):
    """Comm A with a skewed AllReduce and an AllGather at ``allgather`` on both ranks."""
    return create_report(
        comms={
            _COMM_A: {
                0: {"AllReduce": 5, "AllGather": allgather},
                1: {"AllReduce": 4, "AllGather": allgather},
            }
        }
    )


# A frozen, mismatched communicator: confirmed after the baseline + 2 polls.
_FROZEN = create_single_comm_report({1: 5, 2: 4})

# Each case is a confirm count and the polls fed to the callback, each paired
# with the expected ``comm_deadlock_count`` after it or, for the final poll
# of a hang, the text the NCCLHangError must contain. The first poll of every
# case is a baseline: there is nothing to diff it against.
_STREAK_CASES = [
    pytest.param(
        3,
        [
            (_FROZEN, {}),
            (_FROZEN, {_COMM_A: 1}),
            (create_healthy_report(), {}),
        ],
        id="healthy_poll_resets_streak",
    ),
    pytest.param(
        2,
        [
            (create_single_comm_report({1: 5, 2: 3}), {}),
            (create_single_comm_report({1: 9, 2: 5}), {}),
            (create_single_comm_report({1: 15, 2: 8}), {}),
            (create_single_comm_report({1: 20, 2: 14}), {}),
        ],
        id="skewed_but_advancing_never_deadlocks",
    ),
    pytest.param(
        2,
        [
            (create_single_comm_report({2: 5, 3: 3}), {}),
            (create_single_comm_report({2: 8, 3: 6}), {}),
            # Rank 3 froze but rank 2 still moved, so the comm isn't frozen.
            (create_single_comm_report({2: 10, 3: 6}), {}),
            (create_single_comm_report({2: 10, 3: 6}), {_COMM_A: 1}),
            (create_single_comm_report({2: 10, 3: 6}), "1 of 1 communicators"),
        ],
        id="advancing_then_freezing_deadlocks",
    ),
    pytest.param(
        2,
        [
            (create_two_op_report(2), {}),
            (create_two_op_report(2), {_COMM_A: 1}),
            (create_two_op_report(2), "1 of 1 communicators"),
        ],
        id="every_op_frozen_deadlocks",
    ),
    pytest.param(
        2,
        # AllGather keeps advancing, so the ranks are alive and the stale
        # AllReduce skew must not be treated as a hang.
        [
            (create_two_op_report(2), {}),
            (create_two_op_report(3), {}),
            (create_two_op_report(4), {}),
        ],
        id="another_op_advancing_is_not_a_hang",
    ),
    pytest.param(
        2,
        # A deadlocks while B stays skewed but advancing: only A is confirmed.
        [
            (create_report({_COMM_A: {2: 5, 3: 6}, _COMM_B: {0: 5, 1: 4}}), {}),
            (
                create_report({_COMM_A: {2: 5, 3: 6}, _COMM_B: {0: 7, 1: 6}}),
                {_COMM_A: 1},
            ),
            (
                create_report({_COMM_A: {2: 5, 3: 6}, _COMM_B: {0: 9, 1: 8}}),
                "1 of 2 communicators",
            ),
        ],
        id="communicators_streak_independently",
    ),
    pytest.param(
        2,
        # B appears already skewed; it cannot be flagged on the poll it first
        # appears in (no baseline to diff against).
        [
            (create_report({_COMM_A: {0: 2, 1: 2}}), {}),
            (create_report({_COMM_A: {0: 3, 1: 3}}), {}),
            (create_report({_COMM_A: {0: 4, 1: 4}, _COMM_B: {0: 7, 1: 5}}), {}),
            (
                create_report({_COMM_A: {0: 5, 1: 5}, _COMM_B: {0: 7, 1: 5}}),
                {_COMM_B: 1},
            ),
            (
                create_report({_COMM_A: {0: 6, 1: 6}, _COMM_B: {0: 7, 1: 5}}),
                "1 of 2 communicators",
            ),
        ],
        id="added_communicator_needs_a_baseline",
    ),
    pytest.param(
        2,
        # A's process group is destroyed mid-streak: its streak is dropped and
        # B keeps being evaluated on its own.
        [
            (create_report({_COMM_A: {2: 5, 3: 4}, _COMM_B: {0: 2, 1: 2}}), {}),
            (
                create_report({_COMM_A: {2: 5, 3: 4}, _COMM_B: {0: 3, 1: 3}}),
                {_COMM_A: 1},
            ),
            (create_report({_COMM_B: {0: 6, 1: 4}}), {}),
            (create_report({_COMM_B: {0: 9, 1: 6}}), {}),
        ],
        id="removed_communicator_drops_its_streak",
    ),
]


@pytest.mark.parametrize("confirm_count,polls", _STREAK_CASES)
def test_frozen_streaks(monkeypatch, confirm_count, polls):
    # A communicator is deadlocked only once it is mismatched and *no* rank
    # advanced *any* op for `confirm_count` consecutive polls.
    callback, calls = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_count=confirm_count,
        reports=[report for report, _ in polls],
    )

    for i, (_, expected) in enumerate(polls):
        if isinstance(expected, str):
            with pytest.raises(NCCLHangError, match=expected):
                tick(callback)
        else:
            tick(callback)
            assert callback.comm_deadlock_count == expected, f"after poll {i}"

    hung = isinstance(polls[-1][1], str)
    assert captured_tools(calls) == (_ALL_DIAGNOSTICS if hung else [])


def test_no_new_report_is_noop(monkeypatch):
    # The controller polls (~2s) far more often than the poller publishes (15s).
    # A hook that finds nothing queued must leave detection state, and the RAS
    # history, untouched.
    callback, _ = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        confirm_count=1,
        reports=[create_single_comm_report({2: 5, 3: 3})],
    )

    tick(callback)  # consumes the report
    first_report = callback.prev_report
    assert first_report is not None
    tick(callback)  # poller published nothing
    assert callback.prev_report is first_report
    assert list(callback.ras_history) == [first_report]


# ---------------------------------------------------------------------------
# Confirmed hang: which diagnostics are captured and what the error says
# ---------------------------------------------------------------------------


def confirm_hang(callback) -> NCCLHangError:
    """Drive a callback fed ``[_FROZEN] * 3`` (confirm_count=2) to its hang."""
    tick(callback)  # baseline
    tick(callback)  # frozen -> 1/2
    with pytest.raises(NCCLHangError) as exc_info:
        tick(callback)  # frozen -> 2/2
    return exc_info.value


def test_confirmed_hang_capture_order(monkeypatch):
    callback, calls = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, reports=[_FROZEN] * 3
    )

    confirm_hang(callback)

    # Flight Recorder first, so a rank still running cannot overwrite its ring
    # buffer; nvidia-smi before the stack traces, which take far longer, so the
    # GPU reading is as close to the moment of the hang as we can make it.
    assert captured_tools(calls) == _ALL_DIAGNOSTICS
    # The text report is fetched once and saved alongside the history.
    assert callback._ras_poller.text_queries == 1
    assert dict(calls)[nccl_ras._NCCL_RAS_TOOL] == (FakePoller.TEXT_REPORT,)


_MESSAGE_CASES = [
    pytest.param({}, id="all_produced"),
    pytest.param(
        {tool: RuntimeError("boom") for tool in _ALL_DIAGNOSTICS}, id="all_raise"
    ),
] + [
    pytest.param({tool: outcome}, id=f"{tool}_{outcome_id}")
    for tool in _ALL_DIAGNOSTICS
    for outcome_id, outcome in (("none", False), ("raises", RuntimeError("boom")))
]


@pytest.mark.parametrize("diagnostics", _MESSAGE_CASES)
def test_confirmed_hang_message(monkeypatch, diagnostics):
    # The error points the user at exactly the diagnostics that produced a
    # directory. A diagnostic that uploads nothing or fails must neither stop
    # the others nor suppress the hang, and a real hang must not be misread as
    # a detector bug.
    callback, calls = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_count=2,
        reports=[_FROZEN] * 3,
        diagnostics=diagnostics,
    )

    message = str(confirm_hang(callback))

    assert captured_tools(calls) == _ALL_DIAGNOSTICS
    for tool in _ALL_DIAGNOSTICS:
        produced = diagnostics.get(tool, True) is True
        assert (f"/exp/{HANG_DETECTOR_DIRNAME}/{tool}" in message) is produced, tool
    assert callback._is_ras_degraded is False


@pytest.mark.parametrize("action", [NCCL_RAS_ACTION_FAIL, NCCL_RAS_ACTION_OBSERVE])
def test_confirmed_hang_action(monkeypatch, caplog, propagate_logs, action):
    # Fail mode raises; observe mode logs the same message and keeps training,
    # but still captures every diagnostic: observing is worthless without them.
    callback, calls = make_nccl_ras_callback(
        monkeypatch, action, confirm_count=2, reports=[_FROZEN] * 3
    )

    if action == NCCL_RAS_ACTION_FAIL:
        message = str(confirm_hang(callback))
    else:
        with caplog.at_level(logging.WARNING, logger=nccl_ras.logger.name):
            for _ in range(3):
                tick(callback)  # must not raise
        message = caplog.text

    assert "1 of 1 communicators" in message
    assert captured_tools(calls) == _ALL_DIAGNOSTICS


def test_capture_diagnostic_swallows_failures(caplog, propagate_logs):
    # A diagnostic that fails must never take the run's hang handling with it.
    def boom():
        raise RuntimeError("upload failed")

    with caplog.at_level(logging.ERROR, logger=nccl_ras.logger.name):
        assert NCCLRASCallback.capture_diagnostic("worker stack traces", boom) is None
    assert "worker stack traces" in caplog.text


# ---------------------------------------------------------------------------
# Poller thread and transport
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Lifecycle, degradation and config
# ---------------------------------------------------------------------------


def test_callback_starts_and_stops_poller(monkeypatch):
    # The worker group start owns one poller; teardown stops it and a restart
    # gets a fresh one (never the stopped instance) and fresh detection state:
    # a new worker group is a new set of communicators.
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

    callback.prev_report = _FROZEN
    callback.ras_history.append(_FROZEN)
    callback.comm_deadlock_count = {_COMM_A: 1}

    callback.after_worker_group_start(MagicMock())
    second = callback._ras_poller
    assert second is not first and second.is_alive
    assert callback.prev_report is None
    assert callback.comm_deadlock_count == {}
    assert len(callback.ras_history) == 0
    callback.before_worker_group_shutdown(MagicMock())
    assert _join(second)


def test_callback_degrades_on_fatal_poll_result(monkeypatch):
    fatal = RASQueryError("unsupported_f_option", "binary rejected `-f`", fatal=True)
    callback, calls = make_nccl_ras_callback(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_count=1,
        reports=[fatal, create_single_comm_report({2: 5, 3: 3})],
    )

    tick(callback)  # fatal -> degrade
    assert callback._is_ras_degraded is True
    tick(callback)  # no-op once degraded: the queued report is never consumed
    assert callback.prev_report is None
    assert callback.comm_deadlock_count == {}
    assert not calls

    # A degraded callback does not start a poller for a later worker group.
    callback.after_worker_group_start(MagicMock())
    assert callback._ras_poller is None


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
    tick(callback)  # must not raise
    assert callback._is_ras_degraded is True
    # Once degraded, later polls are a no-op (guarded before next_result).
    tick(callback)
    assert callback.prev_report is None


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
    reports = [_FROZEN] * 4
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=100, reports=reports
    )

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for _ in reports:
            tick(callback)

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
    reports = [_FROZEN] * 4
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=100, reports=reports
    )

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for _ in reports:
            tick(callback)

    text = caplog.text
    assert "Possible NCCL hang detected!" in text
    assert "NCCL hang still suspected!" in text
    assert "NCCLHangError will be raised" not in text


# ---------------------------------------------------------------------------
# Diagnostics, stage 1: the worker-side functions
# ---------------------------------------------------------------------------


def fake_subprocess_run(monkeypatch, **run_result):
    """Script ``subprocess.run`` for the worker-side tools, recording each call.

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
    the worker-side dump whether or not torch is installed.
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


def test_dump_stack_trace_returns_the_py_spy_dump(monkeypatch):
    # The native dump is what shows a rank blocked inside NCCL, so it is returned
    # verbatim rather than summarised.
    calls = fake_subprocess_run(monkeypatch, stdout="native stack")

    assert dump_stack_trace(25.0) == "native stack"
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
    # py-spy is an optional dependency; without it the native frames are lost but
    # the Python stacks of every thread are still worth having.
    fake_subprocess_run(monkeypatch, **run_result)

    trace = dump_stack_trace(25.0)

    assert f"py-spy unavailable: {expected_reason}" in trace
    assert "test_dump_stack_trace_falls_back_to_python" in trace


def test_nvidia_smi_returns_the_report(monkeypatch):
    calls = fake_subprocess_run(monkeypatch, stdout="GPU 00000000:00:04.0\n")

    assert run_nvidia_smi(25.0) == {"ok": True, "stdout": "GPU 00000000:00:04.0\n"}
    ((cmd, kwargs),) = calls
    assert cmd == ["nvidia-smi", "-q"]
    # The driver is what might be wedged, so the call is always bounded.
    assert kwargs["timeout"] == 25.0


@pytest.mark.parametrize(
    "run_result,expected_reason",
    [
        ({"side_effect": FileNotFoundError()}, "`nvidia-smi` is missing on this node"),
        (
            {"side_effect": subprocess.TimeoutExpired("nvidia-smi", 25.0)},
            "timed out after 25s",
        ),
        ({"side_effect": OSError("no permission")}, "no permission"),
        ({"returncode": 9, "stderr": "driver/library mismatch"}, "driver/library"),
    ],
    ids=["not_installed", "driver_stuck", "os_error", "non_zero_exit"],
)
def test_nvidia_smi_failures_say_why(monkeypatch, run_result, expected_reason):
    # The reason is written into the node's file, so it has to be readable.
    fake_subprocess_run(monkeypatch, **run_result)

    result = run_nvidia_smi(25.0)

    assert result["ok"] is False and expected_reason in result["reason"]


def test_flight_recorder_dump_decodes_bytes(fake_c10d):
    # torch returns the trace as utf-8 bytes, but the dumps are uploaded as text
    # files, so the bytes have to be decoded before they get there.
    fake_c10d._dump_fr_trace_json = lambda *args, **kwargs: b'{"entries": []}'

    assert dump_flight_recorder() == {"ok": True, "trace_json": '{"entries": []}'}


def _raise_dump_failed(*args, **kwargs):
    raise RuntimeError("dump failed")


@pytest.mark.parametrize(
    "torch_c,dump_fn,expected_reason",
    [
        (None, None, "c10d"),  # a None module makes the import fail
        ("fake", _raise_dump_failed, "dump failed"),
    ],
    ids=["no_torch", "dump_raises"],
)
def test_flight_recorder_dump_failures(
    monkeypatch, fake_c10d, torch_c, dump_fn, expected_reason
):
    if torch_c is None:
        monkeypatch.setitem(sys.modules, "torch._C", None)
    else:
        fake_c10d._dump_fr_trace_json = dump_fn

    result = dump_flight_recorder()

    assert result["ok"] is False and expected_reason in result["reason"]


# ---------------------------------------------------------------------------
# Diagnostics, stage 2: fanning a worker-side function out to the group
# ---------------------------------------------------------------------------


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

    def worker(rank, value=None, launch_error=None, get_error=None, ready=True):
        worker = make_worker(rank)
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
    "broken,expected_error,expected_log",
    [
        (
            {"launch_error": RuntimeError("actor is dead")},
            "failed to launch: actor is dead",
            "Failed to launch dump_stack_trace on rank 0",
        ),
        (
            {"ready": False},
            "timed out after 30s",
            "dump_stack_trace on rank 0 did not finish within 30s",
        ),
        (
            {"get_error": RuntimeError("worker exited")},
            "failed to collect: worker exited",
            "Failed to collect dump_stack_trace on rank 0",
        ),
    ],
    ids=["launch_failed", "timed_out", "collect_failed"],
)
def test_fan_out_records_per_rank_failures(
    fan_out, caplog, propagate_logs, broken, expected_error, expected_log
):
    # One unreachable rank (the hung one is the interesting one) must not cost
    # us the ranks that did answer: it gets an error saying why, which is what
    # ends up in that rank's file, and the failure is logged.
    workers = [fan_out.worker(0, **broken), fan_out.worker(1, value="stack-1")]

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        dumps = fan_out_to_workers(workers, dump_stack_trace, 25.0, timeout_s=30.0)

    assert dumps[0].value is None and dumps[0].error == expected_error
    assert dumps[1].value == "stack-1" and dumps[1].error is None
    assert expected_log in caplog.text


# ---------------------------------------------------------------------------
# Diagnostics, stage 3: turning each tool's dumps into uploaded files
# ---------------------------------------------------------------------------


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
    """A callback wired to a worker group, to exercise the real dump methods.

    By default two workers, each on its own node.
    """
    if workers is None:
        workers = [make_worker(rank, node_ip=f"10.0.0.{rank}") for rank in (0, 1)]
    callback = NCCLRASCallback()
    worker_group = MagicMock()
    worker_group.get_workers.return_value = workers
    worker_group._storage_context.experiment_fs_path = experiment_fs_path
    callback._worker_group = worker_group
    return callback


def scripted_fan_out(monkeypatch, dumps):
    """Replace the worker fan-out with fixed dumps, recording how it was called."""
    calls = []

    def fake_fan_out(workers, fn, *fn_args, timeout_s):
        calls.append((workers, fn, fn_args, timeout_s))
        return dumps

    monkeypatch.setattr(nccl_ras, "fan_out_to_workers", fake_fan_out)
    return calls


@dataclass
class FanOutDiagnostic:
    """How one fan-out diagnostic talks to its workers and names its files.

    Attributes:
        tool: The ``hang_detector/`` sub-directory it uploads to.
        method: The callback method that captures it.
        fn: The worker-side function fanned out.
        fn_args: The arguments ``fn`` is fanned out with.
        timeout_s: The fan-out's budget.
        ok: The worker's return value for a successful dump of ``contents``.
        failed: The worker's return value when it reports a failure with
            ``reason``, or ``None`` if the worker-side function never does.
        filename: The file a rank's dump lands in (nvidia-smi: its node's).
        placeholder: What is written in place of a dump that failed with
            ``reason``.
    """

    tool: str
    method: str
    fn: Callable[..., Any]
    fn_args: tuple
    timeout_s: float
    ok: Callable[[str], Any]
    failed: Optional[Callable[[str], Any]]
    filename: Callable[[int], str]
    placeholder: Callable[[str], str]


_FAN_OUT_DIAGNOSTICS = [
    FanOutDiagnostic(
        tool=nccl_ras._STACK_TRACES_TOOL,
        method="dump_workers_stack_traces",
        fn=nccl_ras.dump_stack_trace,
        # py-spy has to give up before the controller stops waiting.
        fn_args=(nccl_ras._STACK_DUMP_TIMEOUT_S - 1,),
        timeout_s=nccl_ras._STACK_DUMP_TIMEOUT_S,
        ok=lambda contents: contents,
        failed=None,  # dump_stack_trace always returns a (fallback) trace
        filename=lambda rank: f"rank_{rank}.log",
        placeholder=lambda reason: reason,
    ),
    FanOutDiagnostic(
        tool=nccl_ras._NVIDIA_SMI_TOOL,
        method="dump_nodes_nvidia_smi",
        fn=nccl_ras.run_nvidia_smi,
        fn_args=(nccl_ras._NVIDIA_SMI_TIMEOUT_S - 1,),
        timeout_s=nccl_ras._NVIDIA_SMI_TIMEOUT_S,
        ok=lambda contents: {"ok": True, "stdout": contents},
        failed=lambda reason: {"ok": False, "reason": reason},
        filename=lambda rank: f"node_10.0.0.{rank}.log",
        placeholder=lambda reason: f"no `nvidia-smi` snapshot: {reason}\n",
    ),
    FanOutDiagnostic(
        tool=nccl_ras._FLIGHT_RECORDER_TOOL,
        method="dump_workers_flight_recorder",
        fn=nccl_ras.dump_flight_recorder,
        fn_args=(),
        timeout_s=nccl_ras._FLIGHT_RECORDER_DUMP_TIMEOUT_S,
        ok=lambda contents: {"ok": True, "trace_json": contents},
        failed=lambda reason: {"ok": False, "reason": reason},
        filename=lambda rank: f"rank_{rank}.json",
        # Still valid JSON, so every file in the directory parses.
        placeholder=lambda reason: json.dumps({"ray_train_dump_error": reason}),
    ),
]


@pytest.mark.parametrize(
    "diagnostic", _FAN_OUT_DIAGNOSTICS, ids=lambda diagnostic: diagnostic.tool
)
def test_diagnostic_uploads_one_file_per_target(monkeypatch, uploads, diagnostic):
    # Every rank (nvidia-smi: every node) gets its own file, named so it can be
    # matched to the RAS report, holding exactly what the worker returned.
    monkeypatch.setenv(TORCH_FR_BUFFER_SIZE_ENV_VAR, "2000")
    callback = make_diagnostics_callback()
    workers = callback._worker_group.get_workers()
    calls = scripted_fan_out(
        monkeypatch,
        {
            rank: DiagnosticResult(value=diagnostic.ok(f"dump {rank}"))
            for rank in (0, 1)
        },
    )

    fs_path = getattr(callback, diagnostic.method)()

    assert fs_path == f"/exp/{HANG_DETECTOR_DIRNAME}/{diagnostic.tool}"
    assert uploads == [
        (fs_path, {diagnostic.filename(rank): f"dump {rank}" for rank in (0, 1)})
    ]
    assert calls == [(workers, diagnostic.fn, diagnostic.fn_args, diagnostic.timeout_s)]


_PLACEHOLDER_CASES = [
    pytest.param(diagnostic, source, id=f"{diagnostic.tool}-{source}")
    for diagnostic in _FAN_OUT_DIAGNOSTICS
    for source in ("fan_out_error", "worker_reported_failure")
    if source == "fan_out_error" or diagnostic.failed is not None
]


@pytest.mark.parametrize("diagnostic,source", _PLACEHOLDER_CASES)
def test_diagnostic_failed_target_gets_placeholder(
    monkeypatch, uploads, diagnostic, source
):
    # A rank or node with no dump -- the fan-out couldn't reach it, or the tool
    # itself failed there (e.g. `nvidia-smi` missing on that node) -- still gets
    # a file saying why, so a gap is never silent, and the targets that did
    # answer are still uploaded.
    monkeypatch.setenv(TORCH_FR_BUFFER_SIZE_ENV_VAR, "2000")
    callback = make_diagnostics_callback()
    reason = "it went wrong"
    if source == "fan_out_error":
        bad_dump = DiagnosticResult(error=reason)
    else:
        bad_dump = DiagnosticResult(value=diagnostic.failed(reason))

    scripted_fan_out(
        monkeypatch, {0: bad_dump, 1: DiagnosticResult(value=diagnostic.ok("dump 1"))}
    )
    getattr(callback, diagnostic.method)()

    ((_, files),) = uploads
    assert files == {
        diagnostic.filename(0): diagnostic.placeholder(reason),
        diagnostic.filename(1): "dump 1",
    }


def test_nvidia_smi_queries_one_worker_per_node(monkeypatch, uploads):
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
            0: DiagnosticResult(value={"ok": True, "stdout": "node 1 GPUs"}),
            2: DiagnosticResult(value={"ok": True, "stdout": "node 2 GPUs"}),
        },
    )

    callback.dump_nodes_nvidia_smi()

    ((queried, *_),) = calls
    assert queried == [workers[0], workers[2]]
    ((_, files),) = uploads
    assert files == {
        "node_10.0.0.1.log": "node 1 GPUs",
        "node_10.0.0.2.log": "node 2 GPUs",
    }


@pytest.mark.parametrize(
    "method,has_workers,fr_armed",
    [
        ("dump_workers_stack_traces", False, True),
        ("dump_nodes_nvidia_smi", False, True),
        ("dump_workers_flight_recorder", False, True),
        # The ring buffer has to be armed before the process group is created,
        # so unarmed there is nothing to dump and the fan-out isn't worth the
        # hung job's time.
        ("dump_workers_flight_recorder", True, False),
        # Nothing polled yet: the history has nothing to write.
        ("dump_ras_query_history", True, True),
    ],
    ids=[
        "stack_traces_no_workers",
        "nvidia_smi_no_workers",
        "flight_recorder_no_workers",
        "flight_recorder_not_armed",
        "ras_history_empty",
    ],
)
def test_diagnostic_skipped(monkeypatch, uploads, method, has_workers, fr_armed):
    # With nothing to record there is no empty directory to leave behind in the
    # user's experiment directory, and the error does not point at one.
    if fr_armed:
        monkeypatch.setenv(TORCH_FR_BUFFER_SIZE_ENV_VAR, "2000")
    else:
        monkeypatch.delenv(TORCH_FR_BUFFER_SIZE_ENV_VAR, raising=False)
    callback = make_diagnostics_callback(None if has_workers else [])
    calls = scripted_fan_out(monkeypatch, [])

    assert getattr(callback, method)() is None
    assert calls == [] and uploads == []


# ---------------------------------------------------------------------------
# Diagnostics: the RAS query history
# ---------------------------------------------------------------------------


def test_ras_history_buffer(monkeypatch):
    # Every successful poll is recorded, healthy or not, and the buffer reaches
    # back past the confirmation window (otherwise the saved history only ever
    # shows the communicator already stalled), evicting the oldest beyond that.
    confirm_count = 3
    maxlen = confirm_count + nccl_ras._RAS_HISTORY_MARGIN_POLLS
    reports = [create_single_comm_report({1: count}) for count in range(maxlen + 5)]
    callback, _ = make_nccl_ras_callback(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=confirm_count, reports=reports
    )

    for _ in reports:
        tick(callback)

    assert callback.ras_history.maxlen == maxlen
    assert list(callback.ras_history) == reports[-maxlen:]


@pytest.mark.parametrize(
    "text_report", ["human readable report", None], ids=["with_text", "without_text"]
)
def test_ras_history_upload(uploads, text_report):
    # Each retained poll is written verbatim -- the raw `ncclras` output, so
    # hosts, pids and missing ranks survive -- under a filename made from its
    # RAS timestamp. The text report is a separate query that can fail while
    # the history is still worth writing.
    callback = make_diagnostics_callback()
    callback.ras_history.extend(
        parse_ras_schema(ras_json)
        for ras_json in (HEALTHY_RAS_JSON, DEAD_RANK_RAS_JSON, MULTI_COMM_RAS_JSON)
    )

    fs_path = callback.dump_ras_query_history(text_report)

    assert fs_path == f"/exp/{HANG_DETECTOR_DIRNAME}/nccl_ras"
    expected = {
        "ncclras_2026-06-19-06-51-56.json": HEALTHY_RAS_JSON,
        "ncclras_2026-06-19-06-55-57.json": DEAD_RANK_RAS_JSON,
        "ncclras_2026-06-19-06-56-24.json": MULTI_COMM_RAS_JSON,
    }
    if text_report:
        expected["ncclras_report.txt"] = text_report
    assert uploads == [(fs_path, expected)]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
