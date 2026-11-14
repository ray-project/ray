"""Tests of the NCCL hang detector on ``ray.train.health``: the evaluators in
``ray.train.health.evaluators.nccl_ras``, and ``NCCLHangHealthCallback``,
which runs them for a run.

Most are copies of ``test_nccl_ras_callback.py``'s, which check that
``NCCLRASCallback`` detects, reports and acts on the same hangs.
"""
import asyncio
import json
import logging
import re
import sys
from typing import Dict, List, Optional, Tuple, Union
from unittest.mock import MagicMock

import pytest

from ray.train.health import (
    Diagnose,
    HealthState,
    Noop,
    ProbeResult,
    Reattempt,
)
from ray.train.health.evaluators import nccl_ras
from ray.train.health.evaluators.nccl_ras import (
    HangDiagnostics,
    NCCLHangDetectorState,
    NCCLHangReattempt,
    RASReport,
    WorkerInfo,
    parse_ras_schema,
)
from ray.train.health.probes.nccl_ras import RASQueryError
from ray.train.v2._internal.callbacks import nccl_hang_health
from ray.train.v2._internal.callbacks.nccl_hang_health import NCCLHangHealthCallback
from ray.train.v2._internal.constants import (
    ENABLE_NCCL_HANG_DETECTOR_ENV_VAR,
    HANG_DETECTOR_DIRNAME,
    NCCL_RAS_ACTION_ENV_VAR,
    NCCL_RAS_ACTION_FAIL,
    NCCL_RAS_ACTION_OBSERVE,
    NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR,
    NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR,
    TORCH_FR_BUFFER_SIZE_ENV_VAR,
)
from ray.train.v2._internal.metrics import base as metrics_base
from ray.train.v2.tests.util import create_dummy_run_context

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
    "ras_json", ["not json", "{}", '{"communicators": [{"hash": "0x1"}]}']
)
def test_parse_unexpected_output_is_none(ras_json):
    assert parse_ras_schema(ras_json) is None


# ---------------------------------------------------------------------------
# Detection: the check driven like the HealthManager drives it
# ---------------------------------------------------------------------------

TEXT_REPORT = "NCCL RAS text report"
_NODE = "node-a"


def make_train_worker(
    world_rank: int, node_ip: str, pid: int, node_id: str = _NODE
) -> MagicMock:
    worker = MagicMock()
    worker.distributed_context.world_rank = world_rank
    worker.metadata.node_id = node_id
    worker.metadata.node_ip = node_ip
    worker.metadata.pid = pid
    return worker


_DEFAULT_WORKERS = [
    make_train_worker(0, "10.0.0.1", 100),
    make_train_worker(1, "10.0.0.1", 101),
]

# The diagnostics a confirmed hang captures, in the order they are uploaded.
_ALL_DIAGNOSTICS = [
    nccl_ras.FLIGHT_RECORDER_TOOL,
    nccl_ras.NVIDIA_SMI_TOOL,
    nccl_ras.NCCL_RAS_TOOL,
    nccl_ras.STACK_TRACES_TOOL,
]
_PROBE_OF_TOOL = {
    nccl_ras.FLIGHT_RECORDER_TOOL: "FlightRecorderProbe",
    nccl_ras.NVIDIA_SMI_TOOL: "NvidiaSmiProbe",
    nccl_ras.NCCL_RAS_TOOL: "NCCLRASTextProbe",
    nccl_ras.STACK_TRACES_TOOL: "StackTraceProbe",
}


def to_ras_json(report: RASReport) -> str:
    """Write a report as ``ncclras -f json`` would, with every rank RUNNING."""
    running = {
        "init_state": 0,
        "async_error": 0,
        "finalize_called": False,
        "destroy_flag": False,
        "abort_flag": False,
    }
    communicators = []
    for comm_id, ranks in report.comm_op_counts.items():
        processes = report.comm_rank_processes.get(comm_id, {})
        communicators.append(
            {
                "hash": comm_id,
                "ranks": [
                    {
                        "rank": rank,
                        "host": processes.get(rank, ("10.255.255.255", 0))[0],
                        "pid": processes.get(rank, ("10.255.255.255", 0))[1],
                        "status": running,
                        "collective_counts": counts,
                    }
                    for rank, counts in ranks.items()
                ],
            }
        )
    return json.dumps({"timestamp": report.timestamp, "communicators": communicators})


class Detector:
    """The NCCL hang check of a ``NCCLHangHealthCallback``, driven like the
    ``HealthManager`` drives it: each tick gives the evaluator one new
    ``NCCLRASProbe`` report, and a ``Diagnose`` it returns is run at once.

    Args:
        callback: The callback whose check to drive.

    Attributes:
        callback: The callback that gives the check the run.
        evaluator: The check's ``NCCLHangEvaluator``.
        diagnoses: The probe names of each ``Diagnose`` that ran.
        text_results: The ``NCCLRASTextProbe`` results the ``Diagnose``\\ s get,
            in turn. Each gets ``TEXT_REPORT`` once these run out.
        results: Results that replace the default ones, by probe name.
    """

    def __init__(self, callback: NCCLHangHealthCallback):
        self.callback = callback
        (self.evaluator,) = callback.health_check().evaluator_creator()
        self.diagnoses: List[List[str]] = []
        self.text_results: List[ProbeResult] = []
        self.results: Dict[str, Dict] = {}
        self._time_s = 0.0
        self._state: Optional[HealthState] = None

    def tick(self, poll: Union[RASReport, Exception]):
        """One poll that ``NCCLRASProbe`` answered with ``poll``.

        Args:
            poll: The report, or the query's error.

        Returns:
            The decision, after running a ``Diagnose``.
        """
        self._time_s += 1
        if isinstance(poll, Exception):
            result = ProbeResult(error=poll, timestamp_s=self._time_s)
        else:
            result = ProbeResult(detail=to_ras_json(poll), timestamp_s=self._time_s)
        self._state = HealthState(probe_results={"NCCLRASProbe": {_NODE: result}})
        return self.run(self.evaluator.evaluate(self._state))

    def tick_without_a_new_report(self):
        """A controller tick between two polls: the same report again."""
        return self.run(self.evaluator.evaluate(self._state))

    def run(self, decision):
        if not isinstance(decision, Diagnose):
            return decision
        (check,) = decision.checks
        probes = check.probe_creator()
        self.diagnoses.append([probe.probe_name() for probe in probes])
        state = HealthState(
            probe_results={
                probe.probe_name(): self.result_of(probe.probe_name())
                for probe in probes
            }
        )
        (evaluator,) = check.evaluator_creator()
        return evaluator.evaluate(state)

    def result_of(self, probe_name: str) -> Dict:
        if probe_name in self.results:
            return self.results[probe_name]
        workers = self.callback.workers()
        if probe_name == "NCCLRASTextProbe":
            result = (
                self.text_results.pop(0)
                if self.text_results
                else ProbeResult(detail=TEXT_REPORT)
            )
            return {_NODE: result}
        if probe_name == "NvidiaSmiProbe":
            return {
                node_id: ProbeResult(detail=f"GPUs of {node_id}")
                for node_id in sorted({w.node_id for w in workers})
            }
        return {
            w.world_rank: ProbeResult(detail=f"{probe_name} of rank {w.world_rank}")
            for w in workers
        }


def make_detector(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    confirm_count: int,
    first_suspicion_polls: int = 1,
    periodic_warn_every_polls: int = 2,
    workers: Optional[List[MagicMock]] = None,
) -> Detector:
    """Build the check of a callback configured by its env vars.

    The detector confirms hangs on consecutive frozen polls but is configured in
    seconds, so a 1s poll interval makes every "seconds" knob equal a poll count.

    Args:
        monkeypatch: Sets the env vars and escalation milestones.
        action: ``fail`` or ``observe``.
        confirm_count: Consecutive frozen polls that confirm a hang.
        first_suspicion_polls: Frozen polls before the first warning (60s in
            production, shrunk so the small ``confirm_count``\\ s here reach it).
        periodic_warn_every_polls: Frozen polls between reminders (120s in
            production).
        workers: The worker group's workers.

    Returns:
        The detector, with the callback's worker group started.
    """
    monkeypatch.setenv(NCCL_RAS_ACTION_ENV_VAR, action)
    monkeypatch.setenv(NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR, str(confirm_count))
    monkeypatch.setenv(NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, "1")
    monkeypatch.setenv(TORCH_FR_BUFFER_SIZE_ENV_VAR, "2000")
    monkeypatch.setattr(
        nccl_ras, "_FIRST_SUSPICION_AFTER_S", float(first_suspicion_polls)
    )
    monkeypatch.setattr(
        nccl_ras, "_PERIODIC_WARN_EVERY_S", float(periodic_warn_every_polls)
    )

    callback = NCCLHangHealthCallback()
    worker_group = MagicMock()
    worker_group.get_workers.return_value = (
        _DEFAULT_WORKERS if workers is None else workers
    )
    worker_group._storage_context.experiment_fs_path = "/exp"
    callback.after_worker_group_start(worker_group)
    detector = Detector(callback)
    assert detector.evaluator._confirm_poll_counts == confirm_count
    return detector


@pytest.fixture(autouse=True)
def uploads(monkeypatch):
    """Capture each upload as ``(fs_path, {filename: contents})``."""
    calls = []

    def fake_upload(local_dir, filesystem, fs_path):
        from pathlib import Path

        calls.append(
            (fs_path, {p.name: p.read_text() for p in Path(local_dir).iterdir()})
        )

    monkeypatch.setattr(nccl_hang_health, "_upload_to_fs_path", fake_upload)
    return calls


def uploaded_tools(uploads):
    return [fs_path.rsplit("/", 1)[1] for fs_path, _ in uploads]


_COMM_A = "0x2b9ffd12ea17b069"
_COMM_B = "0xced5b798f46495a3"

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

# Each case is the expected number of confirm counts and simulated NCCL RAS polls.
# The first poll of every case is a baseline: there is nothing to diff it against.
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
            (create_single_comm_report({2: 10, 3: 6}), "1 communicator(s)"),
        ],
        id="advancing_then_freezing_deadlocks",
    ),
    pytest.param(
        2,
        [
            (create_two_op_report(2), {}),
            (create_two_op_report(2), {_COMM_A: 1}),
            (create_two_op_report(2), "1 communicator(s)"),
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
                "1 communicator(s)",
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
                "1 communicator(s)",
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
def test_frozen_streaks(monkeypatch, uploads, confirm_count, polls):
    # A communicator is deadlocked only once it is mismatched and *no* rank
    # advanced *any* op for `confirm_count` consecutive polls.
    detector = make_detector(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=confirm_count
    )

    for i, (report, expected) in enumerate(polls):
        decision = detector.tick(report)
        if isinstance(expected, str):
            assert isinstance(decision, Reattempt), f"after poll {i}"
            assert re.search(re.escape(expected), decision.reason)
        else:
            assert not isinstance(decision, Reattempt), f"after poll {i}"
            assert detector.evaluator.comm_deadlock_count == expected, f"after poll {i}"

    hung = isinstance(polls[-1][1], str)
    assert uploaded_tools(uploads) == (_ALL_DIAGNOSTICS if hung else [])


def test_no_new_report_is_noop(monkeypatch):
    # The controller evaluates (~2s) far more often than NCCLRASProbe is polled
    # (15s). An evaluation with no new report must leave detection state, and
    # the RAS history, untouched.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=1)

    detector.tick(create_single_comm_report({2: 5, 3: 3}))
    first_report = detector.evaluator.prev_report
    assert first_report is not None
    assert isinstance(detector.tick_without_a_new_report(), Noop)
    assert detector.evaluator.prev_report is first_report
    assert list(detector.evaluator.ras_history) == [first_report]


# ---------------------------------------------------------------------------
# Confirmed hang: which diagnostics are captured and what the decision says
# ---------------------------------------------------------------------------


def confirm_hang(detector) -> Union[Reattempt, Noop]:
    """Drive a detector with confirm_count=2 to its hang."""
    detector.tick(_FROZEN)  # baseline
    detector.tick(_FROZEN)  # frozen -> 1/2
    return detector.tick(_FROZEN)  # frozen -> 2/2


def test_confirmed_hang_captures_every_diagnostic(monkeypatch, uploads):
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2)

    confirm_hang(detector)

    # The text report is fetched once when the hang is first suspected (and
    # logged), then once more at confirmation with the other diagnostics.
    assert detector.diagnoses == [
        ["NCCLRASTextProbe"],
        [
            "FlightRecorderProbe",
            "NvidiaSmiProbe",
            "NCCLRASTextProbe",
            "StackTraceProbe",
        ],
    ]
    assert uploaded_tools(uploads) == _ALL_DIAGNOSTICS
    (ras_files,) = [files for path, files in uploads if path.endswith("/nccl_ras")]
    assert ras_files["ncclras_report.txt"] == TEXT_REPORT


def test_flight_recorder_needs_its_buffer(monkeypatch, uploads):
    # The ring buffer has to be armed, so an unarmed one dumps nothing.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2)
    monkeypatch.delenv(TORCH_FR_BUFFER_SIZE_ENV_VAR)

    decision = confirm_hang(detector)

    assert "FlightRecorderProbe" not in detector.diagnoses[-1]
    assert uploaded_tools(uploads) == _ALL_DIAGNOSTICS[1:]
    assert "flight_recorder" not in decision.reason


_MESSAGE_CASES = [
    pytest.param({}, id="all_produced"),
    pytest.param({tool: "raises" for tool in _ALL_DIAGNOSTICS}, id="all_raise"),
] + [
    pytest.param({tool: outcome}, id=f"{tool}_{outcome}")
    for tool in _ALL_DIAGNOSTICS
    for outcome in ("raises", "absent")
    # The RAS history always has the report that confirmed the hang.
    if (tool, outcome) != (nccl_ras.NCCL_RAS_TOOL, "absent")
]


@pytest.mark.parametrize("diagnostics", _MESSAGE_CASES)
def test_confirmed_hang_message(monkeypatch, diagnostics):
    # The decision points the user at exactly the diagnostics that were
    # uploaded.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2)
    upload = detector.callback.upload

    def flaky_upload(tool, files):
        if diagnostics.get(tool) == "raises":
            raise RuntimeError("upload failed")
        return upload(tool, files)

    detector.callback.upload = flaky_upload
    for tool, outcome in diagnostics.items():
        if outcome == "absent":
            detector.results[_PROBE_OF_TOOL[tool]] = {}

    decision = confirm_hang(detector)

    assert isinstance(decision, Reattempt)
    for tool in _ALL_DIAGNOSTICS:
        produced = tool not in diagnostics
        assert (f"/exp/{HANG_DETECTOR_DIRNAME}/{tool}" in decision.reason) is produced
    assert detector.evaluator._is_ras_degraded is False


@pytest.mark.parametrize("action", [NCCL_RAS_ACTION_FAIL, NCCL_RAS_ACTION_OBSERVE])
def test_confirmed_hang_action(monkeypatch, caplog, propagate_logs, uploads, action):
    # Fail mode reattempts; observe mode logs the same message and keeps
    # training, but still captures every diagnostic: observing is worthless
    # without them.
    detector = make_detector(monkeypatch, action, confirm_count=2)

    with caplog.at_level(logging.WARNING, logger=nccl_ras.logger.name):
        decision = confirm_hang(detector)

    if action == NCCL_RAS_ACTION_FAIL:
        # The controller raises it as NCCLHangError.
        assert isinstance(decision, NCCLHangReattempt)
        message = decision.reason
    else:
        assert isinstance(decision, Noop)
        message = caplog.text
    assert "NCCL hang detected by the Ray Train NCCL hang detector!" in message
    assert "1 communicator(s)" in message
    assert uploaded_tools(uploads) == _ALL_DIAGNOSTICS


def test_confirmed_hang_is_diagnosed_once(monkeypatch, uploads):
    # Observe mode keeps training after the diagnosis, and the communicator
    # stays frozen: its hang is not diagnosed again.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2)

    confirm_hang(detector)
    for _ in range(3):
        assert not isinstance(detector.tick(_FROZEN), Reattempt)

    assert uploaded_tools(uploads) == _ALL_DIAGNOSTICS


def test_a_dropped_diagnosis_is_returned_again(monkeypatch, uploads):
    # The HealthManager drops a Diagnose returned while other on-demand checks
    # run, so the evaluator returns it again until it runs.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2)
    evaluator = detector.evaluator
    detector.tick(_FROZEN)
    detector.tick(_FROZEN)

    detector.run = lambda decision: decision  # the manager drops the Diagnose
    assert isinstance(detector.tick(_FROZEN), Diagnose)
    assert isinstance(detector.tick(_FROZEN), Diagnose)
    del detector.run

    decision = detector.tick(_FROZEN)
    assert isinstance(decision, Reattempt)
    assert uploaded_tools(uploads) == _ALL_DIAGNOSTICS
    assert not isinstance(evaluator.evaluate(_frozen_state(detector)), Diagnose)


def _frozen_state(detector):
    detector._time_s += 1
    return HealthState(
        probe_results={
            "NCCLRASProbe": {
                _NODE: ProbeResult(
                    detail=to_ras_json(_FROZEN), timestamp_s=detector._time_s
                )
            }
        }
    )


def test_capture_diagnostic_swallows_failures(caplog, propagate_logs):
    # A diagnostic that fails must never take the run's hang handling with it.
    def boom():
        raise RuntimeError("upload failed")

    with caplog.at_level(logging.ERROR, logger=nccl_ras.logger.name):
        assert nccl_ras.capture_diagnostic("worker stack traces", boom) is None
    assert "worker stack traces" in caplog.text


# ---------------------------------------------------------------------------
# Which report is evaluated
# ---------------------------------------------------------------------------


def _state(**results):
    return HealthState(probe_results={"NCCLRASProbe": results})


def test_each_report_is_evaluated_once(monkeypatch):
    # Every node sees the whole RAS mesh: the detector reads the first node
    # that answered, and keeps reading it while it answers.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=5)
    evaluator = detector.evaluator
    a1 = ProbeResult(detail=to_ras_json(_FROZEN), timestamp_s=1)
    b1 = ProbeResult(detail=to_ras_json(_FROZEN), timestamp_s=1.5)

    evaluator.evaluate(_state(**{"node-a": a1, "node-b": b1}))
    assert len(evaluator.ras_history) == 1
    # node-b has a newer report, but node-a has none: nothing new.
    b2 = ProbeResult(detail=to_ras_json(_FROZEN), timestamp_s=2.5)
    evaluator.evaluate(_state(**{"node-a": a1, "node-b": b2}))
    assert len(evaluator.ras_history) == 1
    # node-a fails, so node-b takes over.
    failed = ProbeResult(error=RASQueryError("exit_1"), timestamp_s=2)
    evaluator.evaluate(_state(**{"node-a": failed, "node-b": b2}))
    assert len(evaluator.ras_history) == 2
    assert evaluator.comm_deadlock_count == {_COMM_A: 1}


def test_no_report_before_nccl_starts(monkeypatch):
    # Until NCCL creates its first communicator there is no RAS service, so
    # the queries fail without disabling detection.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=1)
    refused = ProbeResult(error=RASQueryError("exit_1"), timestamp_s=1)

    assert isinstance(detector.evaluator.evaluate(_state(**{_NODE: refused})), Noop)
    assert detector.evaluator._is_ras_degraded is False
    assert detector.evaluator.prev_report is None


def test_a_fatal_error_on_one_node_does_not_disable_detection(monkeypatch):
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=5)
    fatal = RASQueryError("binary_not_found", fatal=True)

    detector.evaluator.evaluate(
        _state(
            **{
                "node-a": ProbeResult(error=fatal, timestamp_s=1),
                "node-b": ProbeResult(detail=to_ras_json(_FROZEN), timestamp_s=1),
            }
        )
    )
    assert detector.evaluator._is_ras_degraded is False
    assert len(detector.evaluator.ras_history) == 1


# ---------------------------------------------------------------------------
# Degrading, and the config
# ---------------------------------------------------------------------------


def test_detector_degrades_on_fatal_poll_result(monkeypatch, uploads):
    fatal = RASQueryError("unsupported_f_option", "binary rejected `-f`", fatal=True)
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=1)

    detector.tick(fatal)  # fatal -> degrade
    assert detector.evaluator._is_ras_degraded is True
    detector.tick(create_single_comm_report({2: 5, 3: 3}))  # no-op once degraded
    assert detector.evaluator.prev_report is None
    assert detector.evaluator.comm_deadlock_count == {}
    assert not uploads

    # It stays disabled for a later attempt.
    detector.evaluator.on_reset()
    detector.tick(create_single_comm_report({2: 5, 3: 3}))
    assert detector.evaluator.prev_report is None


def test_unexpected_error_disables_detection(monkeypatch):
    # A bug in the detection path must never crash training: it is logged and
    # detection is disabled for the rest of the run.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=1)

    def boom(*_args, **_kwargs):
        raise RuntimeError("detector bug")

    detector.evaluator.next_result = boom
    assert isinstance(detector.tick(create_single_comm_report({2: 5, 3: 3})), Noop)
    assert detector.evaluator._is_ras_degraded is True
    # Once degraded, later polls are a no-op.
    detector.tick(create_single_comm_report({2: 5, 3: 3}))
    assert detector.evaluator.prev_report is None


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
        NCCLHangHealthCallback()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"poll_interval_s": 0},
        {"confirm_duration_s": -1},
        {"action": "not-a-mode"},
    ],
)
def test_invalid_check_fails_fast(kwargs):
    with pytest.raises(ValueError):
        nccl_ras.nccl_hang_check(MagicMock(), **kwargs)


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
    monkeypatch.delenv(NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR, raising=False)
    monkeypatch.delenv(NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, raising=False)
    if duration_s is not None:
        monkeypatch.setenv(NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR, duration_s)
    if interval_s is not None:
        monkeypatch.setenv(NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, interval_s)

    check = NCCLHangHealthCallback().health_check()
    (evaluator,) = check.evaluator_creator()
    assert evaluator._confirm_poll_counts == expected_polls
    assert check.poll_interval_s == float(interval_s or 15)
    # Escalation milestones stay below the confirmation streak so a short window
    # still warns before it fails.
    assert 1 <= evaluator._suspicion_polls <= max(1, expected_polls - 1)
    assert evaluator._periodic_warn_polls >= 1


def test_default_action_is_observe(monkeypatch):
    monkeypatch.delenv(NCCL_RAS_ACTION_ENV_VAR, raising=False)
    (evaluator,) = NCCLHangHealthCallback().health_check().evaluator_creator()
    assert evaluator._action == NCCL_RAS_ACTION_OBSERVE


# ---------------------------------------------------------------------------
# Suspected hangs: the warnings, and NCCL's text report
# ---------------------------------------------------------------------------


def test_suspicion_and_periodic_messages_fail_mode(monkeypatch, caplog, propagate_logs):
    # A frozen communicator builds a streak. confirm_count is high enough that no
    # hang is confirmed, so we can observe the escalating warnings on the way up.
    # First-suspicion fires at 1 poll, periodic reminder every 2 (helper defaults).
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=100)

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for _ in range(4):
            detector.tick(_FROZEN)

    text = caplog.text
    # New-suspicion announcement describes the stalled communicator.
    assert "Possible NCCL hang detected by the Ray Train NCCL hang detector!" in text
    assert f"Communicator {_COMM_A} over" in text
    # Periodic reminder uses the "still suspected" wording.
    assert "NCCL hang still suspected by the Ray Train NCCL hang detector!" in text
    # Fail mode says the run will be reattempted.
    assert "The run will be reattempted in" in text
    # The RAS report is introduced so users know where it comes from, and is
    # logged only once for the whole suspected hang.
    assert text.count(TEXT_REPORT) == 1
    assert "NCCL's own `ncclras` report of the suspected hang" in text


def test_periodic_warning_never_precedes_the_first_suspicion(
    monkeypatch, caplog, propagate_logs
):
    # A periodic interval shorter than the first-suspicion threshold must not
    # say "still suspected" before anything was suspected, nor both at once.
    detector = make_detector(
        monkeypatch,
        NCCL_RAS_ACTION_FAIL,
        confirm_count=100,
        first_suspicion_polls=4,
        periodic_warn_every_polls=2,
    )

    warnings = []
    with caplog.at_level(logging.WARNING, logger=nccl_ras.logger.name):
        for _ in range(8):
            caplog.clear()
            detector.tick(create_single_comm_report({1: 5, 2: 4}))
            warnings.append([r.getMessage().split("!")[0] for r in caplog.records])

    # Frozen streaks 1..7 follow the baseline poll.
    assert warnings == [
        [],
        [],
        [],
        [],
        ["Possible NCCL hang detected by the Ray Train NCCL hang detector"],
        [],
        ["NCCL hang still suspected by the Ray Train NCCL hang detector"],
        [],
    ]


def test_ras_text_report_is_logged_again_for_a_new_hang(
    monkeypatch, caplog, propagate_logs
):
    stalled = create_single_comm_report({1: 5, 2: 4})
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=100)

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for report in [stalled, stalled, create_healthy_report(), stalled, stalled]:
            detector.tick(report)

    assert caplog.text.count(TEXT_REPORT) == 2


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
    detector = make_detector(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        confirm_count=100,
        periodic_warn_every_polls=1,
    )

    logged_at = []
    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for poll, comms in enumerate(polls):
            caplog.clear()
            detector.tick(create_report(comms))
            if TEXT_REPORT in caplog.text:
                logged_at.append(poll)

    assert logged_at == text_polls


def test_ras_text_report_fetch_is_retried_while_suspected(
    monkeypatch, caplog, propagate_logs
):
    # A failed fetch at the first suspicion is retried on the next warning
    # rather than lost until the hang is confirmed.
    detector = make_detector(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        confirm_count=100,
        periodic_warn_every_polls=1,
    )
    detector.text_results = [ProbeResult(error=RASQueryError("exit_1"))]

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for _ in range(4):
            detector.tick(create_single_comm_report({1: 5, 2: 4}))

    assert caplog.text.count(TEXT_REPORT) == 1
    assert "Could not fetch the `ncclras` text report (exit_1)" in caplog.text
    assert detector.diagnoses == [["NCCLRASTextProbe"], ["NCCLRASTextProbe"]]


def test_escalation_absent_in_observe_mode(monkeypatch, caplog, propagate_logs):
    # Observe mode still surfaces the suspicion/periodic warnings but must never
    # say the run will be reattempted, since it only observes.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=100)

    with caplog.at_level(logging.INFO, logger=nccl_ras.logger.name):
        for _ in range(4):
            detector.tick(_FROZEN)

    text = caplog.text
    assert "Possible NCCL hang detected by the Ray Train NCCL hang detector!" in text
    assert "NCCL hang still suspected by the Ray Train NCCL hang detector!" in text
    assert "reattempted" not in text


# ---------------------------------------------------------------------------
# Diagnostics: the uploaded files
# ---------------------------------------------------------------------------


def make_diagnostics(monkeypatch, workers=None, ras_history=()):
    detector = make_detector(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, workers=workers
    )
    return HangDiagnostics(
        detector.callback, "hang", list(ras_history), NCCL_RAS_ACTION_FAIL, lambda: None
    )


_PER_RANK_DIAGNOSTICS = [
    pytest.param(
        "upload_stack_traces",
        nccl_ras.STACK_TRACES_TOOL,
        lambda rank: f"rank_{rank}.log",
        lambda reason: reason,
        id="stack_traces",
    ),
    pytest.param(
        "upload_flight_recorder",
        nccl_ras.FLIGHT_RECORDER_TOOL,
        lambda rank: f"rank_{rank}.json",
        # Still valid JSON, so every file in the directory parses.
        lambda reason: json.dumps({"dump_error": reason}),
        id="flight_recorder",
    ),
]


@pytest.mark.parametrize("method,tool,filename,placeholder", _PER_RANK_DIAGNOSTICS)
def test_diagnostic_uploads_one_file_per_rank(
    monkeypatch, uploads, method, tool, filename, placeholder
):
    # Every rank gets its own file, named so it can be matched to the RAS
    # report, holding exactly what the probe returned, or why there is none.
    diagnostics = make_diagnostics(monkeypatch)
    reason = "it went wrong"
    results = {
        0: ProbeResult(error=RuntimeError(reason)),
        1: ProbeResult(detail="dump 1"),
    }

    fs_path = getattr(diagnostics, method)(results)

    assert fs_path == f"/exp/{HANG_DETECTOR_DIRNAME}/{tool}"
    assert uploads == [
        (fs_path, {filename(0): placeholder(reason), filename(1): "dump 1"})
    ]


def test_nvidia_smi_uploads_one_file_per_node(monkeypatch, uploads):
    # Each node's file is named by its IP, which RAS reports the ranks by.
    workers = [
        make_train_worker(0, "10.0.0.1", 100, node_id="node-a"),
        make_train_worker(1, "10.0.0.1", 101, node_id="node-a"),
        make_train_worker(2, "10.0.0.2", 100, node_id="node-b"),
    ]
    diagnostics = make_diagnostics(monkeypatch, workers=workers)

    diagnostics.upload_nvidia_smi(
        {
            "node-a": ProbeResult(detail="node 1 GPUs"),
            "node-b": ProbeResult(error=RuntimeError("timed out")),
            # A node with no worker left is named by its ID.
            "node-c": ProbeResult(detail="node 3 GPUs"),
        }
    )

    ((_, files),) = uploads
    assert files == {
        "node_10.0.0.1.log": "node 1 GPUs",
        "node_10.0.0.2.log": "no `nvidia-smi` snapshot: timed out",
        "node_node-c.log": "node 3 GPUs",
    }


@pytest.mark.parametrize(
    "method",
    [
        "upload_stack_traces",
        "upload_flight_recorder",
        "upload_nvidia_smi",
        "upload_ras_history",
    ],
)
def test_diagnostic_skipped(monkeypatch, uploads, method):
    # With nothing to record there is no empty directory to leave behind in the
    # user's experiment directory, and the decision does not point at one.
    diagnostics = make_diagnostics(monkeypatch)

    assert getattr(diagnostics, method)({}) is None
    assert uploads == []


def test_ras_history_buffer(monkeypatch):
    # Every report is recorded, healthy or not, evicting the oldest beyond.
    confirm_count = 3
    maxlen = confirm_count + nccl_ras._RAS_HISTORY_MARGIN_POLLS
    reports = [create_single_comm_report({1: count}) for count in range(maxlen + 5)]
    detector = make_detector(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=confirm_count
    )

    for report in reports:
        detector.tick(report)

    history = detector.evaluator.ras_history
    assert history.maxlen == maxlen
    assert [r.comm_op_counts for r in history] == [
        r.comm_op_counts for r in reports[-maxlen:]
    ]


@pytest.mark.parametrize(
    "text_report", ["human readable report", None], ids=["with_text", "without_text"]
)
def test_ras_history_upload(monkeypatch, uploads, text_report):
    # Each retained poll is written verbatim
    diagnostics = make_diagnostics(
        monkeypatch,
        ras_history=[
            parse_ras_schema(ras_json)
            for ras_json in (HEALTHY_RAS_JSON, DEAD_RANK_RAS_JSON, MULTI_COMM_RAS_JSON)
        ],
    )
    text_results = (
        {_NODE: ProbeResult(detail=text_report)}
        if text_report
        else {_NODE: ProbeResult(error=RASQueryError("exit_1"))}
    )

    fs_path = diagnostics.upload_ras_history(text_results)

    assert fs_path == f"/exp/{HANG_DETECTOR_DIRNAME}/nccl_ras"
    expected = {
        "ncclras_2026-06-19-06-51-56.json": HEALTHY_RAS_JSON,
        "ncclras_2026-06-19-06-55-57.json": DEAD_RANK_RAS_JSON,
        "ncclras_2026-06-19-06-56-24.json": MULTI_COMM_RAS_JSON,
    }
    if text_report:
        expected["ncclras_report.txt"] = text_report
    assert uploads == [(fs_path, expected)]


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class MockGauge:
    """Stand-in for ``ray.util.metrics.Gauge`` that keeps the values set."""

    def __init__(self, name: str, description: str, tag_keys: tuple = ()):
        self.values: Dict[frozenset, float] = {}

    def set(self, value: float, tags: Dict[str, str]):
        self.values[frozenset(tags.items())] = value


@pytest.fixture
def mock_gauge(monkeypatch):
    monkeypatch.setattr(metrics_base, "Gauge", MockGauge)


def recorded_detector_metrics(callback):
    """The detector's ``(state, stall duration)`` metrics as last recorded."""
    state_metric = callback._metrics[nccl_hang_health.GRAFANA_STATE]
    (state,) = [s for s in NCCLHangDetectorState if state_metric.get_value(s)]
    return (
        state,
        callback._metrics[nccl_hang_health.GRAFANA_STALL_DURATION_S].get_value(),
    )


def test_detector_metrics_follow_suspicion_confirmation_and_recovery(
    monkeypatch, mock_gauge
):
    # A communicator freezes until confirmed (observe mode, so the run goes
    # on), then recovers. The state follows the log thresholds: suspected from
    # the first suspicion warning, confirmed at the confirm duration. The stall
    # duration counts from the first frozen poll.
    frozen = create_single_comm_report({1: 5, 2: 4})
    detector = make_detector(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        confirm_count=3,
        first_suspicion_polls=2,
    )
    detector.callback.after_controller_start(create_dummy_run_context())

    recorded = []
    for report in [frozen] * 4 + [create_healthy_report()]:
        detector.tick(report)
        recorded.append(recorded_detector_metrics(detector.callback))

    assert recorded == [
        (NCCLHangDetectorState.HEALTHY, 0.0),  # baseline, nothing to diff against
        (NCCLHangDetectorState.HEALTHY, 1.0),  # frozen, below first suspicion
        (NCCLHangDetectorState.SUSPECTED, 2.0),
        (NCCLHangDetectorState.CONFIRMED, 3.0),
        (NCCLHangDetectorState.HEALTHY, 0.0),  # recovered
    ]


def test_detector_metrics_record_confirmation_before_acting(monkeypatch, mock_gauge):
    # In fail mode the confirmation reattempts the run, so the confirmed state
    # must already be recorded by then or it would never reach Grafana.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2)
    detector.callback.after_controller_start(create_dummy_run_context())
    recorded = []
    detector.run = lambda decision: (
        recorded.append(recorded_detector_metrics(detector.callback)),
        decision,
    )[1]

    assert isinstance(confirm_hang(detector), Diagnose)
    assert recorded[-1] == (NCCLHangDetectorState.CONFIRMED, 2.0)


def test_detector_metrics_tagged_with_the_run(monkeypatch, mock_gauge):
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2)
    run_context = create_dummy_run_context()
    detector.callback.after_controller_start(run_context)
    detector.tick(create_healthy_report())

    base_tags = {
        "ray_train_run_name": run_context.get_run_config().name,
        "ray_train_run_id": run_context.run_id,
    }
    metrics = detector.callback._metrics
    stall_metric = metrics[nccl_hang_health.GRAFANA_STALL_DURATION_S]
    assert stall_metric._gauge.values == {frozenset(base_tags.items()): 0.0}
    # The state is an enum metric: one series per state, tagged with its name.
    state_tags = {**base_tags, nccl_hang_health.GRAFANA_STATE_TAG_KEY: "HEALTHY"}
    state_metric = metrics[nccl_hang_health.GRAFANA_STATE]
    assert state_metric._gauge.values == {frozenset(state_tags.items()): 1}


def test_detector_metrics_reset_to_healthy_on_a_new_attempt(monkeypatch, mock_gauge):
    # A new attempt starts with a new worker group, so the panel must not keep
    # showing CONFIRMED while the new group comes up.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2)
    detector.callback.after_controller_start(create_dummy_run_context())
    confirm_hang(detector)
    assert recorded_detector_metrics(detector.callback) == (
        NCCLHangDetectorState.CONFIRMED,
        2.0,
    )

    detector.evaluator.on_reset()
    assert recorded_detector_metrics(detector.callback) == (
        NCCLHangDetectorState.HEALTHY,
        0.0,
    )


@pytest.mark.parametrize("cause", ["fatal_poll_result", "unexpected_error"])
def test_detector_metrics_record_degraded(monkeypatch, mock_gauge, cause):
    # Once detection is disabled the panel shows DEGRADED rather than a stale
    # HEALTHY, including on a new attempt.
    fatal = RASQueryError("unsupported_f_option", "binary rejected `-f`", fatal=True)
    frozen = create_single_comm_report({1: 5, 2: 4})
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=5)
    detector.callback.after_controller_start(create_dummy_run_context())
    detector.tick(frozen)
    detector.tick(frozen)
    assert recorded_detector_metrics(detector.callback) == (
        NCCLHangDetectorState.SUSPECTED,
        1.0,
    )

    if cause == "unexpected_error":

        def boom(*_args, **_kwargs):
            raise RuntimeError("detector bug")

        detector.evaluator.next_result = boom
        detector.tick(frozen)
    else:
        detector.tick(fatal)
    assert detector.evaluator._is_ras_degraded is True
    assert recorded_detector_metrics(detector.callback) == (
        NCCLHangDetectorState.DEGRADED,
        0,
    )

    detector.evaluator.on_reset()
    assert recorded_detector_metrics(detector.callback) == (
        NCCLHangDetectorState.DEGRADED,
        0,
    )


def test_detector_metrics_reset_on_controller_shutdown(monkeypatch, mock_gauge):
    # A finished run must not leave its last state set on the gauges.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=5)
    detector.callback.after_controller_start(create_dummy_run_context())
    detector.tick(_FROZEN)
    detector.tick(_FROZEN)

    asyncio.run(detector.callback.before_controller_shutdown())

    for metric in detector.callback._metrics.values():
        assert set(metric._gauge.values.values()) == {0}


def test_detector_metrics_shutdown_before_controller_start(monkeypatch):
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=5)
    asyncio.run(detector.callback.before_controller_shutdown())  # must not raise


def test_detector_metrics_skipped_before_controller_start(monkeypatch):
    # Without a run context there is nothing to tag the metrics with, so
    # detection runs without recording them.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=5)
    detector.tick(_FROZEN)
    detector.tick(_FROZEN)
    assert detector.callback._metrics is None
    assert detector.evaluator.comm_deadlock_count == {_COMM_A: 1}


# ---------------------------------------------------------------------------
# Naming the ranks
# ---------------------------------------------------------------------------


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
        WorkerInfo(0, "node-a", "10.0.0.1", 100),
        WorkerInfo(1, "node-b", "10.0.0.2", 100),
        WorkerInfo(2, "node-b", "10.0.0.2", 200),
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


def test_workers_come_from_the_worker_group(monkeypatch):
    callback = make_detector(
        monkeypatch,
        NCCL_RAS_ACTION_OBSERVE,
        confirm_count=2,
        workers=[make_train_worker(3, "10.0.0.2", 200, node_id="node-b")],
    ).callback
    assert callback.workers() == [WorkerInfo(3, "node-b", "10.0.0.2", 200)]

    callback.before_worker_group_shutdown(MagicMock())
    assert callback.workers() == []


# Comm ranks 0 and 1 are Ray Train ranks 2 and 3 on two nodes.
_PAIR_PROCESSES = {_COMM_A: {0: ("10.0.0.1", 100), 1: ("10.0.0.2", 200)}}
_PAIR_WORKERS = [
    make_train_worker(2, "10.0.0.1", 100, node_id="node-a"),
    make_train_worker(3, "10.0.0.2", 200, node_id="node-b"),
]


def test_confirmed_hang_names_the_train_ranks(monkeypatch):
    report = create_report({_COMM_A: {0: 5, 1: 4}}, processes=_PAIR_PROCESSES)
    detector = make_detector(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, workers=_PAIR_WORKERS
    )

    detector.tick(report)
    detector.tick(report)
    decision = detector.tick(report)

    # Comm ranks are translated to train ranks, without blaming either side.
    assert (
        f"Communicator {_COMM_A} over train ranks 2-3, no progress for 2s. "
        "AllReduce launches: 5 by train rank 2; 4 by train rank 3"
    ) in decision.reason


def test_only_mismatched_ops_are_described(monkeypatch):
    report = create_single_comm_report(
        {
            0: {"AllGather": 3, "AllReduce": 9, "Broadcast": 1},
            1: {"AllGather": 2, "AllReduce": 9, "Broadcast": 1},
            2: {"AllGather": 3, "AllReduce": 7, "Broadcast": 1},
        }
    )
    detector = make_detector(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2, workers=[]
    )

    line = detector.evaluator.describe_stalled_comms(report, [_COMM_A])
    assert "AllGather launches: 3 by comm ranks 0, 2" in line
    assert "AllReduce launches: 9 by comm ranks 0-1" in line
    assert "Broadcast" not in line


def test_suspicion_warnings_name_the_train_ranks(monkeypatch, caplog, propagate_logs):
    report = create_report({_COMM_A: {0: 5, 1: 4}}, processes=_PAIR_PROCESSES)
    detector = make_detector(
        monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=100, workers=_PAIR_WORKERS
    )

    with caplog.at_level(logging.WARNING, logger=nccl_ras.logger.name):
        for _ in range(4):
            detector.tick(report)

    warnings = [record.getMessage() for record in caplog.records]
    assert len(warnings) == 2
    for message in warnings:
        assert "5 by train rank 2; 4 by train rank 3" in message


def test_unmatched_rank_falls_back_to_the_ras_process(monkeypatch):
    # A rank RAS knows but Ray Train doesn't (e.g. a user-spawned subprocess)
    # is still named by where RAS says it runs.
    report = create_report(
        {_COMM_A: {0: 5, 1: 4}},
        processes={_COMM_A: {0: ("10.0.0.1", 100), 1: ("10.0.0.9", 999)}},
    )
    detector = make_detector(
        monkeypatch, NCCL_RAS_ACTION_FAIL, confirm_count=2, workers=_PAIR_WORKERS
    )

    detector.tick(report)
    detector.tick(report)
    decision = detector.tick(report)

    assert "4 by comm rank 1 (no matching train worker)" in decision.reason


def test_describing_ranks_never_raises(monkeypatch):
    # Missing log detail must never disable detection.
    detector = make_detector(monkeypatch, NCCL_RAS_ACTION_OBSERVE, confirm_count=2)
    detector.callback._worker_group.get_workers.side_effect = RuntimeError("boom")
    report = create_single_comm_report({0: 5, 1: 4})

    assert detector.evaluator.describe_stalled_comms(report, [_COMM_A]) == ""


# ---------------------------------------------------------------------------
# The env var
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("enabled", [True, False])
def test_the_env_var_turns_on_the_health_check(monkeypatch, enabled):
    # The env var registers the health version; NCCLRASCallback is never
    # registered.
    from ray.train.v2._internal.callbacks.nccl_ras import NCCLRASCallback
    from ray.train.v2.api.config import ScalingConfig
    from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer

    monkeypatch.setenv(ENABLE_NCCL_HANG_DETECTOR_ENV_VAR, "1" if enabled else "0")
    trainer = DataParallelTrainer(
        lambda: None, scaling_config=ScalingConfig(num_workers=1)
    )

    callbacks = trainer._create_default_callbacks()

    assert any(isinstance(c, NCCLHangHealthCallback) for c in callbacks) is enabled
    assert not any(isinstance(c, NCCLRASCallback) for c in callbacks)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
