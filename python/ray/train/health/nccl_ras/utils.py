"""What the NCCL hang detector's evaluators share: the parsed ``ncclras``
report, and the translation of its ranks to Ray Train's. Copied in order from
``NCCLRASCallback``'s module, ``ray.train.v2._internal.callbacks.nccl_ras``,
which keeps its own copies until it's deleted."""
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple

from ray.train.health.nccl_ras.constants import MAX_RANKS_LOGGED

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.worker_group import Worker

logger = logging.getLogger(__name__)


@dataclass
class RASReport:
    """Structured summary of an ``ncclras`` JSON report.

    Attributes:
        timestamp: str, when `ncclras` responded.
        comm_op_counts: Maps each communicator id to ``{global_rank: {op_name:
            count}}`` for ALL of that communicator's ranks (no filtering, no
            majority logic).
        comm_rank_status: Maps each communicator and their ranks with their
            status. A hang requires that the rank to be RUNNING.
        comm_rank_processes: Maps each communicator and their ranks to the
            ``(host, pid)`` of the process running that rank, used to find the
            Ray Train worker behind a communicator rank.
        raw_json: The ``ncclras`` output this report was parsed from
    """

    timestamp: str
    comm_op_counts: Dict[str, Dict[int, Dict[str, int]]]
    comm_rank_status: Dict[str, Dict[int, str]]
    comm_rank_processes: Dict[str, Dict[int, Tuple[str, int]]]
    raw_json: str

    @property
    def comm_op_skews(self) -> Dict[str, Dict[str, int]]:
        """Per-communicator, per-op spread (max-min) across its ranks, this poll.

        Returns:
            ``{comm_id: {op_name: max_count - min_count for ranks}}``.
        """
        skews: Dict[str, Dict[str, int]] = {}
        for comm_id, ranks in self.comm_op_counts.items():
            rank_op_counts = defaultdict(list)
            for op_counts in ranks.values():
                for op, count in op_counts.items():
                    rank_op_counts[op].append(count)
            skews[comm_id] = {
                op: max(counts) - min(counts) for op, counts in rank_op_counts.items()
            }
        return skews

    @property
    def mismatched_comms(self) -> Set[str]:
        """Communicator ids whose ranks have op counts that are mismatched and its ranks are RUNNING."""
        return {
            comm_id
            for comm_id, op_skews in self.comm_op_skews.items()
            if any(skews > 0 for skews in op_skews.values())
            and all(
                rank_status == "RUNNING"
                for rank_status in self.comm_rank_status[comm_id].values()
            )
        }

    def mismatched_op_ranks(self, comm_id: str) -> Dict[str, Dict[int, List[int]]]:
        """Group a communicator's ranks by launch count, for each mismatched op.

        Args:
            comm_id: A communicator in this report.

        Returns:
            ``{op_name: {count: [rank, ...]}}`` for the ops whose counts
            differ between the communicator's ranks.
        """
        rank_op_counts = self.comm_op_counts[comm_id]
        mismatched: Dict[str, Dict[int, List[int]]] = {}
        for op, skew in self.comm_op_skews[comm_id].items():
            if skew == 0:
                continue
            ranks_by_count: Dict[int, List[int]] = defaultdict(list)
            for rank, op_counts in rank_op_counts.items():
                ranks_by_count[op_counts.get(op, 0)].append(rank)
            mismatched[op] = ranks_by_count
        return mismatched

    @staticmethod
    def rank_status(status: Dict[str, Any]) -> str:
        """Convert a rank's status to a human-readable string."""
        # TODO: `async_error` is ignored currently in the upstream ras implementation
        #  https://github.com/NVIDIA/nccl/blob/master/src/ras/client_support.cc
        if status["abort_flag"] is True:
            return "ABORT"
        if status["finalize_called"] is True or status["destroy_flag"] is True:
            return "FINALIZE"
        if status["init_state"] == 0:
            return "RUNNING"
        else:
            return "INIT"


def compute_report_op_diff(
    prev: RASReport, curr: RASReport
) -> Dict[str, Dict[int, Dict[str, int]]]:
    """Per-communicator, per-rank op-count delta between two consecutive RAS reports.

    Args:
        prev: The previous poll's report.
        curr: The current poll's report.

    Returns:
        ``{comm_id: {rank: {op_name: cur - prev}}}``, restricted to the
        communicators and ranks the two reports share.
    """
    op_count_diff: Dict[str, Dict[int, Dict[str, int]]] = {}
    for comm_id, comm_curr_counts in curr.comm_op_counts.items():
        comm_prev_counts = prev.comm_op_counts.get(comm_id)
        if not comm_prev_counts:
            continue

        comm_diff: Dict[int, Dict[str, int]] = {}
        for rank, curr_counts in comm_curr_counts.items():
            if rank not in comm_prev_counts:
                continue

            prev_counts = comm_prev_counts[rank]
            comm_diff[rank] = {
                op: count - prev_counts.get(op, 0) for op, count in curr_counts.items()
            }

        op_count_diff[comm_id] = comm_diff
    return op_count_diff


# NCCL 2.28.9 emits malformed JSON for a ``missing_ranks[]`` with a missing comma
_MISSING_COMMA_RE = re.compile(r'([\d"el])(\s*\n\s*)("[^"\n]*"\s*:)')


def parse_ras_schema(ras_json: str) -> Optional[RASReport]:
    """Parse ``ncclras -f json`` output into a :class:`RASReport`.

    Targets the NCCL 2.28-2.30 JSON schema::

        {
          "nccl_version": ..., "communicators_count": N,
          "communicators": [
            {
              "hash": ..., "secondary_hash": ...,
              "size": ..., "ranks_count": ..., "missing_ranks_count": ...,
              "ranks": [
                {"rank": 0, "host": ..., "pid": ..., "cuda_dev": ...,
                 "status": {...}, "collective_counts": {...}}, ...
              ],
              "missing_ranks": [
                {"rank": 3, "host": ..., "pid": ...,
                 "status": {"unresponsive": true, "considered_dead": false}}, ...
              ]
            }, ...
          ]
        }

    Args:
        ras_json: The raw ``ncclras -f json`` output.

    Returns:
        The parsed :class:`RASReport`, or ``None`` if the parsing failed.
    """
    try:
        data = json.loads(ras_json)
    except (json.JSONDecodeError, TypeError):
        try:
            # NCCL 2.28.9 JSON ``missing_ranks[]`` has a missing comma, try repair with regex.
            repaired = _MISSING_COMMA_RE.sub(r"\1,\2\3", ras_json)
            data = json.loads(repaired)
        except (json.JSONDecodeError, TypeError) as e:
            logger.info("Parsing NCCL RAS failed with %s", e)
            return None

    try:
        comm_op_counts, comm_rank_status, comm_rank_processes = {}, {}, {}
        for comm in data["communicators"]:
            comm_op_counts[comm["hash"]] = {
                rank["rank"]: {
                    op: int(count) for op, count in rank["collective_counts"].items()
                }
                for rank in comm["ranks"]
            }
            comm_rank_status[comm["hash"]] = {
                rank["rank"]: RASReport.rank_status(rank["status"])
                for rank in comm["ranks"]
            }
            comm_rank_processes[comm["hash"]] = {
                rank["rank"]: (rank["host"], int(rank["pid"])) for rank in comm["ranks"]
            }

        return RASReport(
            timestamp=data["timestamp"],
            comm_op_counts=comm_op_counts,
            comm_rank_status=comm_rank_status,
            comm_rank_processes=comm_rank_processes,
            raw_json=ras_json,
        )
    except (KeyError, TypeError, ValueError) as e:
        logger.info(
            "NCCL RAS JSON did not match the expected schema: %s",
            e,
        )
        return None


def translate_ras_ranks_to_train(
    workers: List["Worker"], comm_rank_processes: Dict[int, Tuple[str, int]]
) -> Dict[int, int]:
    """Translate a communicator's ranks into Ray Train world ranks.

    RAS identifies a rank by the host and pid of its NCCL process, which for
    Ray Train is the worker actor's process. A pid is only unique per node, so
    ``(node_ip, pid)`` is matched first; the pid alone is the fallback when
    exactly one worker has it, because on multi-NIC nodes RAS can report a
    different interface's address than Ray's node IP.

    Args:
        workers: The train workers.
        comm_rank_processes: ``{comm_rank: (host, pid)}`` as reported by RAS.

    Returns:
        ``{comm_rank: train world rank}`` for the ranks with a matching train
        worker; ranks without one (e.g. a process the user spawned) are absent.
    """
    by_process: Dict[Tuple[str, int], int] = {}
    by_pid: Dict[int, List[int]] = defaultdict(list)
    for worker in workers:
        if worker.distributed_context is None:
            continue
        world_rank = worker.distributed_context.world_rank
        by_process[(worker.metadata.node_ip, worker.metadata.pid)] = world_rank
        by_pid[worker.metadata.pid].append(world_rank)

    train_rank_of: Dict[int, int] = {}
    for comm_rank, (host, pid) in comm_rank_processes.items():
        if (host, pid) in by_process:
            train_rank_of[comm_rank] = by_process[(host, pid)]
        elif len(by_pid.get(pid, [])) == 1:
            train_rank_of[comm_rank] = by_pid[pid][0]
    return train_rank_of


def format_ranks(comm_ranks: Iterable[int], train_rank_of: Dict[int, int]) -> str:
    """Name a communicator's ranks compactly, e.g. ``train ranks 0-3, 5``.

    Ranks are named by Ray Train world rank, falling back to the communicator
    rank for ranks without a train worker. Consecutive ranks collapse into
    ranges, and each list is cut after ``MAX_RANKS_LOGGED`` runs so a
    1000-rank job stays one line.

    Args:
        comm_ranks: Communicator ranks to name.
        train_rank_of: The translation from :func:`match_train_ranks`.

    Returns:
        The train ranks, then the unmatched communicator ranks.
    """
    comm_ranks = list(comm_ranks)
    groups = [
        ("train rank", [train_rank_of[r] for r in comm_ranks if r in train_rank_of]),
        ("comm rank", [r for r in comm_ranks if r not in train_rank_of]),
    ]
    parts = []
    for noun, ranks in groups:
        if not ranks:
            continue
        runs: List[List[int]] = []
        for rank in sorted(ranks):
            if runs and runs[-1][1] == rank - 1:
                runs[-1][1] = rank
            else:
                runs.append([rank, rank])
        shown = [
            str(lo) if lo == hi else f"{lo}-{hi}" for lo, hi in runs[:MAX_RANKS_LOGGED]
        ]
        hidden = sum(hi - lo + 1 for lo, hi in runs[MAX_RANKS_LOGGED:])
        if hidden:
            shown.append(f"and {hidden} more")
        plural = "s" if len(ranks) > 1 else ""
        parts.append(f"{noun}{plural} {', '.join(shown)}")
    if parts and groups[1][1]:
        parts[-1] += " (no matching train worker)"
    return ", ".join(parts)
