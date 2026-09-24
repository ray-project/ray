"""Controller integration of the health loop: Collect -> Decide -> Act.

Registered when ``RunConfig.health_config`` has policies.

- Collect: ``health.report()`` rides ``WorkerStatus.health`` on the existing
  poll; cluster probes run on a background thread.
- Decide: ``HealthManager.poll_decision()`` once per controller poll.
- Act: ``UserCallback.after_health_decision`` first. ``DIAGNOSE`` pushes
  on-demand probes; ``REATTEMPT`` and ``EVICT`` raise ``HealthDecisionError``
  into the failure policy; evicted nodes are excluded by label selector.
- Pre-flight: before each worker group is scheduled, pre-flight probes run on
  candidate nodes not yet screened, and rejected nodes are evicted.

Not wired yet: ``WorkerProbe`` execution, the ``NodeMonitor`` (node-scoped
probes run in a task pinned to the node instead), and ``stop_workers``.
"""
import functools
import logging
import os
import queue
import tempfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional, Set, Union

import ray
from ray.train.health._internal.manager import HealthManager, Results
from ray.train.health._internal.on_demand import OnDemandRunner
from ray.train.health.decision import Action, Diagnose, HealthDecision
from ray.train.health.exceptions import HealthDecisionError
from ray.train.health.policy import HealthConfig
from ray.train.health.probe import (
    ClusterContext,
    ClusterProbe,
    OnDemandProbe,
    OnDemandProbeContext,
    ProbeResult,
)
from ray.train.health.state import WorkerHealth
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.storage import _upload_to_fs_path
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.context import TrainRunContext
    from ray.train.v2._internal.execution.worker_group import (
        WorkerGroup,
        WorkerGroupContext,
        WorkerGroupPollStatus,
    )
    from ray.train.v2.api.callback import UserCallback
    from ray.train.v2.api.config import ScalingConfig

logger = logging.getLogger(__name__)

NODE_ID_LABEL_KEY = "ray.io/node-id"
_DIAGNOSTICS_DIR = "health_diagnostics"


def build_node_exclusion_selector(
    excluded_node_ids: List[str],
) -> Optional[Dict[str, str]]:
    """A bundle label selector that keeps workers off ``excluded_node_ids``."""
    if not excluded_node_ids:
        return None
    return {NODE_ID_LABEL_KEY: f"!in({','.join(sorted(excluded_node_ids))})"}


def _candidate_nodes(resources_per_worker: Dict[str, float]) -> List[str]:
    """Alive nodes large enough to host at least one worker."""
    return sorted(
        node["NodeID"]
        for node in ray.nodes()
        if node["Alive"]
        and all(
            node["Resources"].get(k, 0) >= v
            for k, v in resources_per_worker.items()
            if v > 0
        )
    )


# ----------------------------------------------------------------------
# On-demand probe dispatch
# ----------------------------------------------------------------------
def _upload_files(storage_filesystem, base_path: str, name: str, files) -> str:
    dest = os.path.join(base_path, _DIAGNOSTICS_DIR, name)
    with tempfile.TemporaryDirectory() as tmp:
        for filename, contents in files.items():
            (Path(tmp) / filename).write_text(contents)
        _upload_to_fs_path(tmp, storage_filesystem, dest)
    return dest


def _poll_on_demand_probe(probe: OnDemandProbe, ctx: OnDemandProbeContext):
    return probe.poll(ctx)


_poll_on_demand_probe_task = ray.remote(num_cpus=0)(_poll_on_demand_probe)


def _gather(
    refs: Dict[str, "ray.ObjectRef"], timeout_s: float
) -> Dict[str, Union[ProbeResult, BaseException]]:
    ready, _ = ray.wait(list(refs.values()), num_returns=len(refs), timeout=timeout_s)
    ready = set(ready)
    out: Dict[str, Union[ProbeResult, BaseException]] = {}
    for entity_id, ref in refs.items():
        if ref not in ready:
            out[entity_id] = TimeoutError(f"timed out after {timeout_s:.0f}s")
            continue
        try:
            out[entity_id] = ray.get(ref)
        except Exception as e:
            out[entity_id] = e
    return out


def _run_on_nodes(probe: OnDemandProbe, contexts: List[OnDemandProbeContext]):
    refs = {
        ctx.entity_id: _poll_on_demand_probe_task.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=ctx.node_id, soft=False
            )
        ).remote(probe, ctx)
        for ctx in contexts
    }
    return _gather(refs, probe.timeout_s)


class _ClusterProbeThread:
    """Polls cluster probes on their intervals and queues results for the
    controller thread, which is the only thread that touches manager state."""

    def __init__(self, manager: HealthManager, ctx: ClusterContext):
        self._manager = manager
        self._ctx = ctx
        self._probes: List[ClusterProbe] = manager.cluster_probes()
        self._results: "queue.SimpleQueue" = queue.SimpleQueue()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="ray-train-health-probes", daemon=True
        )

    def start(self) -> None:
        if self._probes:
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def drain(self):
        while True:
            try:
                yield self._results.get_nowait()
            except queue.Empty:
                return

    def _run(self) -> None:
        next_due = {id(p): 0.0 for p in self._probes}
        while not self._stop.is_set():
            for probe in self._probes:
                if time.monotonic() < next_due[id(probe)]:
                    continue
                next_due[id(probe)] = time.monotonic() + max(probe.interval_s, 0.1)
                results = self._manager.poll_cluster_probe(probe, self._ctx)
                if results is not None:
                    self._results.put((probe, results, time.time()))
            wait = min(next_due.values()) - time.monotonic()
            self._stop.wait(max(0.05, wait))


class HealthCallback(ControllerCallback, WorkerGroupCallback):
    """Owns the run's ``HealthManager`` and carries out its decisions."""

    def __init__(
        self,
        health_config: HealthConfig,
        user_callbacks: Optional[List["UserCallback"]] = None,
        train_run_context: Optional["TrainRunContext"] = None,
    ):
        self._manager = HealthManager(health_config.policies)
        self._user_callbacks = list(user_callbacks or [])
        self._train_run_context = train_run_context
        self._rank_to_node: Dict[int, str] = {}
        self._probe_thread: Optional[_ClusterProbeThread] = None
        self._runner: Optional[OnDemandRunner] = None
        self._screened: Set[str] = set()

    @property
    def manager(self) -> HealthManager:
        return self._manager

    # ------------------------------------------------------------------
    # Pre-flight and scheduling
    # ------------------------------------------------------------------
    def on_controller_start_worker_group(
        self, *, scaling_config: "ScalingConfig", num_workers: int
    ) -> Optional[Dict[str, str]]:
        self._run_preflight(scaling_config._resources_per_worker_not_none)
        return build_node_exclusion_selector(self._manager.evicted_nodes)

    def _run_preflight(self, resources_per_worker: Dict[str, float]) -> None:
        probes = self._manager.preflight_probes()
        if not probes:
            return
        evicted = set(self._manager.evicted_nodes)
        candidates = [
            n
            for n in _candidate_nodes(resources_per_worker)
            if n not in self._screened and n not in evicted
        ]
        if not candidates:
            return
        self._screened.update(candidates)

        try:
            results = OnDemandRunner(run_on_node=_run_on_nodes).preflight(
                probes, candidates
            )
        except Exception:
            logger.exception("[Health] pre-flight failed to run.")
            return
        decision = self._manager.evaluate_preflight(results)
        if decision is None:
            logger.info("[Health] pre-flight passed on %d node(s).", len(candidates))
            return
        self._notify(decision)

    # ------------------------------------------------------------------
    # Worker group lifecycle
    # ------------------------------------------------------------------
    def after_worker_group_start(self, worker_group: "WorkerGroup") -> None:
        self._stop_probe_thread()
        workers = {
            w.distributed_context.world_rank: w for w in worker_group.get_workers()
        }
        self._rank_to_node = {r: w.metadata.node_id for r, w in workers.items()}
        self._manager.on_worker_group_start()

        ctx = ClusterContext(
            node_ids=sorted(set(self._rank_to_node.values())),
            rank_to_node=dict(self._rank_to_node),
        )
        self._probe_thread = _ClusterProbeThread(self._manager, ctx)
        self._probe_thread.start()

        def run_on_workers(probe, contexts):
            refs = {
                ctx.entity_id: workers[ctx.rank].execute_async(
                    _poll_on_demand_probe, probe, ctx
                )
                for ctx in contexts
            }
            return _gather(refs, probe.timeout_s)

        storage = worker_group._storage_context
        self._runner = OnDemandRunner(
            run_on_node=_run_on_nodes,
            run_on_worker=run_on_workers,
            upload=functools.partial(
                _upload_files, storage.storage_filesystem, storage.experiment_fs_path
            ),
        )

    def before_worker_group_shutdown(self, worker_group: "WorkerGroup") -> None:
        self._stop_probe_thread()

    def after_worker_group_abort(
        self, worker_group_context: "WorkerGroupContext"
    ) -> None:
        self._stop_probe_thread()

    def _stop_probe_thread(self) -> None:
        if self._probe_thread is not None:
            self._probe_thread.stop()
            self._probe_thread = None
        self._runner = None

    # ------------------------------------------------------------------
    # Collect -> Decide -> Act
    # ------------------------------------------------------------------
    def after_worker_group_poll_status(
        self, worker_group_status: "WorkerGroupPollStatus"
    ) -> None:
        if not self._manager.enabled:
            return
        try:
            for status in worker_group_status.worker_statuses.values():
                if isinstance(status.health, WorkerHealth):
                    self._manager.ingest_worker_health(status.health)
            if self._probe_thread is not None:
                for probe, results, at in self._probe_thread.drain():
                    self._manager.ingest_cluster_results(probe, results, at)
            decision = self._manager.poll_decision()
        except Exception:
            logger.exception("[Health] loop failed; skipping this poll.")
            return
        if decision is not None:
            self._act(decision)

    def _act(self, decision: HealthDecision) -> None:
        self._notify(decision)
        if decision.action is Action.DIAGNOSE:
            self._diagnose(decision)
        elif decision.action in (Action.REATTEMPT, Action.EVICT):
            raise HealthDecisionError(decision.reason, decision=decision)

    def _notify(self, decision: HealthDecision) -> None:
        logger.warning(
            "[Health] %s (%s): %s",
            decision.action.name,
            decision.cause.name,
            decision.reason,
        )
        for callback in self._user_callbacks:
            try:
                callback.after_health_decision(
                    run_context=self._train_run_context, health_decision=decision
                )
            except Exception:
                logger.exception(
                    "[Health] %s.after_health_decision raised.",
                    type(callback).__name__,
                )

    def _diagnose(self, decision: Diagnose) -> None:
        if self._runner is None:
            return
        try:
            results: Results = self._runner.diagnose(decision, self._rank_to_node)
        except Exception:
            logger.exception("[Health] diagnostics failed to run.")
            return
        self._manager.ingest_probe_results(results)
        for path in sorted(_artifacts(results)):
            logger.warning("[Health] diagnostic output: %s", path)


def _artifacts(results: Mapping[str, Mapping[str, ProbeResult]]) -> Set[str]:
    return {a for per in results.values() for r in per.values() for a in r.artifacts}
