"""Shared by the health release scripts: the faults they inject, and a timeline
of what the controller did about them.

``Timeline`` records every health decision and every restart or error, with a
timestamp. Faults record when they fire, so the scripts print each event as
seconds after the fault.
"""
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import ray
from gpu_health import event_path
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

EVENTS_ACTOR = "health_release_events"

# Shared by every node of an Anyscale cluster.
SHARED_STORAGE = "/mnt/cluster_storage"


def ship_by_value(*modules) -> None:
    """The controller and workers can't import this directory, so send these
    modules by value."""
    for module in modules:
        ray.cloudpickle.register_pickle_by_value(module)


def storage_path() -> Optional[str]:
    return SHARED_STORAGE if os.path.isdir(SHARED_STORAGE) else None


# Nodes of the local cluster, one CPU worker each, like 4 nodes of 1 GPU.
LOCAL_NODES = 4


def workers_from_env() -> Optional[int]:
    """``HEALTH_TEST_WORKERS``, if set."""
    value = os.environ.get("HEALTH_TEST_WORKERS")
    return int(value) if value else None


def connect(
    local: bool = False,
    workers: Optional[int] = None,
    runtime_env: Optional[Dict] = None,
) -> Tuple[int, bool]:
    """Start a local cluster of ``LOCAL_NODES`` single-CPU nodes, or connect to
    the running cluster.

    Args:
        local: Whether to start a local cluster.
        workers: How many workers to train, if not one per node of the local
            cluster or one per GPU of the running cluster.
        runtime_env: The job's ``runtime_env``.

    Returns:
        How many workers to train, and whether they use GPUs.
    """
    if local:
        from ray.cluster_utils import Cluster

        cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 0})
        for _ in range(LOCAL_NODES):
            cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address, runtime_env=runtime_env)
        return workers or LOCAL_NODES, False
    ray.init(address="auto", runtime_env=runtime_env)
    gpus = int(ray.cluster_resources().get("GPU", 0))
    if not (workers or gpus):
        raise RuntimeError(
            "The cluster has no GPUs up: give it fixed-size GPU worker nodes, "
            "or set HEALTH_TEST_WORKERS."
        )
    return workers or gpus, True


# The apt `libnccl2`, which ships the `ncclras` client, on x86_64 Ubuntu.
SYSTEM_LIBNCCL = "/usr/lib/x86_64-linux-gnu/libnccl.so.2"


def system_nccl_version() -> Optional[Tuple[int, ...]]:
    """The version of the system NCCL, from the file ``SYSTEM_LIBNCCL`` links
    to, such as ``libnccl.so.2.28.7``."""
    match = re.search(
        r"libnccl\.so\.(\d+)\.(\d+)\.(\d+)$", os.path.realpath(SYSTEM_LIBNCCL)
    )
    return tuple(int(g) for g in match.groups()) if match else None


def torch_nccl_version() -> Optional[Tuple[int, ...]]:
    """The version of the NCCL torch loads."""
    try:
        import torch

        version = torch.cuda.nccl.version()
    except Exception:  # noqa: BLE001
        return None
    if isinstance(version, int):  # older torch: 22809 for 2.28.9
        return version // 10000, version // 100 % 100, version % 100
    return tuple(version)


def nccl_runtime_env() -> Dict:
    """The job's ``runtime_env`` for NCCL RAS: arm the PyTorch Flight Recorder,
    and preload the system NCCL when it's newer than torch's. ``torch<=2.10``
    bundles NCCL 2.27.5, before ``ncclras -f json``; NCCL 2.x is ABI-stable, so
    a newer library under torch works, and an older one may not."""
    os.environ.setdefault("TORCH_FR_BUFFER_SIZE", "2000")
    env_vars = {"TORCH_FR_BUFFER_SIZE": os.environ["TORCH_FR_BUFFER_SIZE"]}
    system, bundled = system_nccl_version(), torch_nccl_version()
    if system and bundled and system > bundled:
        preload = [p for p in (os.environ.get("LD_PRELOAD"), SYSTEM_LIBNCCL) if p]
        env_vars["LD_PRELOAD"] = " ".join(preload)
    return {"env_vars": env_vars}


# ----------------------------------------------------------------------
# Events
# ----------------------------------------------------------------------
@ray.remote(num_cpus=0)
class _Events:
    def __init__(self):
        self._events: List[Tuple[float, str, str]] = []

    def add(self, t: float, kind: str, detail: str) -> None:
        self._events.append((t, kind, detail))

    def get(self) -> List[Tuple[float, str, str]]:
        return sorted(self._events)

    def has(self, kind: str) -> bool:
        return any(k == kind for _, k, _ in self._events)


def start_events():
    return _Events.options(name=EVENTS_ACTOR, get_if_exists=True).remote()


def record(kind: str, detail: str = "") -> None:
    ray.get_actor(EVENTS_ACTOR).add.remote(time.time(), kind, detail)


def fault_fired() -> bool:
    """Whether a fault already fired in this run, maybe in an earlier attempt."""
    return ray.get(ray.get_actor(EVENTS_ACTOR).has.remote("INJECT"))


class Timeline(ControllerCallback):
    """Records, on the controller, each health decision, each new attempt, and
    each restart or error."""

    def after_health_decision(self, health_decision):
        record(type(health_decision).__name__.upper(), health_decision.reason)

    def after_controller_state_update(self, previous_state, current_state):
        name = type(current_state).__name__
        if name == "RunningState" and type(previous_state).__name__ != name:
            record("RUNNING", "the attempt's training started")
        # A shutdown carries its error on the state it leads to.
        error = getattr(current_state, "training_failed_error", None) or getattr(
            getattr(current_state, "next_state", None), "training_failed_error", None
        )
        if error is not None:
            lines = [
                line
                for line in str(error).strip().splitlines()
                if line.strip() and not line.startswith("Training failed due to")
            ]
            record(name.replace("State", "").upper(), lines[0][:200] if lines else "")


def _fault_time(events) -> Optional[float]:
    return next((t for t, kind, _ in events if kind in ("INJECT", "PAUSE")), None)


def print_timeline(events, title: str) -> None:
    """Print every event, as seconds after the fault first fired."""
    injected = _fault_time(events)
    print(f"\n  {title}")
    for t, kind, detail in events:
        at = f"{t - injected:+7.1f}s" if injected is not None else "        "
        print(f"    {at}  {kind:<12} {detail}")


def seconds_to(events, kinds, prefix: str = "") -> Optional[float]:
    """Seconds from the fault firing to the first event of one of ``kinds``
    whose detail starts with ``prefix``."""
    injected = _fault_time(events)
    hit = next(
        (
            t
            for t, kind, detail in events
            if kind in kinds and detail.startswith(prefix) and t >= (injected or 0)
        ),
        None,
    )
    if injected is None or hit is None:
        return None
    return hit - injected


def list_artifacts(root: str) -> None:
    for dirpath, _, files in sorted(os.walk(root)):
        for f in sorted(files):
            path = os.path.join(dirpath, f)
            print(
                f"      {os.path.relpath(path, root)}  ({os.path.getsize(path):,} bytes)"
            )


def banner(title: str, fault) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)
    print(f"  injected: {fault.describe()}")
    sys.stdout.flush()


# ----------------------------------------------------------------------
# NCCL faults, injected inside the training loop.
# ----------------------------------------------------------------------
@dataclass
class LeaveCollective:
    """A hang: ``rank`` stops calling the collective at ``at_step`` and sleeps.

    The process stays alive, so every peer blocks in the next all-reduce, the
    rank's op count stays one behind, and RAS reports the communicator as
    mismatched with every rank still RUNNING. Nothing raises; without a
    detector the job waits for the collective timeout.
    """

    rank: int
    at_step: int

    def describe(self) -> str:
        return (
            f"rank {self.rank} stops calling the collective at step "
            f"{self.at_step} and sleeps (a silent hang)"
        )

    def maybe_inject(self, rank: int, step: int) -> None:
        if rank == self.rank and step == self.at_step:
            record("INJECT", self.describe())
            time.sleep(3600)


@dataclass
class SlowStep:
    """Not a fault: ``rank`` pauses ``pause_s`` before its collective every
    ``every`` steps, as a checkpoint save or a GC pause would. To RAS it looks
    like the start of a hang until the rank arrives."""

    rank: int
    every: int
    pause_s: float

    def describe(self) -> str:
        return (
            f"rank {self.rank} pauses {self.pause_s:.0f}s before its collective "
            f"every {self.every} steps (a slow step, not a hang)"
        )

    def maybe_inject(self, rank: int, step: int) -> None:
        if rank == self.rank and step > 0 and step % self.every == 0:
            record("PAUSE", f"rank {rank}, step {step}, {self.pause_s:.0f}s")
            time.sleep(self.pause_s)


# ----------------------------------------------------------------------
# GPU health events, mocked: an injector writes one on a node, and the probes
# in gpu_health read it there.
# ----------------------------------------------------------------------
def _write_gpu_event(event: Dict) -> None:
    path = event_path(ray.get_runtime_context().get_node_id())
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".tmp", "w") as f:
        json.dump(event, f)
    os.replace(path + ".tmp", path)


def _clear_gpu_event() -> None:
    path = event_path(ray.get_runtime_context().get_node_id())
    if os.path.exists(path):
        os.remove(path)


def inject_gpu_event(node_id: str, event: Dict) -> None:
    """Make the probes in ``gpu_health`` report ``event`` on ``node_id`` from
    their next poll, until ``clear_gpu_events()``."""
    record("INJECT", f"GPU event on node {node_id[:8]}: {event}")
    ray.get(
        ray.remote(num_cpus=0)(_write_gpu_event)
        .options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False)
        )
        .remote(event)
    )


def clear_gpu_events() -> None:
    """Remove the injected GPU health events from every node."""
    clear = ray.remote(num_cpus=0)(_clear_gpu_event)
    ray.get(
        [
            clear.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node["NodeID"], soft=False
                )
            ).remote()
            for node in ray.nodes()
            if node["Alive"]
        ]
    )
