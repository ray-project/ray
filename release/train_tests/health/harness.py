"""Shared by the GPU scripts: the faults they inject, and a timeline of what
the controller did about them.

The timeline is the comparison. ``Timeline`` is a ``ControllerCallback`` that
records every health decision and every error the controller acts on, with a
timestamp, from either detector. Faults record the moment they fire, so every
event is reported as seconds after injection.
"""
import os
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import ray
from ray.train.v2._internal.execution.callback import ControllerCallback

EVENTS_ACTOR = "health_release_events"

# Shared across nodes on Anyscale, so diagnostics written on any node can be
# listed from the driver.
SHARED_STORAGE = "/mnt/cluster_storage"


def ship_by_value(*modules) -> None:
    """The controller and workers cannot import this directory; send by value."""
    for module in modules:
        ray.cloudpickle.register_pickle_by_value(module)


def storage_path() -> Optional[str]:
    return SHARED_STORAGE if os.path.isdir(SHARED_STORAGE) else None


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


def start_events():
    return _Events.options(name=EVENTS_ACTOR).remote()


def record(kind: str, detail: str = "") -> None:
    ray.get_actor(EVENTS_ACTOR).add.remote(time.time(), kind, detail)


def _message(error) -> str:
    text = getattr(error, "_error_message", None) or str(error)
    first = next((line for line in text.splitlines() if line.strip()), "")
    return f"{type(error).__name__}: {first[:240]}"


class Timeline(ControllerCallback):
    """Records, on the controller, what it decided and when."""

    def after_health_decision(self, health_decision):
        record(health_decision.action.name, health_decision.reason)

    def after_controller_state_update(self, previous_state, current_state):
        name = type(current_state).__name__
        if name == "RunningState" and type(previous_state).__name__ != name:
            record("RUNNING", "worker group started")
        error = getattr(current_state, "training_failed_error", None)
        if error is None:
            error = getattr(
                getattr(current_state, "next_state", None),
                "training_failed_error",
                None,
            )
        if error is not None and name in ("RestartingState", "ShuttingDownState"):
            record(name.replace("State", "").upper(), _message(error))


def _fault_time(events) -> Optional[float]:
    return next((t for t, kind, _ in events if kind in ("INJECT", "PAUSE")), None)


def print_timeline(events, title: str) -> None:
    """Every event, as seconds after the fault first fired."""
    injected = _fault_time(events)
    print(f"\n  {title}")
    for t, kind, detail in events:
        at = f"{t - injected:+7.1f}s" if injected is not None else "        "
        print(f"    {at}  {kind:<12} {detail}")


def seconds_to(events, kinds) -> Optional[float]:
    """Seconds from the fault firing to the first event of one of ``kinds``."""
    injected = _fault_time(events)
    hit = next((t for t, kind, _ in events if kind in kinds), None)
    if injected is None or hit is None:
        return None
    return hit - injected


def list_artifacts(experiment_dir: str, subdirs) -> None:
    for sub in subdirs:
        root = os.path.join(experiment_dir, sub)
        if not os.path.isdir(root):
            continue
        for dirpath, _, files in sorted(os.walk(root)):
            for f in sorted(files):
                path = os.path.join(dirpath, f)
                rel = os.path.relpath(path, experiment_dir)
                print(f"      {rel}  ({os.path.getsize(path):,} bytes)")


# ----------------------------------------------------------------------
# Faults. Each one says what it does, so the output explains itself.
# ----------------------------------------------------------------------
@dataclass
class LeaveCollective:
    """A hang: ``rank`` stops calling the collective at ``at_step`` and sleeps.

    The process stays alive, so every peer blocks inside the next all-reduce,
    the rank's op count stays one behind, and RAS reports the communicator as
    MISMATCH with every rank still RUNNING. Nothing raises; without a detector
    the job waits for the collective timeout.
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
    ``every`` steps, as a checkpoint save or a GC pause would.

    To RAS this looks exactly like the start of a hang: its peers wait inside
    the all-reduce, mismatched and not advancing, until the rank arrives.
    """

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


def banner(title: str, fault) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)
    print(f"  injected: {fault.describe()}")
    sys.stdout.flush()
