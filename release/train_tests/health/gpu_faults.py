#!/usr/bin/env python
"""GPU faults, mocked as GPU health events, through ``ray.train.health``.

    python gpu_faults.py                  # every scenario, on the running cluster
    python gpu_faults.py workflow         # one of them
    python gpu_faults.py --local workflow # a local CPU cluster: 2 nodes, gloo

Scenarios, on 2 nodes A and B:

  workflow  A pre-flight all-reduce over every worker, with training stopped
            (``HealthCheck(stop_workers=True)``), passes and training starts.
            Rank 0 then makes its node A slow and hot. ``GpuSpeedProbe`` sees A
            slow, ``DiagnoseASlowNode`` diagnoses with ``GpuTemperatureProbe``,
            which finds A too hot, and ``ReattemptOnAHotNode`` reattempts. With ``max_failures=0`` the run ends with
            ``HealthCheckError``.
  xid79     The last rank injects XID 79 (GPU fallen off the bus) on its node
            B. ``EvictOnFatalGpuEvent`` evicts B, outside the retry budget,
            and training restarts. Nothing drains B, so the restart lands on it
            again; B is evicted only once, so training then finishes.

Pass: each scenario's decisions and ending match.
"""
import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import ray

sys.path.insert(0, str(Path(__file__).parent))
import gpu_health  # noqa: E402
import harness  # noqa: E402

harness.ship_by_value(harness, gpu_health)


@dataclass
class GpuEventFault:
    """``rank`` (``-1`` for the last) injects ``event`` on its own node at
    ``at_step``, once per run: a restarted attempt doesn't inject it again."""

    rank: int
    at_step: int
    event: Dict

    def describe(self) -> str:
        rank = "the last rank" if self.rank == -1 else f"rank {self.rank}"
        return f"{rank} injects {self.event} on its node at step {self.at_step}"

    def maybe_inject(self, rank: int, world_size: int, step: int) -> None:
        if rank == self.rank % world_size and step == self.at_step:
            if not harness.fault_fired():
                node_id = ray.get_runtime_context().get_node_id()
                harness.inject_gpu_event(node_id, self.event)


SCENARIOS = {
    "workflow": dict(
        fault=GpuEventFault(
            rank=0,
            at_step=20,
            event={
                "checkName": "GpuThermalWatch",
                "isFatal": False,
                "isHealthy": False,
                "entitiesImpacted": [{"entityType": "GPU", "entityValue": "0"}],
                "message": "GPU 0 is throttling",
                "recommendedAction": "NONE",
                # What the mocked GpuSpeedProbe and GpuTemperatureProbe read.
                "relative_speed": 0.5,
                "temp_c": 92.0,
            },
        ),
        preflight=lambda: [gpu_health.all_reduce_preflight_check()],
        inflight=lambda: [gpu_health.slow_node_check()],
        decisions=["DIAGNOSE", "REATTEMPT"],
        error="HealthCheckError",
    ),
    "xid79": dict(
        fault=GpuEventFault(
            rank=-1,
            at_step=20,
            event=gpu_health.xid_event(79, gpu=3),
        ),
        preflight=lambda: [],
        inflight=lambda: [gpu_health.gpu_health_event_check()],
        decisions=["EVICT"],
        error=None,
    ),
}


def make_train_func(fault: GpuEventFault, steps: int):
    def train_func():
        import time

        import torch
        import torch.distributed as dist

        import ray.train

        context = ray.train.get_context()
        rank, world_size = context.get_world_rank(), context.get_world_size()
        device = "cuda" if torch.cuda.is_available() else "cpu"
        tensor = torch.ones(256, 256, device=device)
        for step in range(steps):
            fault.maybe_inject(rank, world_size, step)
            dist.all_reduce(tensor)
            time.sleep(0.1)

    return train_func


def run(name: str, workers: int, use_gpu: bool) -> bool:
    from ray.train import FailureConfig, RunConfig, ScalingConfig
    from ray.train.health import HealthConfig
    from ray.train.torch import TorchTrainer
    from ray.train.v2.api.exceptions import TrainingFailedError

    scenario = SCENARIOS[name]
    harness.banner(name, scenario["fault"])
    harness.clear_gpu_events()
    events = harness.start_events()
    trainer = TorchTrainer(
        make_train_func(scenario["fault"], steps=300),
        scaling_config=ScalingConfig(num_workers=workers, use_gpu=use_gpu),
        run_config=RunConfig(
            name=f"gpu_{name}_{int(time.time())}",
            storage_path=harness.storage_path(),
            health_config=HealthConfig(
                preflight_checks=scenario["preflight"](),
                inflight_checks=scenario["inflight"](),
            ),
            callbacks=[harness.Timeline()],
            failure_config=FailureConfig(max_failures=0),
        ),
    )
    error = None
    try:
        trainer.fit()
    except TrainingFailedError as e:
        error = e
    timeline = ray.get(events.get.remote())
    ray.kill(events)
    harness.clear_gpu_events()

    harness.print_timeline(timeline, "timeline (seconds after the fault fired):")
    decisions = [k for _, k, _ in timeline if k in ("DIAGNOSE", "REATTEMPT", "EVICT")]
    ended = type(error).__name__ if error else None
    ok = decisions == scenario["decisions"] and ended == scenario["error"]
    print(f"\n  decisions {decisions}, ended with {ended}: {'PASS' if ok else 'FAIL'}")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenarios", nargs="*", help=f"some of {list(SCENARIOS)}")
    parser.add_argument(
        "--local", action="store_true", help="a local 2-node CPU cluster"
    )
    parser.add_argument(
        "--workers", type=int, default=int(os.environ.get("HEALTH_TEST_WORKERS", "8"))
    )
    args = parser.parse_args()
    unknown = set(args.scenarios) - set(SCENARIOS)
    if unknown:
        parser.error(f"unknown scenarios {sorted(unknown)}")

    if args.local:
        from ray.cluster_utils import Cluster

        cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 0})
        for _ in range(2):
            cluster.add_node(num_cpus=2)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        workers, use_gpu = 4, False
    else:
        ray.init(address="auto")
        workers, use_gpu = args.workers, True

    results = {
        name: run(name, workers, use_gpu) for name in args.scenarios or SCENARIOS
    }
    print("\n" + "=" * 78)
    for name, ok in results.items():
        print(f"  {name:<10} {'PASS' if ok else 'FAIL'}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
