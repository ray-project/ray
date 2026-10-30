"""Memory release test: APPO on Footsies, multi-node, high throughput.

Trains APPO on the Footsies fighting game for a fixed time and checks that the memory
of the Learners, AggregatorActors and EnvRunners doesn't grow. The rewards don't
matter: the main policy plays against itself (both players use the same RLModule), so
no RLModules are added or removed during the run.

Every training iteration, a probe actor on each node reads the USS of every Learner,
AggregatorActor and EnvRunner process. After a warmup, the script fits a line to each
process's USS over time and fails if any slope exceeds its category's limit. All
slopes are also reported as perf metrics, so slow growth shows up across nightlies.

Run on a cluster with 2 GPUs (one per Learner) and enough CPUs for the EnvRunners,
e.g. `multi_node_2x1gpu_2x16cpus.yaml`:
    python footsies_appo_memory.py
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psutil

import ray
from ray import tune
from ray._private.test_utils import safe_write_to_results_json
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.core.rl_module import MultiRLModuleSpec, RLModuleSpec
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent.footsies.footsies_env import (
    env_creator,
)
from ray.rllib.examples.envs.classes.multi_agent.footsies.utils import (
    platform_for_binary_to_download,
)
from ray.rllib.examples.rl_modules.classes.lstm_containing_rlm import (
    LSTMContainingRLModuleWithTargetNetwork,
)
from ray.tune.registry import register_env
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

parser = argparse.ArgumentParser()
parser.add_argument("--num-env-runners", type=int, default=24)
parser.add_argument("--num-learners", type=int, default=2)
parser.add_argument("--num-gpus-per-learner", type=float, default=1)
parser.add_argument("--num-aggregator-actors-per-learner", type=int, default=2)
parser.add_argument("--duration-s", type=float, default=600)
parser.add_argument("--warmup-s", type=float, default=180)
# Limits on each process's USS growth after the warmup, in MiB per hour. Healthy runs
# measure at most ~4 (Learners), ~26 (AggregatorActors) and ~14 (EnvRunners).
parser.add_argument("--max-learner-growth", type=float, default=50)
parser.add_argument("--max-aggregator-growth", type=float, default=100)
parser.add_argument("--max-env-runner-growth", type=float, default=50)
args = parser.parse_args()

MAIN = "main"
CATEGORIES = ("learner", "aggregator", "env_runner")


@ray.remote(num_cpus=0)
class _NodeProbe:
    """Reads the USS of the given processes on this node."""

    def uss_mib(self, pids):
        out = {}
        for pid in pids:
            try:
                out[pid] = psutil.Process(pid).memory_full_info().uss / 2**20
            except psutil.Error:
                out[pid] = None  # The process is gone (e.g. restarted).
        return out


def _location(*_):
    return ray.get_runtime_context().get_node_id(), os.getpid()


class _MemoryProbe(RLlibCallback):
    """Adds the USS of every Learner, AggregatorActor and EnvRunner to the result."""

    def on_train_result(self, *, algorithm, result, **kwargs):
        if not hasattr(self, "_processes"):
            aggregators = algorithm._aggregator_actor_manager
            self._processes = {
                "learner": [
                    r.get() for r in algorithm.learner_group.foreach_learner(_location)
                ],
                "aggregator": (
                    [r.get() for r in aggregators.foreach_actor(_location)]
                    if aggregators
                    else []
                ),
                "env_runner": algorithm.env_runner_group.foreach_env_runner(
                    _location, local_env_runner=False
                ),
            }
            nodes = {n for procs in self._processes.values() for n, _ in procs}
            self._probes = {
                n: _NodeProbe.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(n, soft=False)
                ).remote()
                for n in nodes
            }
        pids_by_node = {}
        for procs in self._processes.values():
            for node, pid in procs:
                pids_by_node.setdefault(node, []).append(pid)
        nodes = list(pids_by_node)
        readings = ray.get(
            [self._probes[n].uss_mib.remote(pids_by_node[n]) for n in nodes]
        )
        uss = dict(zip(nodes, readings))
        result["memory_probe"] = {
            cat: {f"{node[:8]}:{pid}": uss[node][pid] for node, pid in procs}
            for cat, procs in self._processes.items()
        }


class _Collector(tune.Callback):
    """Keeps the memory readings and counters of every result (driver side)."""

    def __init__(self):
        self.rows = []

    def on_trial_result(self, iteration, trials, trial, result, **info):
        learners = result.get("learners", {}).get("__all_modules__", {})
        self.rows.append(
            {
                "t": result["time_total_s"],
                "memory": result.get("memory_probe", {}),
                "env_steps_sampled": result.get("env_runners", {}).get(
                    "num_env_steps_sampled_lifetime", 0
                ),
                "module_steps_trained": learners.get(
                    "num_module_steps_trained_lifetime", 0
                ),
            }
        )


def _slope_per_hour(points):
    """Least-squares slope of (seconds, MiB) points, in MiB per hour."""
    n = len(points)
    mt = sum(t for t, _ in points) / n
    mv = sum(v for _, v in points) / n
    den = sum((t - mt) ** 2 for t, _ in points)
    return sum((t - mt) * (v - mv) for t, v in points) / den * 3600 if den else 0.0


register_env("FootsiesEnv", env_creator)
binary_dir = Path("/tmp/ray/binaries/footsies")
config = (
    APPOConfig()
    .environment(
        "FootsiesEnv",
        env_config={
            "max_t": 1000,
            "frame_skip": 4,
            "observation_delay": 16,
            "train_start_port": 45001,
            "eval_start_port": 55001,
            "host": "localhost",
            "binary_download_dir": binary_dir,
            "binary_extract_dir": binary_dir,
            "binary_to_download": platform_for_binary_to_download(render=False),
            "log_unity_output": False,
        },
    )
    .env_runners(
        env_runner_cls=MultiAgentEnvRunner,
        num_env_runners=args.num_env_runners,
        num_cpus_per_env_runner=1,
        batch_mode="truncate_episodes",
        rollout_fragment_length=256,
        episodes_to_numpy=True,
        create_env_on_local_worker=False,
    )
    .learners(
        num_learners=args.num_learners,
        num_cpus_per_learner=1,
        num_gpus_per_learner=args.num_gpus_per_learner,
        num_aggregator_actors_per_learner=args.num_aggregator_actors_per_learner,
    )
    .training(
        train_batch_size_per_learner=2048,
        minibatch_size=256,
        lr=5e-4,
        entropy_coeff=0.01,
        target_network_update_freq=0.5,
    )
    .multi_agent(
        policies={MAIN},
        # Self-play: both players use (and train) the same RLModule.
        policy_mapping_fn=lambda agent_id, episode, **kwargs: MAIN,
        policies_to_train=[MAIN],
    )
    .rl_module(
        rl_module_spec=MultiRLModuleSpec(
            rl_module_specs={
                MAIN: RLModuleSpec(
                    module_class=LSTMContainingRLModuleWithTargetNetwork,
                    model_config={
                        "lstm_cell_size": 128,
                        "dense_layers": [128, 128],
                        "max_seq_len": 64,
                    },
                )
            }
        )
    )
    .reporting(min_time_s_per_iteration=10)
    .callbacks(_MemoryProbe)
)

if __name__ == "__main__":
    start = time.time()
    collector = _Collector()
    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=tune.RunConfig(
            stop={"time_total_s": args.duration_s},
            callbacks=[collector],
            verbose=1,
            # The trainable may run on any node, so Tune needs storage that all nodes
            # can reach.
            storage_path=(
                "/mnt/cluster_storage"
                if os.path.isdir("/mnt/cluster_storage")
                else None
            ),
        ),
    )
    results = tuner.fit()
    if results.errors:
        raise RuntimeError(f"Training failed: {results.errors}")

    rows = [r for r in collector.rows if r["memory"] and r["t"] >= args.warmup_s]
    if len(rows) < 5:
        raise RuntimeError(f"Only {len(rows)} memory readings after the warmup.")

    limits = {
        "learner": args.max_learner_growth,
        "aggregator": args.max_aggregator_growth,
        "env_runner": args.max_env_runner_growth,
    }
    slopes = {cat: {} for cat in CATEGORIES}
    for cat in CATEGORIES:
        for proc in rows[-1]["memory"].get(cat, {}):
            points = [
                (r["t"], r["memory"][cat][proc])
                for r in rows
                if r["memory"].get(cat, {}).get(proc) is not None
            ]
            if len(points) >= 5:
                slopes[cat][proc] = _slope_per_hour(points)

    too_fast = [
        f"{cat} {proc}: {slope:+.1f} MiB/h (limit {limits[cat]:.0f})"
        for cat in CATEGORIES
        for proc, slope in slopes[cat].items()
        if slope > limits[cat]
    ]
    missing = [cat for cat in CATEGORIES if not slopes[cat]]
    window_h = (rows[-1]["t"] - rows[0]["t"]) / 3600
    module_steps = rows[-1]["module_steps_trained"] - rows[0]["module_steps_trained"]
    env_steps = rows[-1]["env_steps_sampled"] - rows[0]["env_steps_sampled"]
    passed = not too_fast and not missing

    perf_metrics = []
    summary = {}
    for cat in CATEGORIES:
        values = sorted(slopes[cat].values())
        if not values:
            continue
        summary[cat] = {
            "num_processes": len(values),
            "max_growth_mib_per_h": max(values),
            "median_growth_mib_per_h": values[len(values) // 2],
            "growth_mib_per_h": dict(sorted(slopes[cat].items())),
            "uss_mib_at_end": {
                p: rows[-1]["memory"][cat][p] for p in sorted(slopes[cat])
            },
        }
        for stat in ("max", "median"):
            perf_metrics.append(
                {
                    "perf_metric_name": f"footsies_appo_{cat}_{stat}_uss_growth_mib_per_h",
                    "perf_metric_value": summary[cat][f"{stat}_growth_mib_per_h"],
                    "perf_metric_type": "MEMORY",
                }
            )
    perf_metrics.append(
        {
            "perf_metric_name": "footsies_appo_env_steps_sampled_per_s",
            "perf_metric_value": env_steps / (window_h * 3600),
            "perf_metric_type": "THROUGHPUT",
        }
    )
    perf_metrics.append(
        {
            "perf_metric_name": "footsies_appo_module_steps_trained_per_s",
            "perf_metric_value": module_steps / (window_h * 3600),
            "perf_metric_type": "THROUGHPUT",
        }
    )
    out = {
        "time_taken": time.time() - start,
        "passed": passed,
        "window_s": window_h * 3600,
        "num_readings": len(rows),
        "memory": summary,
        "too_fast": too_fast,
        "missing_categories": missing,
        "perf_metrics": perf_metrics,
    }
    print(json.dumps(out, indent=2))
    safe_write_to_results_json(out)
    if not passed:
        sys.exit(
            "Memory grows too fast or wasn't measured: "
            + "; ".join(too_fast + [f"no {cat} readings" for cat in missing])
        )
