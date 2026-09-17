"""Benchmark entrypoint: load an experiment YAML, dispatch to a launcher.

Usage:
    # Ray Train (default launcher) — single submission from the head node;
    # Ray schedules the workers across the cluster's GPU nodes.
    python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml

    # torch.distributed parity baseline (Ray actors as the launcher)
    python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml \
        --set launcher=ray_torch_distributed

    # Override any config field inline
    python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml \
        --set training.num_steps=20 data.dataset=synthetic
"""

import argparse
import json
import logging
import os
import pprint
from typing import Any, Dict

import ray

# Run on the harness root so `core`, `frameworks`, `data` import cleanly.
import sys

HARNESS_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HARNESS_ROOT)

from core.experiment_config import ExperimentConfig, load_experiment  # noqa: E402

logger = logging.getLogger(__name__)

# Shared cluster storage (visible to all nodes on the cluster).
RESULTS_DIR = "/mnt/cluster_storage"


def write_results(metrics: Dict[str, Any], experiment_name: str) -> None:
    """Persist final metrics: the release-test JSON (reuses Ray's
    ``safe_write_to_results_json``) plus a per-experiment file that
    ``collect.py`` aggregates into the comparison table.
    """
    payload = {"experiment": experiment_name, **metrics}
    try:
        from ray._private.test_utils import safe_write_to_results_json

        safe_write_to_results_json(payload)
    except Exception as e:  # local runs without the release-test harness
        logger.warning(f"safe_write_to_results_json unavailable ({e}).")

    path = os.path.join(RESULTS_DIR, f"{experiment_name}_results.json")
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    logger.info(f"Wrote results to {path}")


def run_experiment(cfg: ExperimentConfig) -> Dict[str, Any]:
    """Dispatch an experiment to its launcher and return final metrics."""
    # Register the harness as a Ray job-level working_dir so it is uploaded
    # once and inherited by ALL workers (which may be on other nodes).
    if not ray.is_initialized():
        ray.init(runtime_env={"working_dir": HARNESS_ROOT})

    if cfg.launcher == "ray_train":
        from core.launchers.ray_launcher import run_with_ray

        return run_with_ray(cfg)
    elif cfg.launcher == "ray_torch_distributed":
        # The torch.distributed parity baseline: vanilla init_process_group
        # ("env://") with Ray actors as the launcher (placement + rank/master
        # env vars). This is exactly how the legacy air_benchmarks ran "vanilla
        # torch" — Ray actors stand up the process group, no ssh/srun needed.
        from core.launchers.ray_torch_distributed_launcher import (
            run_with_torch_distributed,
        )

        return run_with_torch_distributed(cfg)
    raise ValueError(
        f"Unknown launcher: {cfg.launcher}. Use 'ray_train' or 'ray_torch_distributed'."
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True, help="Path to experiment YAML")
    parser.add_argument(
        "--launcher",
        default=None,
        help="Override the launcher from the YAML (ray_train | ray_torch_distributed)",
    )
    parser.add_argument(
        "--set",
        nargs="*",
        default=[],
        dest="overrides",
        help="Inline overrides, e.g. training.num_steps=20 data.dataset=synthetic",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Accepted for release-test compatibility (glue.py appends it to "
        "smoke runs). A no-op: smoke behavior lives in the smoke experiment "
        "YAML that the release smoke_test block already points at.",
    )
    args = parser.parse_args()

    cfg = load_experiment(args.experiment, overrides=args.overrides)
    if args.launcher:
        cfg.launcher = args.launcher

    logger.info("Experiment config:\n" + pprint.pformat(cfg.to_dict()))

    metrics = run_experiment(cfg)

    logger.info(
        "\n"
        + "-" * 80
        + f"\nFinal metrics for {cfg.name}:\n"
        + pprint.pformat(metrics)
        + "\n"
        + "-" * 80
    )

    if not metrics:
        # e.g. the torch.distributed launcher returns {} when no rank reported. Fail
        # loudly: a release test must never pass without benchmark results.
        raise RuntimeError(f"{cfg.name} finished but produced no metrics.")
    write_results(metrics, cfg.name)
    if metrics.get("oom"):
        # The oom=true row is persisted above for debugging, but a scheduled
        # benchmark run that OOMed has no valid throughput/MFU — fail the job
        # rather than letting the release test pass without real numbers.
        raise RuntimeError(f"{cfg.name} hit CUDA OOM; no valid benchmark result.")


if __name__ == "__main__":
    # Ray Train v2 is the default; no RAY_TRAIN_V2_ENABLED needed.
    main()
