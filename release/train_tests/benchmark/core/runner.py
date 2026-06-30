"""Benchmark entrypoint: load an experiment YAML, dispatch to a launcher.

Usage:
    # Ray Train (default launcher) — single submission from the head node;
    # Ray schedules the workers across the cluster's GPU nodes.
    python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml

    # torch.distributed parity baseline (Ray actors as the launcher)
    python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml \
        --set launcher=torchrun_ray

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

# Run on the harness root so `core`, `frameworks`, `data` import cleanly under
# both `python -m core.runner` and torchrun.
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.experiment_config import ExperimentConfig, load_experiment  # noqa: E402
from core.train_context import shared_storage_root  # noqa: E402

logger = logging.getLogger(__name__)


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

    path = os.path.join(shared_storage_root(), f"{experiment_name}_results.json")
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    logger.info(f"Wrote results to {path}")


def run_experiment(cfg: ExperimentConfig) -> Dict[str, Any]:
    """Dispatch an experiment to its launcher and return final metrics."""
    if cfg.launcher == "ray_train":
        from core.launchers.ray_launcher import run_with_ray

        return run_with_ray(cfg)
    elif cfg.launcher == "torchrun_ray":
        # The torch.distributed parity baseline: vanilla init_process_group
        # ("env://") with Ray actors as the launcher (placement + rank/master
        # env vars). This is exactly how the legacy air_benchmarks ran "vanilla
        # torch" — Ray actors stand up the process group, no ssh/srun needed.
        from core.launchers.torchrun_ray_launcher import run_with_torchrun_ray

        return run_with_torchrun_ray(cfg)
    raise ValueError(
        f"Unknown launcher: {cfg.launcher}. Use 'ray_train' or 'torchrun_ray'."
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True, help="Path to experiment YAML")
    parser.add_argument(
        "--launcher",
        default=None,
        help="Override the launcher from the YAML (ray_train | torchrun_ray)",
    )
    parser.add_argument(
        "--set",
        nargs="*",
        default=[],
        dest="overrides",
        help="Inline overrides, e.g. training.num_steps=20 data.dataset=synthetic",
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

    # Under torchrun, only rank 0 returns metrics; non-zero ranks skip writing.
    if metrics:
        write_results(metrics, cfg.name)


if __name__ == "__main__":
    # Ray Train v2 is the default; no RAY_TRAIN_V2_ENABLED needed.
    main()
