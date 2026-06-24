"""Benchmark entrypoint: load an experiment YAML, dispatch to a launcher.

Usage:
    # Ray Train (default launcher) — single submission from the head node;
    # Ray schedules the workers across the cluster's GPU nodes.
    python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml

    # Torchrun parity baseline — launched ON the GPU node(s), one process per
    # GPU. Single node:
    torchrun --standalone --nproc_per_node=8 -m core.runner \
        --experiment experiments/qwen3_06b_deepspeed.yaml --launcher torchrun

    # Override any config field inline
    python -m core.runner --experiment experiments/qwen3_06b_deepspeed.yaml \
        --set training.num_steps=20 data.dataset=synthetic
"""

import argparse
import logging
import os
import pprint
from typing import Any, Dict

# Run on the harness root so `core`, `frameworks`, `data` import cleanly under
# both `python -m core.runner` and torchrun.
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.experiment_config import ExperimentConfig, load_experiment  # noqa: E402
from core.sinks import write_results  # noqa: E402

logger = logging.getLogger(__name__)


def run_experiment(cfg: ExperimentConfig) -> Dict[str, Any]:
    """Dispatch an experiment to its launcher and return final metrics."""
    if cfg.launcher == "ray":
        from core.launchers.ray_launcher import run_with_ray

        return run_with_ray(cfg)
    elif cfg.launcher == "torchrun":
        from core.launchers.torchrun_launcher import run_with_torchrun

        return run_with_torchrun(cfg)
    elif cfg.launcher == "torchrun_ray":
        # torch.distributed (env:// rendezvous) placed by Ray actors — the
        # torchrun parity baseline without ssh-ing into GPU nodes.
        from core.launchers.torchrun_ray_launcher import run_with_torchrun_ray

        return run_with_torchrun_ray(cfg)
    raise ValueError(f"Unknown launcher: {cfg.launcher}")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True, help="Path to experiment YAML")
    parser.add_argument(
        "--launcher",
        default=None,
        help="Override the launcher from the YAML (ray | torchrun)",
    )
    parser.add_argument(
        "--set",
        nargs="*",
        default=[],
        dest="overrides",
        help="Inline overrides, e.g. training.num_steps=20 data.dataset=synthetic",
    )
    parser.add_argument(
        "--prepare-data",
        action="store_true",
        help="Prefetch the model + dataset into a shared HF cache, then exit. "
        "Run once before the distributed run so workers hit a warm cache.",
    )
    args = parser.parse_args()

    cfg = load_experiment(args.experiment, overrides=args.overrides)
    if args.launcher:
        cfg.launcher = args.launcher

    logger.info("Experiment config:\n" + pprint.pformat(cfg.to_dict()))

    if args.prepare_data:
        from core.prepare import prepare_experiment

        prepare_experiment(cfg)
        logger.info("Prepare complete. Re-run without --prepare-data to train.")
        return

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
