"""Benchmark entrypoint: load an experiment YAML, dispatch to a launcher.

Usage:
    # Ray Train (default launcher)
    RAY_TRAIN_V2_ENABLED=1 python -m core.runner \
        --experiment experiments/qwen3_06b_deepspeed.yaml

    # Torchrun parity baseline
    torchrun --nproc_per_node=2 -m core.runner \
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
    args = parser.parse_args()

    cfg = load_experiment(args.experiment, overrides=args.overrides)
    if args.launcher:
        cfg.launcher = args.launcher

    logger.info("Experiment config:\n" + pprint.pformat(cfg.to_dict()))

    metrics = run_experiment(cfg)

    logger.info(
        "\n" + "-" * 80 + f"\nFinal metrics for {cfg.name}:\n"
        + pprint.pformat(metrics) + "\n" + "-" * 80
    )

    # Under torchrun, only rank 0 returns metrics; non-zero ranks skip writing.
    if metrics:
        write_results(metrics, cfg.name)


if __name__ == "__main__":
    if os.environ.get("RAY_TRAIN_V2_ENABLED") is None:
        os.environ["RAY_TRAIN_V2_ENABLED"] = "1"
    main()
