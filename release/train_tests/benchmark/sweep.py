"""Run a benchmark sweep over config axes (e.g. sequence length, batch size).

For a small model the interesting surface is seq_len x micro_batch_size, not
sharding strategy. Each cell is one run with a unique name, so it writes its own
<name>_results.json and `collect.py` renders the whole grid.

Usage:
    # 4 seq lengths x 3 batch sizes = 12 runs (OOM cells are skipped)
    python sweep.py --experiment experiments/qwen3_06b_deepspeed.yaml \
        --axis data.seq_len=1024,2048,4096,8192 \
        --axis data.micro_batch_size=1,2,4

    # preview the matrix without running
    python sweep.py --experiment <exp>.yaml --axis data.seq_len=1024,2048 --dry-run
"""

import argparse
import itertools
import logging
import os
import sys
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.experiment_config import load_experiment  # noqa: E402
from core.runner import run_experiment, write_results  # noqa: E402

logger = logging.getLogger(__name__)


def expand_axes(axes: Dict[str, List[str]]) -> List[Dict[str, str]]:
    """Cartesian product of axes into a list of {key: value} override sets."""
    keys = list(axes)
    return [dict(zip(keys, combo)) for combo in itertools.product(*axes.values())]


def cell_name(base_name: str, combo: Dict[str, str]) -> str:
    """Unique, filesystem-safe run name encoding this cell's axis values."""
    suffix = "_".join(f"{key.split('.')[-1]}{value}" for key, value in combo.items())
    return f"{base_name}__{suffix}" if suffix else base_name


def _parse_axis(arg: str) -> tuple:
    if "=" not in arg:
        raise argparse.ArgumentTypeError(f"--axis must be key=v1,v2,...; got {arg}")
    key, values = arg.split("=", 1)
    return key, [v for v in values.split(",") if v != ""]


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True)
    parser.add_argument(
        "--axis",
        action="append",
        default=[],
        type=_parse_axis,
        help="Sweep axis as dotted.key=v1,v2,... (repeatable).",
    )
    parser.add_argument(
        "--launcher", default=None, help="Override launcher for all cells."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print the matrix, don't run."
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        default=True,
        help="Keep going if a cell fails (e.g. OOM). On by default.",
    )
    args = parser.parse_args()

    axes = dict(args.axis)
    base = load_experiment(args.experiment)
    cells = expand_axes(axes) if axes else [{}]

    logger.info(f"Sweep '{base.name}': {len(cells)} cells over axes {dict(axes)}")
    for combo in cells:
        logger.info(f"  - {cell_name(base.name, combo)}: {combo}")
    if args.dry_run:
        return

    completed, failed = [], []
    for combo in cells:
        name = cell_name(base.name, combo)
        overrides = [f"{k}={v}" for k, v in combo.items()] + [f"name={name}"]
        cfg = load_experiment(args.experiment, overrides=overrides)
        if args.launcher:
            cfg.launcher = args.launcher

        logger.info(f"=== Running {name} ({combo}) ===")
        try:
            metrics = run_experiment(cfg)
            if metrics:
                write_results(metrics, name)
            completed.append(name)
        except Exception as e:  # noqa: BLE001 - one bad cell shouldn't kill the grid
            logger.error(f"Cell {name} failed: {type(e).__name__}: {e}")
            failed.append(name)
            if not args.continue_on_error:
                raise

    logger.info(f"Sweep done: {len(completed)} ok, {len(failed)} failed.")
    if failed:
        logger.info(f"Failed cells (often OOM): {failed}")
    logger.info("Render with: python collect.py")


if __name__ == "__main__":
    main()
