"""Pull-vs-push comparison for the training ingestion benchmark.

Runs ``train_benchmark.py`` twice on the same cluster — first with the
default pull-based streaming split, then with the push-based one — and
fails unless push reaches at least ``--push_min_throughput_ratio`` of
pull's ``train/global_throughput``. Both runs' metrics and the ratio are
written as the release test result.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from typing import Dict, List

from ray._private.test_utils import safe_write_to_results_json

logger = logging.getLogger(__name__)

PUSH_FLAG = "--ray_data_push_based_split"
THROUGHPUT_KEY = "train/global_throughput"


def run_benchmark(
    benchmark_args: List[str], push_based: bool, output_json: str
) -> Dict:
    script = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "train_benchmark.py"
    )
    cmd = [sys.executable, script, *benchmark_args]
    if push_based:
        cmd.append(f"{PUSH_FLAG}=True")
    env = dict(os.environ, TEST_OUTPUT_JSON=output_json)
    mode = "push" if push_based else "pull"
    logger.info(f"Running the {mode} benchmark: {' '.join(cmd)}")
    subprocess.run(cmd, env=env, check=True)
    with open(output_json) as f:
        return json.load(f)


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--push_min_throughput_ratio",
        type=float,
        default=0.9,
        help="Fail if push global throughput falls below this fraction of pull's.",
    )
    own_args, benchmark_args = parser.parse_known_args()
    assert not any(
        arg.startswith(PUSH_FLAG) for arg in benchmark_args
    ), f"Don't pass {PUSH_FLAG}; this script runs both modes itself."

    pull_metrics = run_benchmark(
        benchmark_args, push_based=False, output_json="/tmp/pull_result.json"
    )
    push_metrics = run_benchmark(
        benchmark_args, push_based=True, output_json="/tmp/push_result.json"
    )

    pull_throughput = pull_metrics[THROUGHPUT_KEY]
    push_throughput = push_metrics[THROUGHPUT_KEY]
    ratio = push_throughput / pull_throughput

    results = {
        "pull_global_throughput": pull_throughput,
        "push_global_throughput": push_throughput,
        "push_pull_throughput_ratio": round(ratio, 4),
        "pull": pull_metrics,
        "push": push_metrics,
    }
    safe_write_to_results_json(results)
    logger.info(
        f"pull={pull_throughput:.1f} rows/s, push={push_throughput:.1f} rows/s, "
        f"ratio={ratio:.3f} (required >= {own_args.push_min_throughput_ratio})"
    )

    if ratio < own_args.push_min_throughput_ratio:
        raise SystemExit(
            f"Push global throughput ({push_throughput:.1f} rows/s) is below "
            f"{own_args.push_min_throughput_ratio:.0%} of pull's "
            f"({pull_throughput:.1f} rows/s): ratio={ratio:.3f}."
        )


if __name__ == "__main__":
    main()
