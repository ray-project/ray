"""Learning regression tests for RLlib (torch and tf).

Runs Atari/PyBullet benchmarks for all major algorithms.
"""

import argparse
import json
import os
from pathlib import Path

from ray.rllib.utils.test_utils import run_learning_tests_from_yaml

parser = argparse.ArgumentParser()
parser.add_argument(
    "--smoke-test",
    action="store_true",
    default=False,
    help="Finish quickly for training.")
args = parser.parse_args()

if __name__ == "__main__":
    # Get path of this very script to look for yaml files.
    abs_yaml_path = Path(__file__).parent
    print("abs_yaml_path={}".format(abs_yaml_path))

    # This pattern match is kind of hacky. Avoids cluster.yaml to get sucked
    # into this.
    yaml_files = abs_yaml_path.rglob("*test*.yaml")
    yaml_files = sorted(
        map(lambda path: str(path.absolute()), yaml_files), reverse=True)

    # Run all tests in the found yaml files.
    results = run_learning_tests_from_yaml(
        yaml_files=yaml_files,
        smoke_test=args.smoke_test,
    )

    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/rllib_learning_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    print("PASSED.")
