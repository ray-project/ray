"""Core Learning regression tests for RLlib (torch and tf).

Runs Atari/PyBullet benchmarks for the most popular algorithms.
"""

import json
import os
from pathlib import Path

from ray.rllib.utils.test_utils import run_learning_tests_from_yaml

if __name__ == "__main__":
    # Get path of this very script to look for yaml files.
    abs_yaml_path = Path(__file__).parent
    print("abs_yaml_path={}".format(abs_yaml_path))

    yaml_files = abs_yaml_path.rglob("*.yaml")
    yaml_files = sorted(
        map(lambda path: str(path.absolute()), yaml_files), reverse=True
    )

    # Run all tests in the found yaml files.
    results = run_learning_tests_from_yaml(yaml_files=yaml_files)

    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/rllib_learning_test_core.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    print("Ok.")
