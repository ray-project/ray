"""Learning regression tests for RLlib (torch and tf).

Runs Atari/PyBullet benchmarks for all major algorithms.
"""

import json
import os
from pathlib import Path

from ray.rllib.utils.test_utils import run_learning_tests_from_yaml

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for training.",
    )
    parser.add_argument(
        "--yaml",
        type=str,
        default="",
        help="Pattern for yaml files to match within the yaml_files/ dir. E.g. 'a-e/*' "
             "matches all yaml files inside yaml_files/a-e/ and 'a-e/a2c*' matches "
             "all A2C-related tests.",
    )
    args = parser.parse_args()

    assert args.yaml, "--yaml can't be empty."

    # Get path of this very script to look for yaml files.
    abs_yaml_path = os.path.join(
        str(Path(__file__).parent), "yaml_files", args.yaml_sub_dir
    )
    print("abs_yaml_path={}".format(abs_yaml_path))

    yaml_files = Path(abs_yaml_path).rglob("*.yaml")
    yaml_files = sorted(
        map(lambda path: str(path.absolute()), yaml_files), reverse=True
    )

    # Run all tests in the found yaml files.
    results = run_learning_tests_from_yaml(
        yaml_files=yaml_files,
        # Note(jungong) : run learning tests to full desired duration
        # for performance regression purpose.
        # Talk to jungong@ if you have questions about why we do this.
        use_pass_criteria_as_stop=False,
        smoke_test=args.smoke_test,
    )

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/learning_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    # Check, if any tests did not succeed and if yes, fail here.
    if not all(results["passed"]):
        print("Not all tests ok -> Failing with error")
        raise ValueError("Not all tests reached the passing criteria!")
    else:
        print("Ok.")
