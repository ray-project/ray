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
        "--yaml-sub-dir",
        type=str,
        default="",
        help="Sub directory under yaml_files/ to look for test files.",
    )
    parser.add_argument(
        "--framework",
        type=str,
        default="tf",
        help="The framework (tf|tf2|torch) to use.",
    )
    args = parser.parse_args()

    assert args.yaml_sub_dir, "--yaml-sub-dir can't be empty."

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
        framework=args.framework,
    )

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/learning_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    if len(results["not_passed"]) > 0:
        raise ValueError(
            "Not all learning tests successfully learned the tasks.\n"
            f"Results=\n{results}"
        )
    else:
        print("Ok.")
