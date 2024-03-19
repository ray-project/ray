"""Learning regression tests on the new API stack for RLlib."""

import json
import os
from pathlib import Path

from ray.rllib.utils.test_utils import run_learning_tests_from_yaml_or_py


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
        "--config-dir",
        type=str,
        default="",
        help="Sub directory under yaml_files/ to look for .py config files.",
    )
    parser.add_argument(
        "--framework",
        type=str,
        default="tf",
        help="The framework (tf|tf2|torch) to use.",
    )
    args = parser.parse_args()

    assert args.config_dir, "--config-dir can't be empty."

    # Get path of this very script to look for yaml files.
    abs_config_path = os.path.join(
        str(Path(__file__).parent), "yaml_files", args.config_dir
    )
    print("abs_config_path={}".format(abs_config_path))

    py_files = Path(abs_config_path).rglob("*.py")
    py_files = sorted(map(lambda path: str(path.absolute()), py_files), reverse=True)

    # Run all tests in the found yaml files.
    results = run_learning_tests_from_yaml_or_py(
        config_files=py_files,
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
