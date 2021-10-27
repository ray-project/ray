"""Connect tests for Tune & RLlib.

Runs a couple of hard learning tests using Anyscale connect.
"""

import json
import os

from ray.rllib.examples.tune_framework import run

if __name__ == "__main__":
    results = run()

    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/rllib_connect_tests.json")
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    print("Ok.")
