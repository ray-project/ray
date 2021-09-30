"""Runtime env test on Ray Client

This test installs runtime environments on a remote cluster using local
pip requirements.txt files.  It is intended to be run using Anyscale connect.
This complements existing per-commit tests in CI, for which we don't have
access to a physical remote cluster.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import argparse
import json
import os
import time
from pathlib import Path

import ray
from ray._private.test_utils import is_anyscale_connect


def test_pip_requirements_files():
    pip_file_18 = Path("/tmp/runtime_env_pip_18.txt")
    pip_file_18.write_text("requests==2.18.0")

    @ray.remote
    def get_version():
        import requests
        return requests.__version__

    env_18 = {"pip": str(pip_file_18)}
    assert ray.get(
        get_version.options(runtime_env=env_18).remote() == "2.18.0")

    @ray.remote
    class VersionActor:
        def get_version(self):
            import requests
            return requests.__version__

    pip_file_16 = Path("/tmp/runtime_env_pip_16.txt")
    pip_file_16.write_text("requests==2.16.0")

    env_16 = {"pip": str(pip_file_16)}
    actor_16 = VersionActor.options(runtime_env=env_16).remote()
    assert ray.get(actor_16.get_version.remote() == "2.16.0")

    os.remove(pip_file_18)
    os.remove(pip_file_16)


def main():
    test_pip_requirements_files()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Finish quickly for testing.")
    args = parser.parse_args()

    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "rte_ray_client")
    if is_anyscale_connect(addr):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    main()

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/rte_ray_client.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
