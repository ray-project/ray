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
import tempfile
import time
from pathlib import Path

import ray


def test_pip_requirements_files(tmpdir: str):
    """Test requirements.txt with tasks and actors.

    Test specifying in @ray.remote decorator and in .options.
    """
    pip_file_18 = Path(os.path.join(tmpdir, "runtime_env_pip_18.txt"))
    pip_file_18.write_text("requests==2.18.0")
    env_18 = {"pip": str(pip_file_18)}

    pip_file_16 = Path(os.path.join(tmpdir, "runtime_env_pip_16.txt"))
    pip_file_16.write_text("requests==2.16.0")
    env_16 = {"pip": str(pip_file_16)}

    @ray.remote(runtime_env=env_16)
    def get_version():
        import requests

        return requests.__version__

    # TODO(architkulkarni): Uncomment after #19002 is fixed
    # assert ray.get(get_version.remote()) == "2.16.0"
    assert ray.get(get_version.options(runtime_env=env_18).remote()) == "2.18.0"

    @ray.remote(runtime_env=env_18)
    class VersionActor:
        def get_version(self):
            import requests

            return requests.__version__

    # TODO(architkulkarni): Uncomment after #19002 is fixed
    # actor_18 = VersionActor.remote()
    # assert ray.get(actor_18.get_version.remote()) == "2.18.0"

    actor_16 = VersionActor.options(runtime_env=env_16).remote()
    assert ray.get(actor_16.get_version.remote()) == "2.16.0"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing."
    )
    args = parser.parse_args()

    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "rte_ray_client")
    # Test reconnecting to the same cluster multiple times.
    for use_working_dir in [True, True, False, False]:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_env = {"working_dir": tmpdir} if use_working_dir else None
            print("Testing with use_working_dir=" + str(use_working_dir))
            if addr is not None and addr.startswith("anyscale://"):
                ray.init(address=addr, job_name=job_name, runtime_env=runtime_env)
            else:
                ray.init(address="auto", runtime_env=runtime_env)
            test_pip_requirements_files(tmpdir)
            ray.shutdown()

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/rte_ray_client.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED")
