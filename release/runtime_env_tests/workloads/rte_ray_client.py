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
import pkg_resources


def _get_pip_install_test_version() -> str:
    return pkg_resources.get_distribution("pip-install-test").version


def test_pip_requirements_files(tmpdir: str):
    """Test requirements.txt with tasks and actors.

    Test specifying in @ray.remote decorator and in .options.
    """

    files = {}
    envs = {}

    for version in ["0.4", "0.5"]:
        files[version] = Path(os.path.join(tmpdir, f"requirements_{version}.txt"))
        files[version].write_text(f"pip-install-test=={version}")
        envs[version] = {"pip": str(files[version])}

    @ray.remote(runtime_env=envs["0.4"])
    def get_version():
        return _get_pip_install_test_version()

    assert ray.get(get_version.remote()) == "0.4"
    assert ray.get(get_version.options(runtime_env=envs["0.5"]).remote()) == "0.5"

    @ray.remote(runtime_env=envs["0.4"])
    class VersionActor:
        def get_version(self):
            return _get_pip_install_test_version()

    actor = VersionActor.remote()
    assert ray.get(actor.get_version.remote()) == "0.4"

    actor = VersionActor.options(runtime_env=envs["0.5"]).remote()
    assert ray.get(actor.get_version.remote()) == "0.5"


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
