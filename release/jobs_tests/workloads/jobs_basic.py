"""Runtime env test on Ray Client

This test installs runtime environments on a remote cluster using local
pip requirements.txt files.  It is intended to be run using Anyscale connect.
This complements existing per-commit tests in CI, for which we don't have
access to a physical remote cluster.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import argparse
from asyncio import subprocess
import json
import os
import tempfile
import time
from pathlib import Path

import ray
from ray.job_submission import JobSubmissionClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing."
    )
    args = parser.parse_args()

    start = time.time()

    address = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "jobs_basic")

    if address is not None and address.startswith("anyscale://"):
        pass
    else:
        address = "http://127.0.0.1:8265"
    
    client = JobSubmissionClient(address)
    job_id = client.submit_job(entrypoint="python jobs_basic_driver_script.py", runtime_env={"pip": ["ray[tune]"], "working_dir": "./"}, )

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/jobs_basic.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    logs = client.get_job_logs(job_id)
    assert "Starting Ray Tune job" in logs
    assert "Best config:" in logs

    print("PASSED")
