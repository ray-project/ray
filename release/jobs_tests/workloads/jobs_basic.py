"""Job submission test

This test runs a basic Tune job on a remote cluster.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import argparse
import json
import os
import time
from typing import Optional
from ray.dashboard.modules.job.common import JobStatus

from ray.job_submission import JobSubmissionClient


def wait_until_finish(
    client: JobSubmissionClient,
    job_id: str,
    timeout_s: int = 10 * 60,
    retry_interval_s: int = 1,
) -> Optional[JobStatus]:
    start_time_s = time.time()
    while time.time() - start_time_s <= timeout_s:
        status = client.get_job_status(job_id)
        print(f"status: {status}")
        if status in {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}:
            return status
        time.sleep(retry_interval_s)
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing."
    )
    parser.add_argument(
        "--working-dir",
        required=True,
        help="working_dir to use for the job within this test.",
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
    path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "run_simple_tune_job.py")
    )
    job_id = client.submit_job(
        entrypoint=f"python '{path}'",
        runtime_env={"pip": ["ray[tune]"], "working_dir": args.working_dir},
    )
    timeout_s = 10 * 60
    status = wait_until_finish(client=client, job_id=job_id, timeout_s=timeout_s)

    print("Status message: ", client.get_job_info(job_id=job_id).message)

    assert status == JobStatus.SUCCEEDED

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
