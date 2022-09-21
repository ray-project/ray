"""Job submission long running test

Submits many jobs on a long running cluster.

20 jobs every 10 seconds for 8 hours (or 10 minutes if smoke test).
Each job sleeps for 60 seconds. Total of ~50k jobs.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import argparse
import json
import os
import time
import pprint
from typing import Optional
from ray.dashboard.modules.job.common import JobStatus

from ray.job_submission import JobSubmissionClient


def wait_until_finish(
    client: JobSubmissionClient,
    job_id: str,
    timeout_s: int = 10 * 60,
    retry_interval_s: int = 10,
) -> Optional[JobStatus]:
    start_time_s = time.time()
    while time.time() - start_time_s <= timeout_s:
        # Test calling list_jobs
        client.list_jobs()
        status = client.get_job_status(job_id)
        print(f"status for job {job_id}: {status}")
        if status in {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}:
            return status
        time.sleep(retry_interval_s)
    return None


def submit_batch_jobs(
    client: JobSubmissionClient,
    num_jobs: int,
    timeout_s: int = 10 * 60,
    retry_interval_s: int = 1,
) -> bool:
    job_ids = []
    for i in range(num_jobs):
        job_id = client.submit_job(
            runtime_env={"working_dir": os.path.dirname(os.path.abspath(__file__))},
            entrypoint="echo starting && sleep 60 && echo finished",
        )
        job_ids.append(job_id)
        print(f"submitted job: {job_id}")

    for job_id in job_ids:
        status = wait_until_finish(client, job_id, timeout_s, retry_interval_s)
        if status != JobStatus.SUCCEEDED:
            print(
                f"job {job_id} failed with status {status} (`None` indicates timeout)"
            )
            return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing."
    )

    args = parser.parse_args()
    if args.smoke_test:
        print("Running smoke test")
        timeout = 10 * 60
    else:
        print("Running full test")
        timeout = 8 * 60 * 60

    start = time.time()

    address = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "jobs_basic")

    if address is None or not address.startswith("anyscale://"):
        address = "http://127.0.0.1:8265"

    client = JobSubmissionClient(address)

    while time.time() - start < timeout:
        # Submit a batch of jobs
        if not submit_batch_jobs(client, 20):
            print("FAILED")
            exit(1)

        # Test list jobs
        jobs = client.list_jobs()
        print("list jobs:")
        pprint.pprint(jobs)

        time.sleep(10)

    time_taken = time.time() - start
    result = {
        "time_taken": time_taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/jobs_basic.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED")
