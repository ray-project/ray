"""Job submission long running test

Submits many simple jobs on a long running cluster.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import argparse
import json
import os
import time
import random
from typing import List, Optional
from ray.dashboard.modules.job.common import JobStatus
from ray.dashboard.modules.job.pydantic_models import JobDetails
import ray
from ray.job_submission import JobSubmissionClient

NUM_CLIENTS = 4
NUM_JOBS_PER_BATCH = 8

SMOKE_TEST_TIMEOUT = 10 * 60  # 10 minutes
FULL_TEST_TIMEOUT = 8 * 60 * 60  # 8 hours


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
        if status in {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}:
            return status
        time.sleep(retry_interval_s)
    return None


def submit_batch_jobs(
    clients: List[JobSubmissionClient],
    num_jobs: int,
    timeout_s: int = 10 * 60,
    retry_interval_s: int = 1,
) -> bool:
    job_ids = []
    for i in range(num_jobs):
        # Cycle through clients arbitrarily
        client = clients[i % len(clients)]
        job_id = client.submit_job(
            entrypoint="echo hello",
        )
        job_ids.append(job_id)
        print(f"submitted job: {job_id}")

    for job_id in job_ids:
        client = clients[job_ids.index(job_id) % len(clients)]
        status = wait_until_finish(client, job_id, timeout_s, retry_interval_s)
        if status != JobStatus.SUCCEEDED:
            print(
                f"Info for failed/timed-out job {job_id}: {client.get_job_info(job_id)}"
            )
            print(
                f"Logs for failed/timed-out job {job_id}: {client.get_job_logs(job_id)}"
            )
            print(
                f"Job {job_id} failed with status {status} (`None` indicates timeout)"
            )
            return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing."
    )

    parser.add_argument("--num-clients", type=int, default=NUM_CLIENTS)
    parser.add_argument("--num-jobs-per-batch", type=int, default=NUM_JOBS_PER_BATCH)

    args = parser.parse_args()
    if args.smoke_test:
        print(f"Running smoke test with timeout {SMOKE_TEST_TIMEOUT} seconds")
        timeout = SMOKE_TEST_TIMEOUT
    else:
        print(f"Running full test (timeout: {FULL_TEST_TIMEOUT}s)")
        timeout = FULL_TEST_TIMEOUT

    start = time.time()

    ray.init()

    @ray.remote
    class Foo:
        def __init__(self):
            self.x = 1
        def bar(self):
            return self.x
    
    # Instantiate 4 actors every 4 seconds
    while time.time() - start < timeout:
        print("Instantiating actors...")
        actors = [Foo.remote() for i in range(4)]
        print("Getting actor results...")
        print(ray.get([actor.bar.remote() for actor in actors]))
        time.sleep(4)

    time_taken = time.time() - start
    result = {
        "time_taken": time_taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/jobs_basic.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED")
