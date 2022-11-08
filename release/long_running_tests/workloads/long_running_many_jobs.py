"""Job submission long running test

Submits many simple jobs on a long running cluster.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import argparse
import json
import os
import time
from typing import List
import ray
from ray.job_submission import JobSubmissionClient

NUM_CLIENTS = 4
NUM_JOBS_PER_BATCH = 4

SMOKE_TEST_TIMEOUT = 10 * 60  # 10 minutes
FULL_TEST_TIMEOUT = 8 * 60 * 60  # 8 hours

SLEEP_TIME_PER_BATCH_S = 4


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
    address = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "jobs_basic")

    if address is None or not address.startswith("anyscale://"):
        address = "http://127.0.0.1:8265"

    clients = [JobSubmissionClient(address) for i in range(NUM_CLIENTS)]

    batch_counter = 0
    while time.time() - start < timeout:
        batch_counter += 1
        print(f"Submitting batch {batch_counter}...")
        # Submit a batch of jobs
        if not submit_batch_jobs(clients, NUM_JOBS_PER_BATCH):
            print("FAILED")
            exit(1)

    time.sleep(SLEEP_TIME_PER_BATCH_S)

    time_taken = time.time() - start
    result = {
        "time_taken": time_taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/jobs_basic.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED")
