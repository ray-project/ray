"""Job submission long running test

Submits many simple jobs on a long running cluster.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import argparse
import os
import time
import random
from typing import List, Optional
from ray.dashboard.modules.job.common import JobStatus
from ray.dashboard.modules.job.pydantic_models import JobDetails
import ray
from ray.job_submission import JobSubmissionClient
from ray._private.test_utils import safe_write_to_results_json

NUM_CLIENTS = 4
NUM_JOBS_PER_BATCH = 4

SMOKE_TEST_TIMEOUT = 10 * 60  # 10 minutes
FULL_TEST_TIMEOUT = 8 * 60 * 60  # 8 hours


def _is_job_not_found_error(exc: RuntimeError) -> bool:
    error_message = str(exc)
    return "status code 404" in error_message


def _ignore_not_found(job_id: str, func, name: str):
    try:
        return func()
    except RuntimeError as exc:
        if _is_job_not_found_error(exc):
            print(
                f"Job {job_id} was not found when {name}; "
                "ignoring because the platform may have already deleted it."
            )
            return None
        raise


def _get_job_info_or_none(
    client: JobSubmissionClient, job_id: str
) -> Optional[JobDetails]:
    return _ignore_not_found(
        job_id, lambda: client.get_job_info(job_id), "fetching info"
    )


def _get_job_logs_or_none(client: JobSubmissionClient, job_id: str) -> Optional[str]:
    return _ignore_not_found(
        job_id, lambda: client.get_job_logs(job_id), "fetching logs"
    )


def wait_until_finish(
    client: JobSubmissionClient,
    job_id: str,
    timeout_s: int = 10 * 60,
    retry_interval_s: int = 10,
) -> Optional[JobStatus]:
    start_time_s = time.time()
    while time.time() - start_time_s <= timeout_s:
        try:
            status = client.get_job_status(job_id)
        except RuntimeError as exc:
            if _is_job_not_found_error(exc):
                print(
                    f"Job {job_id} was not found during periodic status checks; "
                    "ignoring because the platform may have already deleted it."
                )
                return JobStatus.SUCCEEDED
            raise
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
                f"Info for failed/timed-out job {job_id}: {_get_job_info_or_none(client, job_id)}"
            )
            print(
                f"Logs for failed/timed-out job {job_id}: {_get_job_logs_or_none(client, job_id)}"
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

        # Test list jobs
        jobs: List[JobDetails] = clients[0].list_jobs()
        print(f"Total jobs submitted so far: {len(jobs)}")

        # Get job logs from random submission job
        is_submission_job = False
        while not is_submission_job:
            job_details = random.choice(jobs)
            is_submission_job = job_details.type == "SUBMISSION"
        job_id = job_details.submission_id
        print(f"Getting logs for randomly chosen job {job_id}...")
        logs = _get_job_logs_or_none(clients[0], job_id)
        if logs is not None:
            print(logs)

    time_taken = time.time() - start
    result = {
        "time_taken": time_taken,
    }
    safe_write_to_results_json(result, "/tmp/jobs_basic.json")

    print("PASSED")
