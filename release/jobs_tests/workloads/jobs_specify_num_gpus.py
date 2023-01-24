"""Job submission test

This test checks that when using the Ray Jobs API with num_gpus
specified, the driver is run on a node that has a GPU.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import argparse
import json
import os
import time
import torch
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
    job_name = os.environ.get("RAY_JOB_NAME", "jobs_specify_num_gpus")

    if address is not None and address.startswith("anyscale://"):
        pass
    else:
        address = "http://127.0.0.1:8265"

    client = JobSubmissionClient(address)

    # This test script runs on the head node, which should not have a GPU.
    assert not torch.cuda.is_available()

    path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "jobs_check_cuda_available.py")
    )

    for num_gpus in [0, 0.1]:
        job_id = client.submit_job(
            entrypoint=f"python '{path}",
            runtime_env={"working_dir": args.working_dir},
            entrypoint_num_gpus=num_gpus,
        )
        timeout_s = 10 * 60
        status = wait_until_finish(client=client, job_id=job_id, timeout_s=timeout_s)
        job_info = client.get_job_info(job_id)
        print("Status message: ", job_info.message)

        if num_gpus == 0:
            # We didn't specify any GPUs, so the driver should run on the head node.
            # The head node should not have a GPU, so the job should fail.
            assert status == JobStatus.FAILED
            assert "CUDA is not available in the driver script" in job_info.message
        else:
            # We specified a GPU, so the driver should run on the worker node
            # with a GPU, so the job should succeed.
            assert status == JobStatus.SUCCEEDED

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/jobs_specify_num_gpus.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED")
