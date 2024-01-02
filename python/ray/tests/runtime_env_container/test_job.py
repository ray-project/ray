import argparse

import ray
from ray.job_submission import JobStatus, JobSubmissionClient
from ray._private.test_utils import wait_for_condition

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
args = parser.parse_args()

worker_pth = "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py"  # noqa


ray.init()
client = JobSubmissionClient()
job_id = client.submit_job(
    entrypoint="python -V",
    runtime_env={"container": {"image": args.image, "worker_path": worker_pth}},
)


def check_job_succeeded():
    assert client.get_job_status(job_id) == JobStatus.SUCCEEDED
    return True


wait_for_condition(check_job_succeeded)
logs = client.get_job_logs(job_id)
print("Job Logs:", logs)
