import argparse

import ray
from ray._common.test_utils import wait_for_condition
from ray.job_submission import JobStatus, JobSubmissionClient

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
parser.add_argument(
    "--use-image-uri-api",
    action="store_true",
    help="Whether to use the new `image_uri` API instead of the old `container` API.",
)
args = parser.parse_args()

if args.use_image_uri_api:
    runtime_env = {"image_uri": args.image}
else:
    runtime_env = {"container": {"image": args.image}}


ray.init()
client = JobSubmissionClient()
job_id = client.submit_job(
    entrypoint="cat file.txt",
    runtime_env=runtime_env,
)


def check_job_succeeded():
    assert client.get_job_status(job_id) == JobStatus.SUCCEEDED
    return True


wait_for_condition(check_job_succeeded)
logs = client.get_job_logs(job_id)
print("Job Logs:", logs)
assert "helloworldalice" in logs
