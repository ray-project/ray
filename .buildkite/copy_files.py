import argparse
import os
from collections import OrderedDict
import sys
import time
import subprocess

from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import requests

parser = argparse.ArgumentParser(
    description="Helper script to upload files to S3 bucket")
parser.add_argument("--path", type=str, required=False)
parser.add_argument("--destination", type=str)
args = parser.parse_args()

assert args.destination in {"wheels", "containers", "logs", "docker_login"}
assert "BUILDKITE_JOB_ID" in os.environ
assert "BUILDKITE_COMMIT" in os.environ

auth = BotoAWSRequestsAuth(
    aws_host="vop4ss7n22.execute-api.us-west-2.amazonaws.com",
    aws_region="us-west-2",
    aws_service="execute-api",
)

for _ in range(5):
    resp = requests.get(
        "https://vop4ss7n22.execute-api.us-west-2.amazonaws.com/endpoint/",
        auth=auth,
        params={"job_id": os.environ["BUILDKITE_JOB_ID"]})
    print("Getting Presigned URL, status_code", resp.status_code)
    if resp.status_code >= 500:
        print("errored, retrying...")
        print(resp.text)
        time.sleep(5)
    else:
        break
if resp.status_code >= 500:
    print("still errorred after many retries")
    sys.exit(1)

if args.destination == "docker_login":
    pwd = resp.json()["docker_password"]
    subprocess.call(
        ["docker", "login", "--username", "raytravisbot", "--password", pwd])
    sys.exit(0)

sha = os.environ["BUILDKITE_COMMIT"]
assert os.path.exists(args.path)
is_dir = os.path.isdir(args.path)
if is_dir:
    paths = [os.path.join(args.path, f) for f in os.listdir(args.path)]
else:
    paths = [args.path]
print("Planning to upload", paths)

for path in paths:
    fn = os.path.split(path)[-1]
    if args.destination == "wheels":
        c = resp.json()["presigned_resp_prod_wheels"]
        of = OrderedDict(c["fields"])
        of["key"] = f"{sha}/{fn}"

    elif args.destination == "containers":
        c = resp.json()["presigned_containers"]
        of = OrderedDict(c["fields"])
        of["key"] = f"{sha}/{fn}"

    elif args.destination == "logs":
        c = resp.json()["presigned_logs"]
        of = OrderedDict(c["fields"])
        branch = os.environ["BUILDKITE_BRANCH"]
        bk_job_id = os.environ["BUILDKITE_JOB_ID"]
        of["key"] = f"bazel_events/{branch}/{sha}/{bk_job_id}/{fn}"

    else:
        raise ValueError("Unknown destination")

    of["file"] = open(path, "rb")
    r = requests.post(c["url"], files=of)
    print(f"Uploaded {path} to {of['key']}", r.status_code)
