import argparse
import os
from collections import OrderedDict
import sys
import time
import subprocess
from typing import List

from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import requests


def retry(f):
    def inner():
        resp = None
        for _ in range(5):
            resp = f()
            print("Getting Presigned URL, status_code", resp.status_code)
            if resp.status_code >= 500:
                print("errored, retrying...")
                print(resp.text)
                time.sleep(5)
            else:
                return resp
        if resp is None or resp.status_code >= 500:
            print("still errorred after many retries")
            sys.exit(1)

    return inner


@retry
def perform_auth():
    auth = BotoAWSRequestsAuth(
        aws_host="vop4ss7n22.execute-api.us-west-2.amazonaws.com",
        aws_region="us-west-2",
        aws_service="execute-api",
    )
    resp = requests.get(
        "https://vop4ss7n22.execute-api.us-west-2.amazonaws.com/endpoint/",
        auth=auth,
        params={"job_id": os.environ["BUILDKITE_JOB_ID"]},
    )
    return resp


def handle_docker_login(resp):
    pwd = resp.json()["docker_password"]
    subprocess.call(
        ["docker", "login", "--username", "raytravisbot", "--password", pwd]
    )


def gather_paths(dir_path) -> List[str]:
    dir_path = dir_path.replace("/", os.path.sep)
    assert os.path.exists(dir_path)
    if os.path.isdir(dir_path):
        paths = [os.path.join(dir_path, f) for f in os.listdir(dir_path)]
    else:
        paths = [dir_path]
    return paths


dest_resp_mapping = {
    "wheels": "presigned_resp_prod_wheels",
    "branch_wheels": "presigned_resp_prod_wheels",
    "jars": "presigned_resp_prod_wheels",
    "branch_jars": "presigned_resp_prod_wheels",
    "logs": "presigned_logs",
}


def upload_paths(paths, resp, destination):
    dest_key = dest_resp_mapping[destination]
    c = resp.json()[dest_key]
    of = OrderedDict(c["fields"])

    sha = os.environ["BUILDKITE_COMMIT"]
    branch = os.environ["BUILDKITE_BRANCH"]
    bk_job_id = os.environ["BUILDKITE_JOB_ID"]

    current_os = sys.platform

    for path in paths:
        fn = os.path.split(path)[-1]
        of["key"] = {
            "wheels": f"latest/{fn}",
            "branch_wheels": f"{branch}/{sha}/{fn}",
            "jars": f"jars/latest/{current_os}/{fn}",
            "branch_jars": f"jars/{branch}/{sha}/{current_os}/{fn}",
            "logs": f"bazel_events/{branch}/{sha}/{bk_job_id}/{fn}",
        }[destination]
        of["file"] = open(path, "rb")
        r = requests.post(c["url"], files=of)
        print(f"Uploaded {path} to {of['key']}", r.status_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Helper script to upload files to S3 bucket"
    )
    parser.add_argument("--path", type=str, required=False)
    parser.add_argument("--destination", type=str)
    args = parser.parse_args()

    assert args.destination in {
        "branch_jars",
        "branch_wheels",
        "jars",
        "logs",
        "wheels",
        "docker_login",
    }
    assert "BUILDKITE_JOB_ID" in os.environ
    assert "BUILDKITE_COMMIT" in os.environ

    resp = perform_auth()

    if args.destination == "docker_login":
        handle_docker_login(resp)
    else:
        paths = gather_paths(args.path)
        print("Planning to upload", paths)
        upload_paths(paths, resp, args.destination)
