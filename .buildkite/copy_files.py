import argparse
import os
import sys
from collections import OrderedDict

import requests

from ci.ray_ci.rayci_auth import docker_hub_login, get_rayci_api_response


def gather_paths(dir_path):
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

    if os.environ.get("RAYCI_SKIP_UPLOAD", "false") == "true":
        print("Skipping upload.")
        sys.exit(0)

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

    if args.destination == "docker_login":
        docker_hub_login()
    else:
        resp = get_rayci_api_response()
        paths = gather_paths(args.path)
        print("Planning to upload", paths)
        upload_paths(paths, resp, args.destination)
