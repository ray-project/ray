import argparse
import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from collections import OrderedDict
from typing import List

import requests
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth


class BKAnalyticsUploader:
    @staticmethod
    def yield_result_xml_path(event_file_path):
        with open(event_file_path) as f:
            for l in f:
                event_line = json.loads(l)
                if "testResult" not in event_line:
                    continue
                if "testActionOutput" not in event_line["testResult"]:
                    continue
                for output in event_line["testResult"]["testActionOutput"]:
                    if output["name"].endswith("xml"):
                        uri = output["uri"].replace("file://", "")
                        yield uri

    @staticmethod
    def read_and_transform_xml(xml_path):
        f = ET.parse(xml_path)
        case = next(f.iter("testcase"))
        test_name = case.attrib["name"]
        *classname_chunks, name = test_name.split("/")
        case.set("name", name)
        case.set("classname", "/".join(classname_chunks))
        return ET.tostring(f.getroot(), encoding="utf8", method="xml").decode()

    @staticmethod
    def send_to_buildkite(xml_string, token):
        resp = requests.post(
            "https://analytics-api.buildkite.com/v1/uploads",
            headers={"Authorization": f'Token token="{token}"'},
            json={
                "format": "junit",
                "run_env": {
                    "CI": "buildkite",
                    "key": os.environ["BUILDKITE_BUILD_ID"],
                    "job_id": os.environ["BUILDKITE_JOB_ID"],
                    "branch": os.environ["BUILDKITE_BRANCH"],
                    "commit_sha": os.environ["BUILDKITE_COMMIT"],
                    "message": os.environ["BUILDKITE_MESSAGE"],
                    "url": os.environ["BUILDKITE_BUILD_URL"],
                },
                "data": xml_string
            })
        print(resp.text)
        resp.raise_for_status()


def upload_to_bk_analytics(event_files_dir, token):
    event_dir = event_files_dir.replace("/", os.path.sep)
    for name in os.listdir(event_dir):
        if "bazel_log" not in name:
            continue

        path = os.path.join(event_dir, name)
        for xml_path in BKAnalyticsUploader.yield_result_xml_path(path):
            assert os.path.exists(xml_path), xml_path
            parsed_xml = BKAnalyticsUploader.read_and_transform_xml(xml_path)
            BKAnalyticsUploader.send_to_buildkite(parsed_xml, token)


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
        params={"job_id": os.environ["BUILDKITE_JOB_ID"]})
    return resp


def handle_docker_login(resp):
    pwd = resp.json()["docker_password"]
    subprocess.call(
        ["docker", "login", "--username", "raytravisbot", "--password", pwd])


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
            "logs": f"bazel_events/{branch}/{sha}/{bk_job_id}/{fn}"
        }[destination]
        of["file"] = open(path, "rb")
        r = requests.post(c["url"], files=of)
        print(f"Uploaded {path} to {of['key']}", r.status_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Helper script to upload files to S3 bucket")
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

    # Additionally, we will push the logs to Buildkite test analytics (beta)
    if args.destination == "logs":
        token = resp.json()["bk_analytics_token"]
        upload_to_bk_analytics(args.path, token)
