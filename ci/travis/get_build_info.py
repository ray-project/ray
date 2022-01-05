#!/usr/bin/env python
"""
This script gathers build metadata from Travis environment variables and Travis
APIs.

Usage:
$ python get_build_info.py
{
    "json": ["containing", "build", "metadata"]
}
"""

import os
import sys
import json


def gha_get_self_url():
    import requests
    # stringed together api call to get the current check's html url.
    sha = os.environ["GITHUB_SHA"]
    repo = os.environ["GITHUB_REPOSITORY"]
    resp = requests.get(
        "https://api.github.com/repos/{}/commits/{}/check-suites".format(
            repo, sha))
    data = resp.json()
    for check in data["check_suites"]:
        slug = check["app"]["slug"]
        if slug == "github-actions":
            run_url = check["check_runs_url"]
            html_url = (
                requests.get(run_url).json()["check_runs"][0]["html_url"])
            return html_url

    # Return a fallback url
    return "https://github.com/ray-project/ray/actions"


def get_build_env():
    if os.environ.get("GITHUB_ACTION"):
        return {
            "TRAVIS_COMMIT": os.environ["GITHUB_SHA"],
            "TRAVIS_JOB_WEB_URL": gha_get_self_url(),
            "TRAVIS_OS_NAME": "windows",
        }

    if os.environ.get("BUILDKITE"):
        return {
            "TRAVIS_COMMIT": os.environ["BUILDKITE_COMMIT"],
            "TRAVIS_JOB_WEB_URL": (os.environ["BUILDKITE_BUILD_URL"] + "#" +
                                   os.environ["BUILDKITE_BUILD_ID"]),
            "TRAVIS_OS_NAME":  # The map is used to stay consistent with Travis
            {
                "linux": "linux",
                "darwin": "osx",
                "win32": "windows",
            }[sys.platform],
        }

    keys = [
        "TRAVIS_COMMIT",
        "TRAVIS_JOB_WEB_URL",
        "TRAVIS_OS_NAME",
    ]
    return {key: os.environ.get(key) for key in keys}


def get_build_config():
    if os.environ.get("GITHUB_ACTION"):
        return {"config": {"env": "Windows CI"}}

    if os.environ.get("BUILDKITE"):
        return {
            "config": {
                "env": "Buildkite " + os.environ["BUILDKITE_LABEL"]
            }
        }

    import requests
    url = "https://api.travis-ci.com/job/{job_id}?include=job.config"
    url = url.format(job_id=os.environ["TRAVIS_JOB_ID"])
    resp = requests.get(url, headers={"Travis-API-Version": "3"})
    return resp.json()


if __name__ == "__main__":
    build_env = get_build_env()
    build_config = get_build_config()

    print(
        json.dumps(
            {
                "build_env": build_env,
                "build_config": build_config
            }, indent=2))
