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
import json

import requests


def get_build_env():
    keys = [
        "TRAVIS_BRANCH", "TRAVIS_BUILD_ID", "TRAVIS_BUILD_NUMBER",
        "TRAVIS_BUILD_WEB_URL", "TRAVIS_COMMIT", "TRAVIS_COMMIT_MESSAGE",
        "TRAVIS_DIST", "TRAVIS_JOB_ID", "TRAVIS_JOB_NUMBER",
        "TRAVIS_JOB_WEB_URL", "TRAVIS_OS_NAME", "TRAVIS_TEST_RESULT"
    ]
    return {key: os.environ.get(key) for key in keys}


def get_build_config():
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
