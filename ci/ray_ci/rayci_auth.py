"""
Authentication library for RayCI API endpoints.

Provides authenticated access to the RayCI credentials API for Docker Hub
login and S3 presigned URLs.
"""

import os
import subprocess
import time

import requests
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

RAYCI_API_HOST = "vop4ss7n22.execute-api.us-west-2.amazonaws.com"
RAYCI_API_URL = f"https://{RAYCI_API_HOST}/endpoint/"
RAYCI_API_REGION = "us-west-2"
RAYCI_API_SERVICE = "execute-api"
DOCKER_HUB_USERNAME = "raydockerreleaser"


class RayCIAuthError(Exception):
    """Error raised when RayCI API authentication fails."""


def _retry(f):
    """Retry decorator for API calls with fixed-interval retry on 5xx errors."""

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
            raise RayCIAuthError(
                "Failed to get a valid response from RayCI API after multiple retries."
            )

    return inner


@_retry
def get_rayci_api_response():
    """
    Fetch credentials from the RayCI API endpoint.

    Uses AWS SigV4 authentication via the instance's IAM role.
    Requires BUILDKITE_JOB_ID environment variable.

    Returns:
        requests.Response containing credentials for Docker Hub and S3.
    """
    job_id = os.environ.get("BUILDKITE_JOB_ID")
    if not job_id:
        raise ValueError("BUILDKITE_JOB_ID environment variable must be set.")

    auth = BotoAWSRequestsAuth(
        aws_host=RAYCI_API_HOST,
        aws_region=RAYCI_API_REGION,
        aws_service=RAYCI_API_SERVICE,
    )
    resp = requests.get(
        RAYCI_API_URL,
        auth=auth,
        params={"job_id": job_id},
    )
    return resp


def docker_hub_login() -> None:
    """
    Login to Docker Hub using credentials from RayCI API.

    Fetches credentials and runs `docker login` for the rayproject account.
    """
    resp = get_rayci_api_response()
    try:
        pwd = resp.json()["docker_password"]
    except (requests.exceptions.JSONDecodeError, KeyError) as e:
        raise RayCIAuthError(
            f"Could not get docker_password from API response: {resp.text}"
        ) from e
    subprocess.run(
        ["docker", "login", "--username", DOCKER_HUB_USERNAME, "--password-stdin"],
        input=pwd.encode(),
        check=True,
    )
