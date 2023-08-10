import time
import os
import subprocess
import json
from typing import Any, Dict

import boto3

from ray_release.logger import logger

BASE_IMAGE_WAIT_TIMEOUT = 7200
BASE_IMAGE_WAIT_DURATION = 30
DOCKER_ECR = "029272617770.dkr.ecr.us-west-2.amazonaws.com"
DOCKER_PROJECT = "ci_base_images"
PY_VERSIONS = ["py38", "py39"]


def build_ray() -> None:
    """
    Builds ray and ray-ml images for PR builds
    """
    if not _is_pr():
        logger.info("Not a PR, skipping build_ray")
        return
    start = int(time.time())
    base_image = _get_docker_name()
    while (
        not _base_image_exist() and int(time.time()) - start < BASE_IMAGE_WAIT_TIMEOUT
    ):
        timeout = BASE_IMAGE_WAIT_TIMEOUT - (int(time.time()) - start)
        logger.info(
            f"Image {base_image} does not exist yet. " f"Wait for another {timeout}s..."
        )
        time.sleep(BASE_IMAGE_WAIT_DURATION)
    _upload_builds()


def _is_pr() -> bool:
    return os.getenv("BUILDKITE_PULL_REQUEST", "false") != "false"


def _upload_builds() -> None:
    builds = {"steps": [_get_build(py_version) for py_version in PY_VERSIONS]}
    subprocess.run(
        ["buildkite-agent", "pipeline", "upload"],
        input=json.dumps(builds).encode(),
    )


def _get_docker_name() -> str:
    return f"{DOCKER_ECR}/{DOCKER_PROJECT}:{_get_docker_image_tag()}"


def _get_docker_image_tag() -> str:
    return f"oss-ci-build_{os.environ.get('BUILDKITE_COMMIT', '')}"


def _get_build(py_version: str) -> Dict[str, Any]:
    cmd = [
        f"LINUX_WHEELS=1 BUILD_ONE_PYTHON_ONLY={py_version} ./ci/ci.sh build",
        "pip install -q docker aws_requests_auth boto3",
        "./ci/env/env_info.sh",
        "python .buildkite/copy_files.py --destination docker_login",
        f"python ./ci/build/build-docker-images.py --py-versions {py_version} "
        "-T cpu -T cu118 --build-type BUILDKITE --build-base",
    ]
    return {
        "label": py_version,
        "agents": {"queue": "runner_queue_medium_branch"},
        "commands": cmd,
        "plugins": [
            {
                "ray-project/dind#v1.0.10": {
                    "network-name": "dind-network",
                    "certs-volume-name": "ray-docker-certs-client",
                    "additional-volume-mount": "ray-volume:/ray",
                },
            },
            {
                "docker#v5.3.0": {
                    "image": _get_docker_name(),
                    "shell": ["/bin/bash", "-ecil"],
                    "network": "dind-network",
                    "volumes": [
                        "ray-docker-certs-client:/certs/client:ro",
                        "ray-volume:/ray-mount",
                        "/tmp/artifacts:/artifact-mount",
                    ],
                    "workdir": "/ray",
                    "add-caps:": ["SYS_ADMIN", "SYS_PTRACE", "NET_ADMIN"],
                    "shm-size": "2.5gb",
                    "environment": [
                        "BUILDKITE_JOB_ID",
                        "BUILDKITE_COMMIT",
                        "BUILDKITE_LABEL",
                        "BUILDKITE_BRANCH",
                        "BUILDKITE_BUILD_URL",
                        "BUILDKITE_BUILD_ID",
                        "BUILDKITE_MESSAGE",
                        "BUILDKITE_BUILD_NUMBER",
                        "BUILDKITE_PIPELINE_ID",
                    ],
                    "mount-checkout": "false",
                },
            },
        ],
    }


def _base_image_exist() -> bool:
    """
    Checks if the given base docer image exists.
    """
    client = boto3.client("ecr", region_name="us-west-2")
    try:
        client.describe_images(
            repositoryName=DOCKER_PROJECT,
            imageIds=[{"imageTag": _get_docker_image_tag()}],
        )
        return True
    except client.exceptions.ImageNotFoundException:
        return False
