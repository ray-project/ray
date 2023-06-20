from typing import List

import boto3
import hashlib
import os
import subprocess
import sys
import time

from ray_release.config import RELEASE_PACKAGE_DIR
from ray_release.logger import logger
from ray_release.test import Test

DATAPLANE_S3_BUCKET = "ray-release-automation-results"
DATAPLANE_FILENAME = "dataplane.tgz"
DATAPLANE_DIGEST = "f9b0055085690ddad2faa804bb6b38addbcf345b9166f2204928a7ece1c8a39b"
BASE_IMAGE_WAIT_TIMEOUT = 7200
BASE_IMAGE_WAIT_DURATION = 30
RELEASE_BYOD_DIR = os.path.join(RELEASE_PACKAGE_DIR, "ray_release/byod")


def build_anyscale_byod_images(tests: List[Test]) -> None:
    """
    Builds the Anyscale BYOD images for the given tests.
    """
    _download_dataplane_build_file()
    to_be_built = {}
    built = set()
    for test in tests:
        if not test.is_byod_cluster():
            continue
        to_be_built[test.get_ray_image()] = test

    env = os.environ.copy()
    env["DOCKER_BUILDKIT"] = "1"
    start = int(time.time())
    # ray images are built on post-merge, so we can wait for them to be available
    while (
        len(built) < len(to_be_built)
        and int(time.time()) - start < BASE_IMAGE_WAIT_TIMEOUT
    ):
        for ray_image, test in to_be_built.items():
            byod_image = test.get_anyscale_byod_image()
            if _byod_image_exist(test):
                logger.info(f"Image {byod_image} already exists")
                built.add(ray_image)
                continue
            if not _ray_image_exist(ray_image):
                # TODO(can): instead of waiting for the base image to be built, we can
                #  build it ourselves
                timeout = BASE_IMAGE_WAIT_TIMEOUT - (int(time.time()) - start)
                logger.info(
                    f"Image {ray_image} does not exist yet. "
                    f"Wait for another {timeout}s..."
                )
                time.sleep(BASE_IMAGE_WAIT_DURATION)
                continue
            logger.info(f"Building {byod_image} from {ray_image}")
            with open(DATAPLANE_FILENAME, "rb") as build_file:
                subprocess.check_call(
                    [
                        "docker",
                        "build",
                        "--build-arg",
                        f"BASE_IMAGE={ray_image}",
                        "-t",
                        byod_image,
                        "-",
                    ],
                    stdin=build_file,
                    stdout=sys.stderr,
                    env=env,
                )
                subprocess.check_call(
                    [
                        "docker",
                        "build",
                        "--build-arg",
                        f"BASE_IMAGE={byod_image}",
                        "--build-arg",
                        "PIP_REQUIREMENTS=requirements_byod.txt",
                        "--build-arg",
                        "DEBIAN_REQUIREMENTS=requirements_debian_byod.txt",
                        "-t",
                        byod_image,
                        "-f",
                        os.path.join(RELEASE_BYOD_DIR, "byod.Dockerfile"),
                        RELEASE_BYOD_DIR,
                    ],
                    stdout=sys.stderr,
                    env=env,
                )
                subprocess.check_call(
                    ["docker", "push", byod_image],
                    stdout=sys.stderr,
                )
                built.add(ray_image)


def _download_dataplane_build_file() -> None:
    """
    Downloads the dataplane build file from S3.
    """
    s3 = boto3.client("s3")
    s3.download_file(
        Bucket=DATAPLANE_S3_BUCKET,
        Key=DATAPLANE_FILENAME,
        Filename=DATAPLANE_FILENAME,
    )
    with open(DATAPLANE_FILENAME, "rb") as build_context:
        digest = hashlib.sha256(build_context.read()).hexdigest()
        assert digest == DATAPLANE_DIGEST, "Mismatched dataplane digest found!"


def _ray_image_exist(ray_image: str) -> bool:
    """
    Checks if the given image exists in Docker
    """
    p = subprocess.run(
        ["docker", "manifest", "inspect", ray_image],
        stdout=sys.stderr,
        stderr=sys.stderr,
    )
    return p.returncode == 0


def _byod_image_exist(test: Test) -> bool:
    """
    Checks if the given Anyscale BYOD image exists.
    """
    client = boto3.client("ecr")
    try:
        client.describe_images(
            repositoryName=test.get_byod_repo(),
            imageIds=[{"imageTag": test.get_byod_image_tag()}],
        )
        return True
    except client.exceptions.ImageNotFoundException:
        return False
