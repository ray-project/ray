from typing import List

import boto3
import hashlib
import subprocess
import sys
import time

from ray_release.logger import logger
from ray_release.test import (
    Test,
    DATAPLANE_ECR_REPO,
)

DATAPLANE_S3_BUCKET = "ray-release-automation-results"
DATAPLANE_FILENAME = "dataplane.tgz"
DATAPLANE_DIGEST = "f9b0055085690ddad2faa804bb6b38addbcf345b9166f2204928a7ece1c8a39b"


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
        to_be_built[test.get_ray_image()] = test.get_anyscale_byod_image()

    timeout = 0
    # ray images are built on post-merge, so we can wait for them to be available
    while len(built) < len(to_be_built) and timeout < 7200:
        for ray_image, byod_image in to_be_built.items():
            if _byod_image_exist(byod_image.replace(f"{DATAPLANE_ECR_REPO}:", "")):
                logger.info(f"Image {byod_image} already exists")
                built.add(ray_image)
                continue
            logger.info(f"Building {byod_image} from {ray_image}")
            with open(DATAPLANE_FILENAME, "rb") as build_file:
                try:
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
                        stdout=subprocess.DEVNULL,
                        env={"DOCKER_BUILDKIT": "1"},
                    )
                except subprocess.CalledProcessError:
                    # If the ray image does not exist yet, we will retry later
                    logger.info(f"Retry for another {7200 - timeout}s ...")
                    time.sleep(30)
                    timeout += 30
                    continue
                subprocess.check_call(
                    ["docker", "push", byod_image],
                    stdout=subprocess.DEVNULL,
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


def _byod_image_exist(image_tag: str) -> bool:
    """
    Checks if the given Anyscale BYOD image exists.
    """
    client = boto3.client("ecr")
    try:
        client.describe_images(
            repositoryName="anyscale",
            imageIds=[{"imageTag": image_tag}],
        )
        return True
    except client.exceptions.ImageNotFoundException:
        return False
