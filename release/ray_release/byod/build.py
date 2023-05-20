from typing import List

import boto3
import hashlib
import subprocess
import sys

from ray_release.logger import logger
from ray_release.test import Test

DATAPLANE_S3_BUCKET = "ray-release-automation-results"
DATAPLANE_FILENAME = "dataplane.tgz"
DATAPLANE_DIGEST = "f9b0055085690ddad2faa804bb6b38addbcf345b9166f2204928a7ece1c8a39b"


def build_anyscale_byod_images(tests: List[Test]) -> None:
    """
    Builds the Anyscale BYOD images for the given tests.
    """
    _download_dataplane_build_file()
    built = set()
    for test in tests:
        if not test.is_byod_cluster():
            continue
        ray_image = test.get_ray_image()
        if ray_image in built:
            continue
        byod_image = test.get_anyscale_byod_image()
        logger.info(f"Building {byod_image} from {ray_image}")
        with open(DATAPLANE_FILENAME, "rb") as build_context:
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
                stdin=build_context,
                stdout=sys.stderr,
                env={"DOCKER_BUILDKIT": "1"},
            )
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
