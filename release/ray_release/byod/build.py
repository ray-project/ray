from typing import List

import boto3
import subprocess

from ray_release.logger import logger
from ray_release.test import (
    Test,
    S3_BUCKET,
    DATAPLANE_FILENAME,
)


def build_anyscale_byod_images(tests: List[Test]) -> None:
    """
    Builds the Anyscale BYOD images for the given tests.
    """
    s3 = boto3.client("s3")
    s3.download_file(
        Bucket=S3_BUCKET,
        Key=DATAPLANE_FILENAME,
        Filename=DATAPLANE_FILENAME,
    )
    built = set()
    for test, _ in tests:
        if not test.is_byod_cluster():
            continue
        ray_image = test.get_ray_image()
        if ray_image in built:
            continue
        byod_image = test.get_anyscale_byod_image()
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
                stdout=subprocess.DEVNULL,
                env={"DOCKER_BUILDKIT": "1"},
            )
            subprocess.check_call(
                ["docker", "push", byod_image],
                stdout=subprocess.DEVNULL,
            )
            built.add(ray_image)
    return
