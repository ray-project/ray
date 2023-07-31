from typing import List

import boto3
import hashlib
import os
import subprocess
import sys
import time

from ray_release.byod.build_ray import build_ray
from ray_release.config import RELEASE_PACKAGE_DIR
from ray_release.configs.global_config import get_global_config
from ray_release.logger import logger
from ray_release.test import (
    Test,
    DATAPLANE_ECR_REPO,
    DATAPLANE_ECR_ML_REPO,
)

DATAPLANE_S3_BUCKET = "ray-release-automation-results"
DATAPLANE_FILENAME = "dataplane_20230718.tgz"
DATAPLANE_DIGEST = "a3ad426b05f5cf1981fe684ccbffc1dded5e1071a99184d1072b7fc7b4daf8bc"
BASE_IMAGE_WAIT_TIMEOUT = 7200
BASE_IMAGE_WAIT_DURATION = 30
RELEASE_BYOD_DIR = os.path.join(RELEASE_PACKAGE_DIR, "ray_release/byod")
REQUIREMENTS_BYOD = "requirements_byod"
REQUIREMENTS_ML_BYOD = "requirements_ml_byod"
PYTHON_VERSION = "3.8"


def build_champagne_image(
    ray_version: str,
    python_version: str,
    image_type: str,
) -> str:
    """
    Builds the Anyscale champagne image.
    """
    _download_dataplane_build_file()
    env = os.environ.copy()
    env["DOCKER_BUILDKIT"] = "1"
    if image_type == "cpu":
        ray_project = "ray"
        anyscale_repo = DATAPLANE_ECR_REPO
    else:
        ray_project = "ray-ml"
        anyscale_repo = DATAPLANE_ECR_ML_REPO
    ray_image = f"rayproject/{ray_project}:{ray_version}-{python_version}-{image_type}"
    anyscale_image = (
        f"{get_global_config()['byod_ecr']}/{anyscale_repo}:champagne-{ray_version}"
    )

    logger.info(f"Building champagne anyscale image from {ray_image}")
    with open(DATAPLANE_FILENAME, "rb") as build_file:
        subprocess.check_call(
            [
                "docker",
                "build",
                "--build-arg",
                f"BASE_IMAGE={ray_image}",
                "-t",
                anyscale_image,
                "-",
            ],
            stdin=build_file,
            stdout=sys.stderr,
            env=env,
        )
    _validate_and_push(anyscale_image)

    return anyscale_image


def build_anyscale_custom_byod_image(test: Test) -> None:
    if not test.require_custom_byod_image():
        logger.info(f"Test {test.get_name()} does not require a custom byod image")
        return
    byod_image = test.get_anyscale_byod_image()
    if _byod_image_exist(test, base_image=False):
        logger.info(f"Image {byod_image} already exists")
        return

    env = os.environ.copy()
    env["DOCKER_BUILDKIT"] = "1"
    subprocess.check_call(
        [
            "docker",
            "build",
            "--build-arg",
            f"BASE_IMAGE={test.get_anyscale_base_byod_image()}",
            "--build-arg",
            f"POST_BUILD_SCRIPT={test.get_byod_post_build_script()}",
            "-t",
            byod_image,
            "-f",
            os.path.join(RELEASE_BYOD_DIR, "byod.custom.Dockerfile"),
            RELEASE_BYOD_DIR,
        ],
        stdout=sys.stderr,
        env=env,
    )
    _validate_and_push(byod_image)


def build_anyscale_base_byod_images(tests: List[Test]) -> None:
    """
    Builds the Anyscale BYOD images for the given tests.
    """
    build_ray()
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
            byod_image = test.get_anyscale_base_byod_image()
            byod_requirements = (
                f"{REQUIREMENTS_BYOD}_{test.get('python', PYTHON_VERSION)}.txt"
                if test.get_byod_type() == "cpu"
                else f"{REQUIREMENTS_ML_BYOD}_{test.get('python', PYTHON_VERSION)}.txt"
            )
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
                        f"PIP_REQUIREMENTS={byod_requirements}",
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
                _validate_and_push(byod_image)
                built.add(ray_image)


def _validate_and_push(byod_image: str) -> None:
    """
    Validates the given image and pushes it to ECR.
    """
    docker_ray_commit = (
        subprocess.check_output(
            [
                "docker",
                "run",
                "-ti",
                "--entrypoint",
                "python",
                byod_image,
                "-c",
                "import ray; print(ray.__commit__)",
            ],
        )
        .decode("utf-8")
        .strip()
    )
    expected_ray_commit = _get_ray_commit()
    assert (
        docker_ray_commit == expected_ray_commit
    ), f"Expected ray commit {expected_ray_commit}, found {docker_ray_commit}"
    subprocess.check_call(
        ["docker", "push", byod_image],
        stdout=sys.stderr,
    )


def _get_ray_commit() -> str:
    return os.environ.get(
        "COMMIT_TO_TEST",
        os.environ["BUILDKITE_COMMIT"],
    )


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


def _byod_image_exist(test: Test, base_image: bool = True) -> bool:
    """
    Checks if the given Anyscale BYOD image exists.
    """
    if os.environ.get("BYOD_NO_CACHE", False):
        return False
    client = boto3.client("ecr", region_name="us-west-2")
    image_tag = (
        test.get_byod_base_image_tag() if base_image else test.get_byod_image_tag()
    )
    try:
        client.describe_images(
            repositoryName=test.get_byod_repo(),
            imageIds=[{"imageTag": image_tag}],
        )
        return True
    except client.exceptions.ImageNotFoundException:
        return False
