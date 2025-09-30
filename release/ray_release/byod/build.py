from typing import List, Optional, Dict

import os
import subprocess
import sys

from ray_release.config import RELEASE_PACKAGE_DIR
from ray_release.logger import logger
from ray_release.test import (
    Test,
)

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")

RELEASE_BYOD_DIR = (
    os.path.join(bazel_workspace_dir, "release/ray_release/byod")
    if bazel_workspace_dir
    else os.path.join(RELEASE_PACKAGE_DIR, "ray_release/byod")
)


def build_anyscale_custom_byod_image(
    image: str,
    base_image: str,
    post_build_script: str,
    python_depset: Optional[str] = None,
) -> None:
    if _image_exist(image):
        logger.info(f"Image {image} already exists")
        return

    env = os.environ.copy()
    env["DOCKER_BUILDKIT"] = "1"
    docker_build_cmd = [
        "docker",
        "build",
        "--progress=plain",
        "--build-arg",
        f"BASE_IMAGE={base_image}",
        "--build-arg",
        f"POST_BUILD_SCRIPT={post_build_script}",
    ]
    if python_depset:
        docker_build_cmd.extend(["--build-arg", f"PYTHON_DEPSET={python_depset}"])
    docker_build_cmd.extend(
        [
            "-t",
            image,
            "-f",
            os.path.join(RELEASE_BYOD_DIR, "byod.custom.Dockerfile"),
            RELEASE_BYOD_DIR,
        ]
    )
    subprocess.check_call(
        docker_build_cmd,
        stdout=sys.stderr,
        env=env,
    )
    _validate_and_push(image)


def build_anyscale_base_byod_images(tests: List[Test]) -> List[str]:
    """
    Builds the Anyscale BYOD images for the given tests.
    """
    images = set()
    for test in tests:
        images.add(test.get_anyscale_base_byod_image())

    image_list = list(images)
    image_list.sort()

    for image in image_list:
        if not _image_exist(image):
            raise RuntimeError(f"Image {image} not found")

    return image_list


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
    if os.environ.get("RAY_IMAGE_TAG"):
        logger.info(f"Ray commit from image: {docker_ray_commit}")
    else:
        expected_ray_commit = _get_ray_commit()
        assert (
            docker_ray_commit == expected_ray_commit
        ), f"Expected ray commit {expected_ray_commit}, found {docker_ray_commit}"
    logger.info(f"Pushing image to registry: {byod_image}")
    subprocess.check_call(
        ["docker", "push", byod_image],
        stdout=sys.stderr,
    )


def _get_ray_commit(envs: Optional[Dict[str, str]] = None) -> str:
    if envs is None:
        envs = os.environ
    for key in [
        "RAY_WANT_COMMIT_IN_IMAGE",
        "COMMIT_TO_TEST",
        "BUILDKITE_COMMIT",
    ]:
        commit = envs.get(key, "")
        if commit:
            return commit
    return ""


def _image_exist(image: str) -> bool:
    """
    Checks if the given image exists in Docker
    """
    p = subprocess.run(
        ["docker", "manifest", "inspect", image],
        stdout=sys.stderr,
        stderr=sys.stderr,
    )
    return p.returncode == 0
