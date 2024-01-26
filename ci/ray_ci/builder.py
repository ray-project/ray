from typing import List

import click

from ci.ray_ci.builder_container import (
    DEFAULT_PYTHON_VERSION,
    PYTHON_VERSIONS,
    BUILD_TYPES,
    ARCHITECTURE,
    BuilderContainer,
)
from ci.ray_ci.forge_container import ForgeContainer
from ci.ray_ci.docker_container import PLATFORM
from ci.ray_ci.ray_docker_container import RayDockerContainer
from ci.ray_ci.anyscale_docker_container import AnyscaleDockerContainer
from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.utils import logger, docker_login


@click.command()
@click.argument(
    "artifact_type",
    required=True,
    type=click.Choice(["wheel", "docker", "anyscale"]),
)
@click.option(
    "--image-type",
    default="ray",
    type=click.Choice(["ray", "ray-ml"]),
)
@click.option(
    "--build-type",
    default="optimized",
    type=click.Choice(BUILD_TYPES),
)
@click.option(
    "--python-version",
    default=DEFAULT_PYTHON_VERSION,
    type=click.Choice(list(PYTHON_VERSIONS.keys())),
    help=("Python version to build the wheel with"),
)
@click.option(
    "--platform",
    multiple=True,
    type=click.Choice(list(PLATFORM)),
    help=("Platform to build the docker with"),
)
@click.option(
    "--architecture",
    default="x86_64",
    type=click.Choice(list(ARCHITECTURE)),
    help=("Platform to build the docker with"),
)
@click.option(
    "--canonical-tag",
    default=None,
    type=str,
    help=("Tag to use for the docker image"),
)
@click.option(
    "--upload",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Upload the build artifacts"),
)
def main(
    artifact_type: str,
    image_type: str,
    build_type: str,
    python_version: str,
    platform: List[str],
    architecture: str,
    canonical_tag: str,
    upload: bool,
) -> None:
    """
    Build a wheel or jar artifact
    """
    docker_login(_DOCKER_ECR_REPO.split("/")[0])
    if artifact_type == "wheel":
        logger.info(f"Building wheel for {python_version}")
        build_wheel(python_version, build_type, architecture)
        return

    if artifact_type == "docker":
        logger.info(f"Building {image_type} docker for {python_version} on {platform}")
        build_docker(
            image_type,
            python_version,
            build_type,
            platform,
            architecture,
            canonical_tag,
            upload,
        )
        return

    if artifact_type == "anyscale":
        logger.info(
            f"Building {image_type} anyscale for {python_version} on {platform}"
        )
        build_anyscale(
            image_type,
            python_version,
            build_type,
            platform,
            architecture,
            canonical_tag,
            upload,
        )
        return

    raise ValueError(f"Invalid artifact type {artifact_type}")


def build_wheel(python_version: str, build_type: str, architecture: str) -> None:
    """
    Build a wheel artifact.
    """
    BuilderContainer(python_version, build_type, architecture).run()
    ForgeContainer(architecture).upload_wheel()


def build_docker(
    image_type: str,
    python_version: str,
    build_type: str,
    platform: List[str],
    architecture: str,
    canonical_tag: str,
    upload: bool,
) -> None:
    """
    Build a container artifact.
    """
    BuilderContainer(python_version, build_type, architecture).run()
    for p in platform:
        RayDockerContainer(
            python_version, p, image_type, architecture, canonical_tag, upload
        ).run()


def build_anyscale(
    image_type: str,
    python_version: str,
    build_type: str,
    platform: List[str],
    architecture: str,
    canonical_tag: str,
    upload: bool,
) -> None:
    """
    Build an anyscale container artifact.
    """
    BuilderContainer(python_version, build_type, architecture).run()
    for p in platform:
        RayDockerContainer(
            python_version, p, image_type, architecture, canonical_tag, upload=False
        ).run()
        AnyscaleDockerContainer(
            python_version, p, image_type, architecture, canonical_tag, upload
        ).run()
