import click

from ci.ray_ci.builder_container import PYTHON_VERSIONS, BuilderContainer
from ci.ray_ci.forge_container import ForgeContainer
from ci.ray_ci.docker_container import PLATFORM, DockerContainer
from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.utils import logger, docker_login


@click.command()
@click.argument(
    "artifact_type",
    required=True,
    type=click.Choice(["wheel", "docker"]),
)
@click.option(
    "--python-version",
    default="py38",
    type=click.Choice(list(PYTHON_VERSIONS.keys())),
    help=("Python version to build the wheel with"),
)
@click.option(
    "--platform",
    default="cu118",
    type=click.Choice(list(PLATFORM)),
    help=("Platform to build the docker with"),
)
@click.option(
    "--image-type",
    default="ray",
    type=click.Choice(["ray", "ray-ml"]),
)
def main(
    artifact_type: str,
    python_version: str,
    platform: str,
    image_type: str,
) -> None:
    """
    Build a wheel or jar artifact
    """
    docker_login(_DOCKER_ECR_REPO.split("/")[0])
    if artifact_type == "wheel":
        logger.info(f"Building wheel for {python_version}")
        build_wheel(python_version)
        return

    if artifact_type == "docker":
        logger.info(f"Building {image_type} docker for {python_version} on {platform}")
        build_docker(python_version, platform, image_type)
        return

    raise ValueError(f"Invalid artifact type {artifact_type}")


def build_wheel(python_version: str) -> None:
    """
    Build a wheel artifact
    """
    BuilderContainer(python_version).run()
    ForgeContainer().upload_wheel()


def build_docker(python_version: str, platform: str, image_type: str) -> None:
    """
    Build a container artifact
    """
    BuilderContainer(python_version).run()
    DockerContainer(python_version, platform, image_type).run()
