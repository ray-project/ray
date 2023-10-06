import click

from ci.ray_ci.builder_container import PYTHON_VERSIONS, BuilderContainer
from ci.ray_ci.doc_builder_container import DocBuilderContainer
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
    type=click.Choice(["wheel", "doc", "docker", "anyscale"]),
)
@click.option(
    "--python-version",
    default="3.8",
    type=click.Choice(list(PYTHON_VERSIONS.keys())),
    help=("Python version to build the wheel with"),
)
@click.option(
    "--platform",
    default="cu11.8.0",
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

    if artifact_type == "anyscale":
        logger.info(
            f"Building {image_type} anyscale for {python_version} on {platform}"
        )
        build_anyscale(python_version, platform, image_type)
        return

    if artifact_type == "doc":
        logger.info("Building ray docs")
        build_doc()
        return

    raise ValueError(f"Invalid artifact type {artifact_type}")


def build_wheel(python_version: str) -> None:
    """
    Build a wheel artifact.
    """
    BuilderContainer(python_version).run()
    ForgeContainer().upload_wheel()


def build_docker(python_version: str, platform: str, image_type: str) -> None:
    """
    Build a container artifact.
    """
    BuilderContainer(python_version).run()
    RayDockerContainer(python_version, platform, image_type).run()


def build_anyscale(python_version: str, platform: str, image_type: str) -> None:
    """
    Build an anyscale container artifact.
    """
    BuilderContainer(python_version).run()
    RayDockerContainer(python_version, platform, image_type).run()
    AnyscaleDockerContainer(python_version, platform, image_type).run()


def build_doc() -> None:
    """
    Build a doc artifact.
    """
    DocBuilderContainer().run()
