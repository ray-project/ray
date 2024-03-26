import click
import sys

from ci.ray_ci.utils import logger
from ci.ray_ci.automation.docker_tags_lib import (
    list_image_tags,
    get_ray_commit,
    pull_image,
    remove_image,
)
from ci.ray_ci.docker_container import (
    PLATFORMS_RAY,
    PLATFORMS_RAY_ML,
    PYTHON_VERSIONS_RAY,
    PYTHON_VERSIONS_RAY_ML,
    ARCHITECTURES_RAY,
    ARCHITECTURES_RAY_ML,
    RayType,
)


@click.command()
@click.option("--ray_type", required=True, type=RayType)
@click.option("--expected_commit", required=True, type=str)
def main(ray_type, expected_commit):
    if ray_type == RayType.RAY:
        tags = list_image_tags(
            "nightly", ray_type, PYTHON_VERSIONS_RAY, PLATFORMS_RAY, ARCHITECTURES_RAY
        )
    elif ray_type == RayType.RAY_ML:
        tags = list_image_tags(
            "nightly",
            ray_type,
            PYTHON_VERSIONS_RAY_ML,
            PLATFORMS_RAY_ML,
            ARCHITECTURES_RAY_ML,
        )
    tags = [f"rayproject/{ray_type.value}:{tag}" for tag in tags]

    for i, tag in enumerate(tags):
        logger.info(f"{i+1}/{len(tags)} Checking commit for tag {tag} ....")

        pull_image(tag)
        commit = get_ray_commit(tag)

        if commit != expected_commit:
            print(f"Ray commit mismatch for tag {tag}!")
            print("Expected:", expected_commit)
            print("Actual:", commit)
            sys.exit(42)  # Not retrying the check on Buildkite jobs
        else:
            print(f"Commit {commit} match for tag {tag}!")

        if i != 0:  # Only save first pulled image for caching
            remove_image(tag)


if __name__ == "__main__":
    main()
