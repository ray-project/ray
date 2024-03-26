import click
import sys

from ci.ray_ci.automation.docker_tags_lib import list_image_tags, get_ray_commit
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

    for tag in tags:
        commit = get_ray_commit(f"rayproject/{ray_type.value}:{tag}")
        if commit != expected_commit:
            print(f"Ray commit mismatch for tag {tag}!")
            print("Expected:", expected_commit)
            print("Actual:", commit)
            sys.exit(42)  # Not retrying the check on Buildkite jobs
        else:
            print(f"Commit {commit} match for tag {tag}!")


if __name__ == "__main__":
    main()
