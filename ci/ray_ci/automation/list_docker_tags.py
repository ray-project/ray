import sys

import click

from ci.ray_ci.automation.docker_tags_lib import list_image_tags
from ci.ray_ci.docker_container import (
    ARCHITECTURES_RAY,
    ARCHITECTURES_RAY_ML,
    PLATFORMS_RAY,
    PLATFORMS_RAY_ML,
    PYTHON_VERSIONS_RAY,
    PYTHON_VERSIONS_RAY_ML,
    RayType,
)


@click.command()
@click.option("--prefix", required=True, type=str)
@click.option("--ray_type", required=True, type=RayType)
def main(prefix, ray_type):
    if ray_type == RayType.RAY:
        tags = list_image_tags(
            prefix, ray_type, PYTHON_VERSIONS_RAY, PLATFORMS_RAY, ARCHITECTURES_RAY
        )
    elif ray_type == RayType.RAY_ML:
        tags = list_image_tags(
            prefix,
            ray_type,
            PYTHON_VERSIONS_RAY_ML,
            PLATFORMS_RAY_ML,
            ARCHITECTURES_RAY_ML,
        )
    for tag in tags:
        sys.stdout.write(tag + "\n")


if __name__ == "__main__":
    main()
