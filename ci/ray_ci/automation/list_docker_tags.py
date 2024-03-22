import click
import sys

from ci.ray_ci.automation.docker_tags_lib import list_image_tags
from ci.ray_ci.docker_container import (
    PLATFORMS_RAY,
    PLATFORMS_RAY_ML,
    PYTHON_VERSIONS_RAY,
    PYTHON_VERSIONS_RAY_ML,
    ARCHITECTURES_RAY,
    ARCHITECTURES_RAY_ML,
)


@click.command()
@click.option("--prefix", required=True, type=str)
@click.option("--ray_type", required=True, type=click.Choice(["ray", "ray-ml"]))
def main(prefix, ray_type):
    if ray_type == "ray":
        tags = list_image_tags(
            prefix, ray_type, PYTHON_VERSIONS_RAY, PLATFORMS_RAY, ARCHITECTURES_RAY
        )
    elif ray_type == "ray-ml":
        tags = list_image_tags(
            prefix,
            ray_type,
            PYTHON_VERSIONS_RAY_ML,
            PLATFORMS_RAY_ML,
            ARCHITECTURES_RAY_ML,
        )
    output = sys.stdout
    for tag in tags:
        output.write(tag + "\n")


if __name__ == "__main__":
    main()
