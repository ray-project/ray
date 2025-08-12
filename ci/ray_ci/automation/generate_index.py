import click

from ci.ray_ci.automation.docker_tags_lib import generate_index, list_image_tags
from ci.ray_ci.docker_container import (
    ARCHITECTURES_RAY,
    PLATFORMS_RAY,
    PYTHON_VERSIONS_RAY,
    RayType,
)


@click.command()
@click.option("--prefix", required=True, type=str)
def main(prefix):
    tags = list_image_tags(
        prefix, RayType.RAY, PYTHON_VERSIONS_RAY, PLATFORMS_RAY, ARCHITECTURES_RAY
    )
    tags = [f"rayproject/ray:{tag}" for tag in tags]
    indexes_to_publish = []
    for tag in tags:
        if not tag.endswith("-aarch64") and tag + "-aarch64" in tags:
            indexes_to_publish.append((tag, tag + "-aarch64"))

    for tags in indexes_to_publish:
        generate_index(index_name=tags[0], tags=tags)


if __name__ == "__main__":
    main()
