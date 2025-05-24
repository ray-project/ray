import click

from ci.ray_ci.automation.docker_tags_lib import list_image_tags, generate_index
from ci.ray_ci.docker_container import (
    RayType,
    PLATFORMS_RAY,
    PYTHON_VERSIONS_RAY,
    ARCHITECTURES_RAY,
)


@click.command()
@click.option("--prefix", required=True, type=str)
def main(prefix):
    tags = list_image_tags(
        prefix, RayType.RAY, PYTHON_VERSIONS_RAY, PLATFORMS_RAY, ARCHITECTURES_RAY
    )
    indexes_to_publish = []
    for tag in tags:
        if "-aarch64" not in tag and tag + "-aarch64" in tags:
            indexes_to_publish.append((tag, tag + "-aarch64"))

    for tags in indexes_to_publish:
        generate_index(index_name=tags[0], tags=tags)


if __name__ == "__main__":
    main()
