import click

from ci.ray_ci.automation.docker_tags_lib import generate_index, list_image_tags
from ci.ray_ci.docker_container import (
    ARCHITECTURES_RAY,
    ARCHITECTURES_RAY_LLM,
    PLATFORMS_RAY,
    PLATFORMS_RAY_LLM,
    PYTHON_VERSIONS_RAY,
    PYTHON_VERSIONS_RAY_LLM,
    RayType,
)

INDEX_IMAGE_CONFIGS = {
    RayType.RAY: (
        PYTHON_VERSIONS_RAY,
        PLATFORMS_RAY,
        ARCHITECTURES_RAY,
    ),
    RayType.RAY_LLM: (
        PYTHON_VERSIONS_RAY_LLM,
        PLATFORMS_RAY_LLM,
        ARCHITECTURES_RAY_LLM,
    ),
}


@click.command()
@click.option("--prefix", required=True, type=str)
@click.option(
    "--image-type",
    type=click.Choice([image_type.value for image_type in INDEX_IMAGE_CONFIGS]),
    default=RayType.RAY.value,
    show_default=True,
)
def main(prefix, image_type):
    ray_type = RayType(image_type)
    python_versions, platforms, architectures = INDEX_IMAGE_CONFIGS[ray_type]
    tags = list_image_tags(prefix, ray_type, python_versions, platforms, architectures)
    tags = [f"rayproject/{ray_type.value}:{tag}" for tag in tags]
    indexes_to_publish = []
    for tag in tags:
        if not tag.endswith("-aarch64") and tag + "-aarch64" in tags:
            indexes_to_publish.append((tag, tag + "-aarch64"))

    for tags in indexes_to_publish:
        generate_index(index_name=tags[0], tags=tags)


if __name__ == "__main__":
    main()
