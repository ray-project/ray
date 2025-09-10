from typing import List, Tuple
import yaml
from ray_release.configs.global_config import get_global_config
from ray_release.logger import logger
from ray_release.test import Test


def _generate_custom_build_step_key(image: str) -> str:
    # Buildkite step key cannot contain special characters, so they need to be replaced.
    # Buildkite also limits step key length to 80 characters.
    return (
        "custom_build_"
        + image.replace("/", "_")
        .replace(":", "_")
        .replace(".", "_")
        .replace("-", "_")[-40:]
    )


def get_images_from_tests(tests: List[Test]) -> List[Tuple[str, str, str]]:
    """Get a list of custom BYOD images to build from a list of tests."""
    custom_byod_images = set()
    for test in tests:
        if not test.require_custom_byod_image():
            continue
        custom_byod_image_build = (
            test.get_anyscale_byod_image(),
            test.get_anyscale_base_byod_image(),
            test.get_byod_post_build_script(),
        )
        logger.info(f"To be built: {custom_byod_image_build[0]}")
        custom_byod_images.add(custom_byod_image_build)
    return list(custom_byod_images)


def create_custom_build_yaml(destination_file: str, tests: List[Test]) -> None:
    config = get_global_config()
    if not config or not config.get("byod_ecr_region") or not config.get("byod_ecr"):
        raise ValueError("byod_ecr_region and byod_ecr must be set in the config")
    """Create a yaml file for building custom BYOD images"""
    custom_byod_images = get_images_from_tests(tests)
    if not custom_byod_images:
        return
    build_config = {"group": "Custom images build", "steps": []}

    for image, base_image, post_build_script in custom_byod_images:
        if not post_build_script:
            continue
        step = {
            "label": f":tapioca: build custom: {image}",
            "key": _generate_custom_build_step_key(image),
            "instance_type": "release-medium",
            "commands": [
                f"aws ecr get-login-password --region {config['byod_ecr_region']} | docker login --username AWS --password-stdin {config['byod_ecr']}",
                f"bazelisk run //release:custom_byod_build -- --image-name {image} --base-image {base_image} --post-build-script {post_build_script}",
            ],
        }
        if "ray-ml" in image:
            step["depends_on"] = "anyscalemlbuild"
        elif "ray-llm" in image:
            step["depends_on"] = "anyscalellmbuild"
        else:
            step["depends_on"] = "anyscalebuild"
        build_config["steps"].append(step)

    with open(destination_file, "w") as f:
        yaml.dump(build_config, f, default_flow_style=False, sort_keys=False)
