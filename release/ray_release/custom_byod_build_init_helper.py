from typing import List, Tuple
import yaml
from ray_release.configs.global_config import get_global_config
from ray_release.logger import logger
from ray_release.test import Test
import hashlib


def generate_custom_build_step_key(image: str) -> str:
    image_repository, tag = image.split(":")
    tag_variants = tag.split("-")
    # Remove build id from the tag name to make hash consistent
    image_name_without_id = f"{image_repository}:{'-'.join(tag_variants[1:])}"
    logger.info(f"Image: {image_name_without_id}")
    result = hashlib.sha256(image_name_without_id.encode()).hexdigest()[:20]
    logger.info(f"Result: {result}")
    return result


def get_images_from_tests(
    tests: List[Test], build_id: str
) -> List[Tuple[str, str, str]]:
    """Get a list of custom BYOD images to build from a list of tests."""
    custom_byod_images = set()
    for test in tests:
        if not test.require_custom_byod_image():
            continue
        custom_byod_image_build = (
            test.get_anyscale_byod_image(build_id),
            test.get_anyscale_base_byod_image(build_id),
            test.get_byod_post_build_script(),
        )
        logger.info(f"To be built: {custom_byod_image_build[0]}")
        custom_byod_images.add(custom_byod_image_build)
    return list(custom_byod_images)


def create_custom_build_yaml(destination_file: str, tests: List[Test]) -> None:
    """Create a yaml file for building custom BYOD images"""

    config = get_global_config()
    if not config or not config.get("byod_ecr_region") or not config.get("byod_ecr"):
        raise ValueError("byod_ecr_region and byod_ecr must be set in the config")
    custom_byod_images = get_images_from_tests(tests, "$$RAYCI_BUILD_ID")
    if not custom_byod_images:
        return
    build_config = {"group": "Custom images build", "steps": []}

    for image, base_image, post_build_script in custom_byod_images:
        logger.info(
            f"Building custom BYOD image: {image}, base image: {base_image}, post build script: {post_build_script}"
        )
        if not post_build_script:
            continue
        step = {
            "label": f":tapioca: build custom: {image}",
            "key": generate_custom_build_step_key(image),
            "instance_type": "release-medium",
            "commands": [
                "bash release/gcloud_docker_login.sh release/aws2gce_iam.json",
                "export PATH=$(pwd)/google-cloud-sdk/bin:$$PATH",
                f"aws ecr get-login-password --region {config['byod_ecr_region']} | docker login --username AWS --password-stdin {config['byod_ecr']}",
                f"bazelisk run //release:custom_byod_build -- --image-name {image} --base-image {base_image} --post-build-script {post_build_script}",
            ],
        }
        step["depends_on"] = get_prerequisite_step(image)
        build_config["steps"].append(step)

    logger.info(f"Build config: {build_config}")
    with open(destination_file, "w") as f:
        yaml.dump(build_config, f, default_flow_style=False, sort_keys=False)


def get_prerequisite_step(image: str) -> str:
    """Get the base image build step for a job that depends on it."""
    config = get_global_config()
    image_repository, _ = image.split(":")
    image_name = image_repository.split("/")[-1]
    if image_name == "ray-ml":
        return config["release_image_step_ray_ml"]
    elif image_name == "ray-llm":
        return config["release_image_step_ray_llm"]
    else:
        return config["release_image_step_ray"]
