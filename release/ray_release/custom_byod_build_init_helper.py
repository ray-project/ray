from typing import List, Tuple
import yaml
from ray_release.configs.global_config import get_global_config


def _generate_custom_build_step_key(image: str) -> str:
    # Buildkite step key cannot contain special characters, so they need to be replaced.
    # Buildkite also limits step key length to 80 characters.
    return "custom_build_" + image.replace("/", "_").replace(":", "_").replace(".", "_").replace("-", "_")[-40:]

def create_custom_build_yaml(
    destination_file: str, custom_byod_images: List[Tuple[str, str, str]]
) -> None:
    config = get_global_config()
    """Create a yaml file for building custom BYOD images."""
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
