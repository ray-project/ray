import hashlib
import json
import os
import re
from typing import Dict, List, Optional, Set, Tuple

import yaml

from ray_release.configs.global_config import get_global_config
from ray_release.logger import logger
from ray_release.test import Test
from ray_release.util import ANYSCALE_RAY_IMAGE_PREFIX, AZURE_REGISTRY_NAME


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
) -> Tuple[
    List[Tuple[str, str, Optional[str], Optional[str], Optional[Dict[str, str]]]],
    Dict[str, List[str]],
]:
    """Get a list of custom BYOD images to build from a list of tests."""
    custom_byod_images = {}
    custom_image_test_names_map = {}
    for test in tests:
        if not test.require_custom_byod_image():
            continue
        image_tag = test.get_anyscale_byod_image(build_id)
        if image_tag not in custom_byod_images:
            runtime_env = test.get_byod_runtime_env() or None
            custom_byod_images[image_tag] = (
                image_tag,
                test.get_anyscale_base_byod_image(build_id),
                test.get_byod_post_build_script(),
                test.get_byod_python_depset(),
                runtime_env,
            )
        logger.info(f"To be built: {image_tag}")
        if image_tag not in custom_image_test_names_map:
            custom_image_test_names_map[image_tag] = []
        custom_image_test_names_map[image_tag].append(test.get_name())
    return list(custom_byod_images.values()), custom_image_test_names_map


def create_custom_build_yaml(
    destination_file: str, tests: List[Test], gpu_map: Dict[str, str]
) -> None:
    """Create a yaml file for building custom BYOD images"""
    config = get_global_config()
    if (
        not config
        or not config.get("byod_ecr_region")
        or not config.get("byod_ecr")
        or not config.get("aws2gce_credentials")
    ):
        raise ValueError(
            "byod_ecr_region, byod_ecr, and aws2gce_credentials must be set in the config"
        )
    byod_ecr_region = config.get("byod_ecr_region")
    byod_ecr = config.get("byod_ecr")
    aws2gce_credentials = config.get("aws2gce_credentials")
    custom_byod_images, custom_image_test_names_map = get_images_from_tests(
        tests, "$$RAYCI_BUILD_ID"
    )
    if not custom_byod_images:
        return
    build_config = {"group": "Custom images build", "steps": []}
    ray_want_commit = os.getenv("RAY_WANT_COMMIT_IN_IMAGE", "")
    for (
        image,
        base_image,
        post_build_script,
        python_depset,
        runtime_env,
    ) in custom_byod_images:
        logger.info(
            f"Building custom BYOD image: {image}, base image: {base_image}, "
            f"post build script: {post_build_script}, runtime_env: {runtime_env}"
        )
        if not post_build_script and not python_depset and not runtime_env:
            continue
        step_key = generate_custom_build_step_key(image)
        step_name = _get_step_name(image, step_key, custom_image_test_names_map[image])
        env_args = ""
        if runtime_env:
            env_args = " ".join(
                f"--env {k}={v}" for k, v in sorted(runtime_env.items())
            )
        build_cmd_parts = [
            "bazelisk run //release:custom_byod_build --",
            f"--image-name {image}",
            f"--base-image {base_image}",
        ]
        if post_build_script:
            build_cmd_parts.append(f"--post-build-script {post_build_script}")
        if python_depset:
            build_cmd_parts.append(f"--python-depset {python_depset}")
        if env_args:
            build_cmd_parts.append(env_args)
        build_cmd = " ".join(build_cmd_parts)
        step = {
            "label": step_name,
            "key": step_key,
            "instance_type": "release-medium",
            "mount_buildkite_agent": True,
            "commands": [
                f"export RAY_WANT_COMMIT_IN_IMAGE={ray_want_commit}",
                f"bash release/gcloud_docker_login.sh {aws2gce_credentials}",
                "export PATH=$(pwd)/google-cloud-sdk/bin:$$PATH",
                "bash release/azure_docker_login.sh",
                f"az acr login --name {AZURE_REGISTRY_NAME}",
                f"aws ecr get-login-password --region {byod_ecr_region} | docker login --username AWS --password-stdin {byod_ecr}",
                build_cmd,
            ],
        }
        step["depends_on"] = get_prerequisite_step(image, base_image, gpu_map)
        build_config["steps"].append(step)

    with open(destination_file, "w") as f:
        yaml.dump(build_config, f, default_flow_style=False, sort_keys=False)


def _short_tag(gpu: str) -> str:
    """Derive the short gpu tag from a full gpu string.

    Examples: "cu12.3.2-cudnn9" → "cu123", "cpu" → "cpu".
    """
    if gpu in ("cpu", "tpu"):
        return gpu
    base = gpu.split("-", 1)[0]  # "cu12.3.2"
    parts = base.split(".")  # ["cu12", "3", "2"]
    return f"{parts[0]}{parts[1]}" if len(parts) >= 2 else base


def build_short_gpu_map(ray_images_path: str) -> Dict[str, str]:
    """Build short-tag → full-gpu map from ray-images.json."""
    if not os.path.exists(ray_images_path):
        raise FileNotFoundError(f"ray-images.json not found at {ray_images_path}")
    with open(ray_images_path) as f:
        images = json.load(f)
    gpus: Set[str] = set()
    for cfg in images.values():
        gpus.update(
            cfg.get("platforms", [])
        )  # ray-images.json uses platforms instead of gpu.
    result: Dict[str, str] = {}
    for g in gpus:
        short = _short_tag(g)
        if short == "tpu":
            continue  # tpu is not used in release build steps
        if short in result:
            raise ValueError(
                f"Collision detected for short tag '{short}': "
                f"'{result[short]}' and '{g}' both map to the same tag."
            )
        result[short] = g
    return result


def _sanitize_array_value(value: str) -> str:
    """Strip non-alphanumeric characters, matching rayci's array key logic."""
    return re.sub(r"[^a-zA-Z0-9]", "", value)


def get_prerequisite_step(
    image: str,
    base_image: str,
    gpu_map: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Get the base image build step for a job that depends on it.

    Returns the array-suffixed Buildkite step key for the publish step
    that produces the base image.
    """
    if gpu_map is None:
        gpu_map = {}
    config = get_global_config()
    image_repository, _ = image.split(":")
    image_name = image_repository.split("/")[-1]
    if base_image.startswith(ANYSCALE_RAY_IMAGE_PREFIX):
        return "forge"

    # Parse base_image tag: {build_id}-py{ver}-{suffix}
    _, tag = base_image.rsplit(":", 1)
    tag_parts = tag.split("-")
    py_index = None
    for i, part in enumerate(tag_parts):
        if re.match(r"^py\d+$", part):
            py_index = i
            break

    if image_name == "ray-ml":
        bare_key = config["release_image_step_ray_ml"]
        if py_index is None:
            return bare_key
        return f"{bare_key}--python{tag_parts[py_index][2:]}"

    if image_name == "ray-torch":
        tag_suffix = "-".join(tag_parts[py_index + 1 :]) if py_index is not None else ""
        if tag_suffix == "cpu":
            bare_key = config["release_image_step_ray_torch_cpu"]
        else:
            bare_key = config["release_image_step_ray_torch_cuda"]
    elif image_name == "ray-llm":
        bare_key = config["release_image_step_ray_llm"]
    else:
        # ray: pick cpu or cuda key based on tag suffix
        tag_suffix = "-".join(tag_parts[py_index + 1 :]) if py_index is not None else ""
        if tag_suffix == "cpu":
            bare_key = config["release_image_step_ray_cpu"]
        else:
            bare_key = config["release_image_step_ray_cuda"]

    if py_index is None:
        return bare_key

    python_raw = tag_parts[py_index][2:]  # e.g., "310"
    tag_suffix = "-".join(tag_parts[py_index + 1 :])  # e.g., "cu123" or "cpu"

    if tag_suffix == "cpu":
        # anyscalecpubuild has only a python dimension
        return f"{bare_key}--python{python_raw}"

    # cuda publish steps have gpu + python dimensions (alphabetical order)
    full_gpu = gpu_map.get(tag_suffix, tag_suffix)
    sanitized = _sanitize_array_value(full_gpu)

    return f"{bare_key}--gpu{sanitized}-python{python_raw}"


def collect_rayci_select_keys(tests: List[Test], gpu_map: Dict[str, str]) -> Set[str]:
    """Return the RAYCI_SELECT key set (base-image + custom-BYOD) for the given tests."""
    keys: Set[str] = set()
    for test in tests:
        image = test.get_anyscale_byod_image()
        base_image = test.get_anyscale_base_byod_image()
        prereq = get_prerequisite_step(image, base_image, gpu_map)
        if prereq:
            keys.add(prereq)
        if test.require_custom_byod_image():
            keys.add(generate_custom_build_step_key(image))
    return keys


def _get_step_name(image: str, step_key: str, test_names: List[str]) -> str:
    ecr, tag = image.split(":")
    ecr_repo = ecr.split("/")[-1]
    tag_without_build_id_and_custom_hash = tag.split("-")[1:-1]
    step_name = f":tapioca: build custom: {ecr_repo}:{'-'.join(tag_without_build_id_and_custom_hash)} ({step_key})"
    for test_name in test_names[:2]:
        step_name += f" {test_name}"
    return step_name
