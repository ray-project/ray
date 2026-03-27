import copy
from typing import Dict, List, Optional, Set

import yaml

from ray_release.logger import logger
from ray_release.test import Test

# Image group constants
IMAGE_GROUP_RAY = "ray"
IMAGE_GROUP_RAY_ML = "ray-ml"
IMAGE_GROUP_RAY_LLM = "ray-llm"

# Maps step name/key in build.rayci.yml to image group
_STEP_TO_GROUP = {
    "raycpubaseextra-testdeps": IMAGE_GROUP_RAY,
    "raycudabaseextra-testdeps": IMAGE_GROUP_RAY,
    "ray-anyscale-cpu-build": IMAGE_GROUP_RAY,
    "ray-anyscale-cuda-build": IMAGE_GROUP_RAY,
    "anyscalebuild": IMAGE_GROUP_RAY,
    "ray-llmbaseextra-testdeps": IMAGE_GROUP_RAY_LLM,
    "ray-llm-anyscale-cuda-build": IMAGE_GROUP_RAY_LLM,
    "anyscalellmbuild": IMAGE_GROUP_RAY_LLM,
    "ray-mlcudabaseextra-testdeps": IMAGE_GROUP_RAY_ML,
    "ray-ml-anyscale-cuda-build": IMAGE_GROUP_RAY_ML,
    "anyscalemlbuild": IMAGE_GROUP_RAY_ML,
}


def get_needed_image_groups(tests: List[Test]) -> Dict[str, Set[str]]:
    """Analyze tests to determine which image groups and Python versions are needed.

    Returns a dict mapping group name -> set of Python versions needed.
    Groups with no tests are omitted from the result.
    """
    groups: Dict[str, Set[str]] = {}
    for test in tests:
        if test.get_ray_version() and not test.require_custom_byod_image():
            # Uses pre-built DockerHub images, no build needed
            continue
        py = test.get_python_version()
        if test.use_byod_ml_image():
            groups.setdefault(IMAGE_GROUP_RAY_ML, set()).add(py)
        elif test.use_byod_llm_image():
            groups.setdefault(IMAGE_GROUP_RAY_LLM, set()).add(py)
        else:
            groups.setdefault(IMAGE_GROUP_RAY, set()).add(py)
    return groups


def create_filtered_build_yaml(build_yaml_path: str, tests: List[Test]) -> None:
    """Overwrite build.rayci.yml with a filtered version based on test requirements."""
    needed = get_needed_image_groups(tests)
    if needed:
        group_strs = {g: sorted(pvs) for g, pvs in needed.items()}
        logger.info(f"Required image groups: {group_strs}")
    else:
        logger.info("No image groups required from build.rayci.yml")

    with open(build_yaml_path) as f:
        build_config = yaml.safe_load(f)

    filtered_steps = []
    for step in build_config.get("steps", []):
        group = _classify_step(step)
        if group is None:
            # Unknown step, keep it as-is
            filtered_steps.append(step)
            continue

        python_versions = needed.get(group)
        if not python_versions:
            identifier = _get_step_identifier(step)
            logger.info(f"Removing step '{identifier}' (group '{group}' not needed)")
            continue

        # Filter matrix to only include needed Python versions
        if "matrix" in step:
            filtered_step = copy.deepcopy(step)
            filtered_matrix = _filter_matrix(filtered_step["matrix"], python_versions)
            if filtered_matrix is None:
                identifier = _get_step_identifier(step)
                logger.info(
                    f"Removing step '{identifier}' (no matching Python versions)"
                )
                continue
            filtered_step["matrix"] = filtered_matrix
            filtered_steps.append(filtered_step)
        else:
            filtered_steps.append(step)

    build_config["steps"] = filtered_steps

    with open(build_yaml_path, "w") as f:
        yaml.dump(build_config, f, default_flow_style=False, sort_keys=False)


def _get_step_identifier(step: dict) -> str:
    """Get the name or key of a step for identification."""
    return step.get("name") or step.get("key") or step.get("label", "<unknown>")


def _classify_step(step: dict) -> Optional[str]:
    """Classify a build step into an image group.

    Returns the group name, or None if the step is not recognized.
    """
    name = step.get("name", "")
    key = step.get("key", "")
    for identifier in (name, key):
        if identifier in _STEP_TO_GROUP:
            return _STEP_TO_GROUP[identifier]
    return None


def _filter_matrix(matrix, python_versions: Set[str]):
    """Filter a matrix to only include the given Python versions.

    Handles two formats:
    - Simple list: ["3.10", "3.11", "3.12", "3.13"]
    - Setup dict: {setup: {python: [...], ...}, adjustments: [...]}

    Returns the filtered matrix, or None if no entries survive.
    """
    if isinstance(matrix, list):
        return _filter_simple_list_matrix(matrix, python_versions)
    elif isinstance(matrix, dict) and "setup" in matrix:
        return _filter_setup_matrix(matrix, python_versions)
    # Unknown format, return as-is
    return matrix


def _filter_simple_list_matrix(matrix: list, python_versions: Set[str]):
    """Filter a simple list matrix (assumed to be Python versions)."""
    filtered = [v for v in matrix if v in python_versions]
    return filtered if filtered else None


def _filter_setup_matrix(matrix: dict, python_versions: Set[str]):
    """Filter a setup-style matrix with optional adjustments.

    Handles the edge case where a Python version exists only in adjustments
    (not in setup.python) by promoting it to setup.
    """
    setup = matrix.get("setup", {})
    adjustments = matrix.get("adjustments", [])

    # Filter setup.python
    if "python" in setup:
        setup["python"] = [v for v in setup["python"] if v in python_versions]

    # Filter adjustments by python version
    filtered_adjustments = [
        adj
        for adj in adjustments
        if adj.get("with", {}).get("python") in python_versions
    ]

    # Edge case: setup.python is empty but adjustments remain.
    # Promote first adjustment into setup so rayci generates valid matrix entries.
    if "python" in setup and not setup["python"] and filtered_adjustments:
        first_adj = filtered_adjustments.pop(0)
        adj_with = first_adj.get("with", {})
        setup["python"] = [adj_with["python"]]
        # Set the other dimension (cuda/platform) from the adjustment
        for key, value in adj_with.items():
            if key != "python" and key in setup:
                setup[key] = [value]

    # Check if anything survives
    has_setup_entries = "python" in setup and len(setup.get("python", [])) > 0
    has_adjustments = len(filtered_adjustments) > 0

    if not has_setup_entries and not has_adjustments:
        return None

    result = {"setup": setup}
    if filtered_adjustments:
        result["adjustments"] = filtered_adjustments
    return result
