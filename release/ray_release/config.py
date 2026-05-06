import copy
import itertools
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import jsonschema
import yaml

from ray_release.bazel import bazel_runfile
from ray_release.exception import ReleaseTestCLIError, ReleaseTestConfigError
from ray_release.logger import logger
from ray_release.test import (
    Test,
    TestDefinition,
)
from ray_release.util import DeferredEnvVar, deep_update

DEFAULT_WHEEL_WAIT_TIMEOUT = 7200  # Two hours
DEFAULT_COMMAND_TIMEOUT = 1800
DEFAULT_BUILD_TIMEOUT = 3600
DEFAULT_CLUSTER_TIMEOUT = 1800
DEFAULT_AUTOSUSPEND_MINS = 120
DEFAULT_MAXIMUM_UPTIME_MINS = 3200
DEFAULT_WAIT_FOR_NODES_TIMEOUT = 3000

DEFAULT_CLOUD_ID = DeferredEnvVar(
    "RELEASE_DEFAULT_CLOUD_ID",
    "cld_kvedZWag2qA8i5BjxUevf5i7",  # anyscale_v2_default_cloud
)
DEFAULT_CLOUD_NAME = DeferredEnvVar(
    "RELEASE_DEFAULT_CLOUD_NAME",
    "anyscale_v2_default_cloud",
)
DEFAULT_ANYSCALE_PROJECT = DeferredEnvVar(
    "RELEASE_DEFAULT_PROJECT",
    "prj_6rfevmf12tbsbd6g3al5f6zssh",
)

RELEASE_PACKAGE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

RELEASE_TEST_SCHEMA_FILE = bazel_runfile("release/ray_release/schema.json")

RELEASE_TEST_CONFIG_FILES = [
    "release/release_tests.yaml",
    "release/release_data_tests.yaml",
    "release/release_multimodal_inference_benchmarks_tests.yaml",
]

ALLOWED_BYOD_TYPES = [
    "gpu",
    "gpu-cu130",
    "cpu",
    "cu123",
    "llm-cu128",
    "llm-cu130",
    "torch-cpu",
    "torch-cu130",
]

NEW_COMPUTE_CONFIG_KEYS = {
    "cloud",
    "head_node",
    "worker_nodes",
    "advanced_instance_config",
}
# All fields accepted by the anyscale.compute_config.models.ComputeConfig
# dataclass. Used to filter the cluster compute dict before constructing the
# model (to strip keys like idle_termination_minutes that are not part of it).
COMPUTE_CONFIG_MODEL_FIELDS = NEW_COMPUTE_CONFIG_KEYS | {
    "cloud_resource",
    "min_resources",
    "max_resources",
    "zones",
    "enable_cross_zone_scaling",
    "flags",
    "auto_select_worker_config",
}
LEGACY_COMPUTE_CONFIG_KEYS = {
    "aws",
    "cloud_id",
    "head_node_type",
    "worker_node_types",
    "aws_advanced_configurations",
    "advanced_configurations_json",
    "gcp_advanced_configurations_json",
}

CLOUD_ID_TO_NAME = {
    "cld_kvedZWag2qA8i5BjxUevf5i7": "anyscale_v2_default_cloud",
    "cld_wy5a6nhazplvu32526ams61d98": "serve_release_tests_cloud",
    "cld_HSrCZdMCYDe1NmMCJhYRgQ4p": "aioa_aws_735219725452_us_west_2_0000",
    "cld_5nnv7pt2jn2312x2e5v72z53n2": "anyscale_aks_public_default_cloud_us_west_2",
    "cld_vy7xqacrvddvbuy95auinvuqmt": "oss_release_tests_gce",
    "cld_k8WcxPgjUtSE8RVmfZpTLuKM": "anyscale_k8s_gcp_cloud",
    "cld_tPsS3nQz8p5cautbyWgEdr4y": "anyscale_gce_cloud",
}


def read_and_validate_release_test_collection(
    config_files: List[str],
    test_definition_root: str = None,
    schema_file: Optional[str] = None,
) -> List[Test]:
    """Read and validate test collection from config file"""
    tests = []
    for config_file in config_files:
        path = (
            os.path.join(test_definition_root, config_file)
            if test_definition_root
            else bazel_runfile(config_file)
        )
        with open(path, "rt") as fp:
            tests += parse_test_definition(yaml.safe_load(fp))

    validate_release_test_collection(
        tests,
        schema_file=schema_file,
        test_definition_root=test_definition_root,
    )
    return tests


def _test_definition_invariant(
    test_definition: TestDefinition,
    invariant: bool,
    message: str,
) -> None:
    if invariant:
        return
    raise ReleaseTestConfigError(
        f'{test_definition["name"]} has invalid definition: {message}',
    )


def parse_test_definition(test_definitions: List[TestDefinition]) -> List[Test]:
    default_definition = {}
    tests = []
    for test_definition in test_definitions:
        if "matrix" in test_definition and "variations" in test_definition:
            raise ReleaseTestConfigError(
                "You can't specify both 'matrix' and 'variations' in a test definition"
            )

        if test_definition["name"] == "DEFAULTS":
            default_definition = copy.deepcopy(test_definition)
            continue

        # Add default values to the test definition.
        test_definition = deep_update(
            copy.deepcopy(default_definition), test_definition
        )

        if "variations" in test_definition:
            tests.extend(_parse_test_definition_with_variations(test_definition))
        elif "matrix" in test_definition:
            tests.extend(_parse_test_definition_with_matrix(test_definition))
        else:
            tests.append(Test(test_definition))

    return tests


def _parse_test_definition_with_variations(
    test_definition: TestDefinition,
) -> List[Test]:
    tests = []

    variations = test_definition.pop("variations")
    _test_definition_invariant(
        test_definition,
        variations,
        "variations field cannot be empty in a test definition",
    )
    for variation in variations:
        _test_definition_invariant(
            test_definition,
            "__suffix__" in variation,
            "missing __suffix__ field in a variation",
        )
        test = copy.deepcopy(test_definition)
        test["name"] = f'{test["name"]}.{variation.pop("__suffix__")}'
        test = deep_update(test, variation)
        tests.append(Test(test))

    return tests


def _parse_test_definition_with_matrix(test_definition: TestDefinition) -> List[Test]:
    tests = []

    matrix = test_definition.pop("matrix")
    variables = tuple(matrix["setup"].keys())
    combinations = itertools.product(*matrix["setup"].values())
    for combination in combinations:
        test = test_definition
        for variable, value in zip(variables, combination):
            test = _substitute_variable(test, variable, str(value))
        tests.append(Test(test))

    adjustments = matrix.pop("adjustments", [])
    for adjustment in adjustments:
        if not set(adjustment["with"]) == set(variables):
            raise ReleaseTestConfigError(
                "You need to specify all matrix variables in the adjustment."
            )

        test = test_definition
        for variable, value in adjustment["with"].items():
            test = _substitute_variable(test, variable, str(value))
        tests.append(Test(test))

    return tests


def _substitute_variable(data: Dict, variable: str, replacement: str) -> Dict:
    """Substitute a variable in the provided dictionary with a replacement value.

    This function traverses dict and list values, and substitutes the variable in
    string values. The syntax for variables is `{{variable}}`.

    Args:
        data: The dictionary to substitute the variable in.
        variable: The variable to substitute.
        replacement: The replacement value.

    Returns:
        A new dictionary with the variable substituted.

    Examples:
        >>> test_definition = {"name": "test-{{arg}}"}
        >>> _substitute_variable(test_definition, "arg", "1")
        {'name': 'test-1'}
    """
    # Create a copy to avoid mutating the original.
    data = copy.deepcopy(data)

    pattern = r"\{\{\s*" + re.escape(variable) + r"\s*\}\}"
    for key, value in data.items():
        if isinstance(value, dict):
            data[key] = _substitute_variable(value, variable, replacement)
        elif isinstance(value, list):
            data[key] = [re.sub(pattern, replacement, string) for string in value]
        elif isinstance(value, str):
            data[key] = re.sub(pattern, replacement, value)

    return data


def load_schema_file(path: Optional[str] = None) -> Dict:
    path = path or RELEASE_TEST_SCHEMA_FILE
    with open(path, "rt") as fp:
        return json.load(fp)


def validate_release_test_collection(
    test_collection: List[Test],
    schema_file: Optional[str] = None,
    test_definition_root: Optional[str] = None,
):
    try:
        schema = load_schema_file(schema_file)
    except Exception as e:
        raise ReleaseTestConfigError(
            f"Could not load release test validation schema: {e}"
        ) from e

    num_errors = 0
    for test in test_collection:
        error = validate_test(test, schema)
        if error:
            logger.error(
                f"Failed to validate test {test.get('name', '(unnamed)')}: {error}"
            )
            num_errors += 1

        error = validate_test_cluster_compute(test, test_definition_root)
        if error:
            logger.error(
                f"Failed to validate test {test.get('name', '(unnamed)')}: {error}"
            )
            num_errors += 1

    if num_errors > 0:
        raise ReleaseTestConfigError(
            f"Release test configuration error: Found {num_errors} test "
            f"validation errors."
        )


def validate_test(test: Test, schema: Optional[Dict] = None) -> Optional[str]:
    schema = schema or load_schema_file()

    try:
        jsonschema.validate(test, schema=schema)
    except (jsonschema.ValidationError, jsonschema.SchemaError) as e:
        return str(e.message)
    except Exception as e:
        return str(e)

    byod_type = test.get_byod_type()
    python_version = test.get_python_version()
    try:
        validate_byod_type(byod_type, python_version)
    except Exception as e:
        return str(e)

    return None


def validate_byod_type(byod_type: str, python_version: str) -> None:
    if byod_type not in ALLOWED_BYOD_TYPES:
        raise Exception(f"Invalid BYOD type: {byod_type}")
    if byod_type == "gpu" and python_version != "3.10":
        raise Exception("GPU BYOD tests must use Python 3.10")
    if byod_type == "gpu-cu130" and python_version != "3.12":
        raise Exception("GPU cu130 BYOD tests must use Python 3.12")
    if byod_type == "llm-cu128" and python_version != "3.11":
        raise Exception("LLM cu128 BYOD tests must use Python 3.11")
    if byod_type == "llm-cu130" and python_version != "3.12":
        raise Exception("LLM cu130 BYOD tests must use Python 3.12")
    if byod_type in ["torch-cpu", "torch-cu130"] and python_version != "3.13":
        raise Exception(f"{byod_type} BYOD tests must use Python 3.13")
    if byod_type in ["cpu", "cu123"] and python_version not in [
        "3.10",
        "3.11",
        "3.12",
        "3.13",
    ]:
        raise Exception(
            f"Invalid Python version for BYOD type {byod_type}: {python_version}"
        )


def validate_test_cluster_compute(
    test: Test, test_definition_root: Optional[str] = None
) -> Optional[str]:
    from ray_release.template import load_test_cluster_compute

    is_new_schema = test.uses_anyscale_sdk_2026()
    cluster_compute = load_test_cluster_compute(test, test_definition_root)
    return validate_cluster_compute(cluster_compute, is_new_schema=is_new_schema)


def validate_cluster_compute(
    cluster_compute: Dict[str, Any], is_new_schema: bool = False
) -> Optional[str]:
    compute_keys = set(cluster_compute.keys())
    found_legacy_keys = compute_keys & LEGACY_COMPUTE_CONFIG_KEYS
    found_new_keys = compute_keys & NEW_COMPUTE_CONFIG_KEYS

    if is_new_schema:
        # New schema: must not contain legacy keys
        if found_legacy_keys:
            return (
                f"Compute config has legacy schema keys ({found_legacy_keys}) "
                f"but test expects new schema (anyscale_sdk_2026=true)"
            )
        # Validate EBS DeleteOnTermination in new-schema fields
        head_node_aws = cluster_compute.get("head_node", {}).get(
            "advanced_instance_config", {}
        )
        configs_to_check = [
            cluster_compute.get("advanced_instance_config", {}),
            head_node_aws,
        ]
        for worker_node in cluster_compute.get("worker_nodes", []):
            worker_aws = worker_node.get("advanced_instance_config", {})
            configs_to_check.append(worker_aws)
    else:
        # Legacy schema: must contain legacy keys and must not contain new keys
        if found_new_keys:
            return (
                f"Compute config has new schema keys ({found_new_keys}) "
                f"but test expects legacy schema (anyscale_sdk_2026=false)"
            )
        if not found_legacy_keys:
            return (
                "Compute config does not have legacy schema keys "
                "but test expects legacy schema (anyscale_sdk_2026=false)"
            )
        # Validate EBS DeleteOnTermination in legacy-schema fields
        aws = cluster_compute.get("aws", {})
        head_node_aws = cluster_compute.get("head_node_type", {}).get(
            "aws_advanced_configurations", {}
        )
        configs_to_check = [aws, head_node_aws]
        for worker_node in cluster_compute.get("worker_node_types", []):
            worker_node_aws = worker_node.get("aws_advanced_configurations", {})
            configs_to_check.append(worker_node_aws)

    for config in configs_to_check:
        error = validate_aws_config(config)
        if error:
            return error

    return None


def validate_aws_config(aws_config: Dict[str, Any]) -> Optional[str]:
    for block_device_mapping in aws_config.get("BlockDeviceMappings", []):
        ebs = block_device_mapping.get("Ebs")
        if not ebs:
            continue

        if ebs.get("DeleteOnTermination", False) is not True:
            return "Ebs volume does not have `DeleteOnTermination: true` set"
    return None


def find_test(test_collection: List[Test], test_name: str) -> Optional[Test]:
    """Find test with `test_name` in `test_collection`"""
    for test in test_collection:
        if test["name"] == test_name:
            return test
    return None


def as_smoke_test(test: Test) -> Test:
    if "smoke_test" not in test:
        raise ReleaseTestCLIError(
            f"Requested smoke test, but test with name {test['name']} does "
            f"not have any smoke test configuration."
        )

    smoke_test_config = test.pop("smoke_test")
    new_test = deep_update(test, smoke_test_config)
    return new_test


def parse_python_version(version: str) -> Tuple[int, int]:
    """From XY and X.Y to (X, Y)"""
    match = re.match(r"^([0-9])\.?([0-9]+)$", version)
    if not match:
        raise ReleaseTestConfigError(f"Invalid Python version string: {version}")

    return int(match.group(1)), int(match.group(2))


def get_test_cloud_id(test: Test) -> str:
    return test.get("cluster", {}).get("cloud_id", str(DEFAULT_CLOUD_ID))


def get_test_cloud_name(test: Test) -> str:
    cloud_name = test.get("cluster", {}).get("cloud")
    if cloud_name:
        return cloud_name
    cloud_id = get_test_cloud_id(test)
    name = CLOUD_ID_TO_NAME.get(cloud_id)
    if name is None:
        raise ReleaseTestConfigError(
            f"Cloud ID '{cloud_id}' not found in CLOUD_ID_TO_NAME mapping."
        )
    return name


def get_test_project_id(test: Test, default_project_id: Optional[str] = None) -> str:
    if default_project_id is None:
        default_project_id = str(DEFAULT_ANYSCALE_PROJECT)
    return test.get("cluster", {}).get("project_id", default_project_id)
