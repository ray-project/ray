import copy
import json
import os
import re
from typing import Dict, List, Optional, Tuple, Any

import jsonschema
import yaml
from ray_release.test import (
    Test,
    TestDefinition,
)
from ray_release.anyscale_util import find_cloud_by_name
from ray_release.bazel import bazel_runfile
from ray_release.exception import ReleaseTestCLIError, ReleaseTestConfigError
from ray_release.logger import logger
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
DEFAULT_ANYSCALE_PROJECT = DeferredEnvVar(
    "RELEASE_DEFAULT_PROJECT",
    "prj_FKRmeV5pA6X72aVscFALNC32",
)

RELEASE_PACKAGE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

RELEASE_TEST_SCHEMA_FILE = bazel_runfile("release/ray_release/schema.json")


def read_and_validate_release_test_collection(
    config_file: str, schema_file: Optional[str] = None
) -> List[Test]:
    """Read and validate test collection from config file"""
    with open(config_file, "rt") as fp:
        tests = parse_test_definition(yaml.safe_load(fp))

    validate_release_test_collection(tests, schema_file=schema_file)
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
    tests = []
    for test_definition in test_definitions:
        if "variations" not in test_definition:
            tests.append(Test(test_definition))
            continue
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


def load_schema_file(path: Optional[str] = None) -> Dict:
    path = path or RELEASE_TEST_SCHEMA_FILE
    with open(path, "rt") as fp:
        return json.load(fp)


def validate_release_test_collection(
    test_collection: List[Test], schema_file: Optional[str] = None
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

        error = validate_test_cluster_compute(test)
        if error:
            logger.error(
                f"Failed to validate test {test.get('name', '(unnamed)')}: {error}"
            )
            num_errors += 1

        error = validate_test_cluster_env(test)
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


def validate_test_cluster_compute(test: Test) -> Optional[str]:
    from ray_release.template import load_test_cluster_compute

    cluster_compute = load_test_cluster_compute(test)
    return validate_cluster_compute(cluster_compute)


def validate_cluster_compute(cluster_compute: Dict[str, Any]) -> Optional[str]:
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


def validate_test_cluster_env(test: Test) -> Optional[str]:
    if test.is_byod_cluster():
        """
        BYOD clusters are not validated because they do not need cluster environment
        """
        return None

    from ray_release.template import get_cluster_env_path

    cluster_env_path = get_cluster_env_path(test)

    if not os.path.exists(cluster_env_path):
        raise ReleaseTestConfigError(
            f"Cannot load yaml template from {cluster_env_path}: Path not found."
        )

    return None


def validate_aws_config(aws_config: Dict[str, Any]) -> Optional[str]:
    for block_device_mapping in aws_config.get("BlockDeviceMappings", []):
        ebs = block_device_mapping.get("Ebs")
        if not ebs:
            continue

        if not ebs.get("DeleteOnTermination", False) is True:
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
    cloud_id = test["cluster"].get("cloud_id", None)
    cloud_name = test["cluster"].get("cloud_name", None)
    if cloud_id and cloud_name:
        raise RuntimeError(
            f"You can't supply both a `cloud_name` ({cloud_name}) and a "
            f"`cloud_id` ({cloud_id}) in the test cluster configuration. "
            f"Please provide only one."
        )
    elif cloud_name and not cloud_id:
        cloud_id = find_cloud_by_name(cloud_name)
        if not cloud_id:
            raise RuntimeError(f"Couldn't find cloud with name `{cloud_name}`.")
    else:
        cloud_id = cloud_id or str(DEFAULT_CLOUD_ID)
    return cloud_id
