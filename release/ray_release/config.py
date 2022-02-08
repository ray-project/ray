from typing import Any, Dict, List, Optional

import yaml

from ray_release.exception import ReleaseTestConfigError
from ray_release.logger import logger
from ray_release.util import deep_update

Test = Dict[str, Any]


def read_and_validate_release_test_collection(config_file: str) -> List[Test]:
    """Read and validate test collection from config file"""
    with open(config_file, "rt") as fp:
        test_config = yaml.safe_load(fp)

    validate_release_test_collection(test_config)
    return test_config


def validate_release_test_collection(test_collection: List[Dict[str, Any]]):
    errors = []
    for test in test_collection:
        errors += validate_test(test)

    if errors:
        raise ReleaseTestConfigError(
            f"Release test configuration error: Found {len(errors)} warnings.")


def validate_test(test: Test):
    # Todo: implement Schema validation
    return []


def find_test(test_collection: List[Test], test_name: str) -> Optional[Test]:
    """Find test with `test_name` in `test_collection`"""
    for test in test_collection:
        if test["name"] == test_name:
            return test
    return None


def as_smoke_test(test: Test) -> Test:
    if "smoke_test" not in test:
        logger.warning(
            f"Requested smoke test, but test with name {test['name']} does "
            f"not have any smoke test configuration.")
        return test

    smoke_test_config = test.pop("smoke_test")
    new_test = deep_update(test, smoke_test_config)
    return new_test
