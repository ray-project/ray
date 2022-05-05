import copy
import datetime
import json
import os
import re
from typing import Dict, List, Optional

import jinja2
import jsonschema
import yaml

from ray_release.anyscale_util import find_cloud_by_name
from ray_release.exception import ReleaseTestConfigError, ReleaseTestCLIError
from ray_release.logger import logger
from ray_release.util import deep_update


class Test(dict):
    pass


DEFAULT_WHEEL_WAIT_TIMEOUT = 7200  # Two hours
DEFAULT_COMMAND_TIMEOUT = 1800
DEFAULT_BUILD_TIMEOUT = 1800
DEFAULT_CLUSTER_TIMEOUT = 1800
DEFAULT_AUTOSUSPEND_MINS = 120
DEFAULT_WAIT_FOR_NODES_TIMEOUT = 3000

DEFAULT_CLOUD_ID = "cld_4F7k8814aZzGG8TNUGPKnc"

DEFAULT_ENV = {
    "DATESTAMP": str(datetime.datetime.now().strftime("%Y%m%d")),
    "TIMESTAMP": str(int(datetime.datetime.now().timestamp())),
    "EXPIRATION_1D": str(
        (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    ),
    "EXPIRATION_2D": str(
        (datetime.datetime.now() + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    ),
    "EXPIRATION_3D": str(
        (datetime.datetime.now() + datetime.timedelta(days=3)).strftime("%Y-%m-%d")
    ),
}

RELEASE_PACKAGE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

RELEASE_TEST_SCHEMA_FILE = os.path.join(
    RELEASE_PACKAGE_DIR, "ray_release", "schema.json"
)


class TestEnvironment(dict):
    pass


_test_env = None


def get_test_environment():
    global _test_env
    if _test_env:
        return _test_env

    _test_env = TestEnvironment(**DEFAULT_ENV)
    return _test_env


def set_test_env_var(key: str, value: str):
    test_env = get_test_environment()
    test_env[key] = value


def get_test_env_var(key: str, default: Optional[str] = None):
    test_env = get_test_environment()
    return test_env.get(key, default)


def read_and_validate_release_test_collection(config_file: str) -> List[Test]:
    """Read and validate test collection from config file"""
    with open(config_file, "rt") as fp:
        test_config = yaml.safe_load(fp)

    validate_release_test_collection(test_config)
    return test_config


def load_schema_file(path: Optional[str] = None) -> Dict:
    path = path or RELEASE_TEST_SCHEMA_FILE
    with open(path, "rt") as fp:
        return json.load(fp)


def validate_release_test_collection(test_collection: List[Test]):
    try:
        schema = load_schema_file()
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


def get_wheels_sanity_check(commit: Optional[str] = None):
    if not commit:
        cmd = (
            "python -c 'import ray; print("
            '"No commit sanity check available, but this is the '
            "Ray wheel commit:\", ray.__commit__)'"
        )
    else:
        cmd = (
            f"python -c 'import ray; "
            f'assert ray.__commit__ == "{commit}", ray.__commit__\''
        )
    return cmd


def load_and_render_yaml_template(
    template_path: str, env: Optional[Dict] = None
) -> Optional[Dict]:
    if not template_path:
        return None

    if not os.path.exists(template_path):
        raise ReleaseTestConfigError(
            f"Cannot load yaml template from {template_path}: Path not found."
        )

    with open(template_path, "rt") as f:
        content = f.read()

    render_env = copy.deepcopy(os.environ)
    if env:
        render_env.update(env)

    try:
        content = jinja2.Template(content).render(env=render_env)
        return yaml.safe_load(content)
    except Exception as e:
        raise ReleaseTestConfigError(
            f"Error rendering/loading yaml template: {e}"
        ) from e


def load_test_cluster_env(test: Test, ray_wheels_url: str) -> Optional[Dict]:
    cluster_env_file = test["cluster"]["cluster_env"]
    cluster_env_path = os.path.join(
        RELEASE_PACKAGE_DIR, test.get("working_dir", ""), cluster_env_file
    )
    env = get_test_environment()

    commit = env.get("RAY_COMMIT", None)

    if not commit:
        match = re.search(r"/([a-f0-9]{40})/", ray_wheels_url)
        if match:
            commit = match.group(1)

    env["RAY_WHEELS_SANITY_CHECK"] = get_wheels_sanity_check(commit)
    env["RAY_WHEELS"] = ray_wheels_url

    return load_and_render_yaml_template(cluster_env_path, env=env)


def load_test_cluster_compute(test: Test) -> Optional[Dict]:
    cluster_compute_file = test["cluster"]["cluster_compute"]
    cluster_compute_path = os.path.join(
        RELEASE_PACKAGE_DIR, test.get("working_dir", ""), cluster_compute_file
    )
    env = get_test_environment()

    cloud_id = get_test_cloud_id(test)
    env["ANYSCALE_CLOUD_ID"] = cloud_id

    return load_and_render_yaml_template(cluster_compute_path, env=env)


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
        cloud_id = cloud_id or DEFAULT_CLOUD_ID
    return cloud_id
