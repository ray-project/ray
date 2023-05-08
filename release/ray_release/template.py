import copy
import datetime
import os
import re
from typing import Optional, Dict, TYPE_CHECKING

import jinja2
import yaml

from ray_release.bazel import bazel_runfile
from ray_release.config import (
    parse_python_version,
    DEFAULT_PYTHON_VERSION,
    get_test_cloud_id,
)
from ray_release.exception import ReleaseTestConfigError
from ray_release.util import python_version_str

if TYPE_CHECKING:
    from ray_release.config import Test


DEFAULT_ENV = {
    "DATESTAMP": str(datetime.datetime.now().strftime("%Y%m%d")),
    "TIMESTAMP": str(int(datetime.datetime.now().timestamp())),
}


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

    if not os.path.isfile(template_path):
        raise ReleaseTestConfigError(
            f"Cannot load yaml template from {template_path}: Path not found."
        )

    with open(template_path, "rt") as f:
        content = f.read()

    return render_yaml_template(template=content, env=env)


def render_yaml_template(template: str, env: Optional[Dict] = None):
    render_env = copy.deepcopy(os.environ)
    if env:
        render_env.update(env)

    try:
        content = jinja2.Template(template).render(env=render_env)
        return yaml.safe_load(content)
    except Exception as e:
        raise ReleaseTestConfigError(
            f"Error rendering/loading yaml template: {e}"
        ) from e


def get_cluster_env_path(test: "Test") -> str:
    working_dir = test.get("working_dir", "")
    cluster_env_file = test["cluster"]["cluster_env"]
    return bazel_runfile("release", working_dir, cluster_env_file)


def load_test_cluster_env(test: "Test", ray_wheels_url: str) -> Optional[Dict]:
    cluster_env_path = get_cluster_env_path(test)

    env = populate_cluster_env_variables(test, ray_wheels_url=ray_wheels_url)

    return load_and_render_yaml_template(cluster_env_path, env=env)


def populate_cluster_env_variables(test: "Test", ray_wheels_url: str) -> Dict:
    env = get_test_environment()

    commit = env.get("RAY_COMMIT", None)

    if not commit:
        match = re.search(r"/([a-f0-9]{40})/", ray_wheels_url)
        if match:
            commit = match.group(1)

    env["RAY_WHEELS_SANITY_CHECK"] = get_wheels_sanity_check(commit)
    env["RAY_WHEELS"] = ray_wheels_url

    if "python" in test:
        python_version = parse_python_version(test["python"])
    else:
        python_version = DEFAULT_PYTHON_VERSION

    env[
        "RAY_IMAGE_NIGHTLY_CPU"
    ] = f"anyscale/ray:nightly-py{python_version_str(python_version)}"
    env[
        "RAY_IMAGE_ML_NIGHTLY_GPU"
    ] = f"anyscale/ray-ml:nightly-py{python_version_str(python_version)}-gpu"

    return env


def load_test_cluster_compute(test: "Test") -> Optional[Dict]:
    cluster_compute_file = test["cluster"]["cluster_compute"]
    working_dir = test.get("working_dir", "")
    f = bazel_runfile("release", working_dir, cluster_compute_file)
    env = populate_cluster_compute_variables(test)
    return load_and_render_yaml_template(f, env=env)


def populate_cluster_compute_variables(test: "Test") -> Dict:
    env = get_test_environment()

    cloud_id = get_test_cloud_id(test)
    env["ANYSCALE_CLOUD_ID"] = cloud_id
    return env
