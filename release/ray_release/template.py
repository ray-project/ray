import copy
import datetime
import os
from typing import Optional, Dict, TYPE_CHECKING

import jinja2
import yaml

from ray_release.bazel import bazel_runfile
from ray_release.config import get_test_cloud_id
from ray_release.exception import ReleaseTestConfigError

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


def get_cluster_env_path(
    test: "Test", test_definition_root: Optional[str] = None
) -> str:
    working_dir = test.get("working_dir", "")
    cluster_env_file = test["cluster"]["cluster_env"]
    return (
        os.path.join(test_definition_root, working_dir, cluster_env_file)
        if test_definition_root
        else bazel_runfile("release", working_dir, cluster_env_file)
    )


def load_test_cluster_compute(
    test: "Test", test_definition_root: Optional[str] = None
) -> Optional[Dict]:
    cluster_compute_file = test["cluster"]["cluster_compute"]
    working_dir = test.get("working_dir", "")
    f = (
        os.path.join(test_definition_root, working_dir, cluster_compute_file)
        if test_definition_root
        else bazel_runfile("release", working_dir, cluster_compute_file)
    )
    env = populate_cluster_compute_variables(test)
    return load_and_render_yaml_template(f, env=env)


def populate_cluster_compute_variables(test: "Test") -> Dict:
    env = get_test_environment()

    cloud_id = get_test_cloud_id(test)
    env["ANYSCALE_CLOUD_ID"] = cloud_id
    return env
