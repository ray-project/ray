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
_bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")


def get_test_environment():
    global _test_env
    if _test_env:
        return _test_env

    _test_env = TestEnvironment(**DEFAULT_ENV)
    return _test_env


def get_test_env_var(key: str, default: Optional[str] = None):
    test_env = get_test_environment()
    return test_env.get(key, default)


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


def get_working_dir(
    test: "Test",
    test_definition_root: Optional[str] = None,
    bazel_workspace_dir: Optional[str] = None,
) -> str:
    if not bazel_workspace_dir:
        bazel_workspace_dir = _bazel_workspace_dir
    if bazel_workspace_dir and test_definition_root:
        raise ReleaseTestConfigError(
            "test_definition_root should not be specified when running with Bazel."
        )
    working_dir = test.get("working_dir", "")
    if test_definition_root:
        return os.path.join(test_definition_root, working_dir)
    if working_dir.startswith("//"):
        working_dir = working_dir.lstrip("//")
    else:
        working_dir = os.path.join("release", working_dir)
    if bazel_workspace_dir:
        return os.path.join(bazel_workspace_dir, working_dir)
    else:
        return bazel_runfile(working_dir)


def load_test_cluster_compute(
    test: "Test", test_definition_root: Optional[str] = None
) -> Optional[Dict]:
    cluster_compute_file = test["cluster"]["cluster_compute"]
    working_dir = get_working_dir(test, test_definition_root)
    f = os.path.join(working_dir, cluster_compute_file)
    env = populate_cluster_compute_variables(test)
    return load_and_render_yaml_template(f, env=env)


def populate_cluster_compute_variables(test: "Test") -> Dict:
    env = get_test_environment()

    cloud_id = get_test_cloud_id(test)
    env["ANYSCALE_CLOUD_ID"] = cloud_id
    return env
