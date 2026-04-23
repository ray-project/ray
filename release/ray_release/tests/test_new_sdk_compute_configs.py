import os
import sys

import pytest
import yaml

from ray_release.bazel import bazel_runfile
from ray_release.config import (
    DEFAULT_CLOUD_ID,
    DEFAULT_CLOUD_NAME,
    RELEASE_TEST_CONFIG_FILES,
    parse_test_definition,
)
from ray_release.template import (
    _load_and_render_yaml_template,
    load_test_cluster_compute,
)

_COMPUTE_CONFIG_FILE_ENV = "COMPUTE_CONFIG_FILE"
_COMPUTE_CONFIG_FILE_FLAG = "--compute-config-file"


def _extract_compute_config_file_arg(argv):
    """Extract ``--compute-config-file[=VALUE]`` from argv.

    Returns (override_path, remaining_argv). pytest's ``pytest_addoption``
    hook is only collected from conftest.py or plugins — not test modules —
    so we parse the flag ourselves and pass the value via an env var that
    ``pytest_generate_tests`` reads.
    """
    override = None
    remaining = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg.startswith(f"{_COMPUTE_CONFIG_FILE_FLAG}="):
            override = arg.split("=", 1)[1]
        elif arg == _COMPUTE_CONFIG_FILE_FLAG and i + 1 < len(argv):
            override = argv[i + 1]
            i += 1
        else:
            remaining.append(arg)
        i += 1
    return override, remaining


def _collect_new_sdk_tests():
    """Flattened list of release-collection tests with anyscale_sdk_2026=true."""
    tests = []
    for config_file in RELEASE_TEST_CONFIG_FILES:
        with open(bazel_runfile(config_file)) as fp:
            tests += parse_test_definition(yaml.safe_load(fp))
    return [t for t in tests if t.uses_anyscale_sdk_2026()]


def pytest_generate_tests(metafunc):
    if "target" not in metafunc.fixturenames:
        return
    path = os.environ.get(_COMPUTE_CONFIG_FILE_ENV)
    if path:
        metafunc.parametrize(
            "target",
            [("path", os.path.abspath(path))],
            ids=[os.path.basename(path)],
        )
    else:
        tests = _collect_new_sdk_tests()
        metafunc.parametrize(
            "target",
            [("test", t) for t in tests],
            ids=[t["name"] for t in tests],
        )


def _load_single_file(path):
    env = {
        "ANYSCALE_CLOUD_ID": str(DEFAULT_CLOUD_ID),
        "ANYSCALE_CLOUD_NAME": str(DEFAULT_CLOUD_NAME),
    }
    return _load_and_render_yaml_template(path, env=env)


def test_new_sdk_compute_config_is_valid(target):
    from anyscale.compute_config import ComputeConfig

    kind, payload = target
    if kind == "test":
        cluster_compute = load_test_cluster_compute(payload)
        label = (
            f"test {payload['name']!r} "
            f"(cluster_compute={payload['cluster']['cluster_compute']!r})"
        )
    else:
        cluster_compute = _load_single_file(payload)
        label = f"file {payload!r}"

    assert cluster_compute is not None, f"{label}: failed to load/render YAML"

    try:
        ComputeConfig.from_dict(cluster_compute)
    except Exception as e:
        pytest.fail(
            f"ComputeConfig.from_dict failed for {label}: {type(e).__name__}: {e}"
        )


if __name__ == "__main__":
    override, remaining = _extract_compute_config_file_arg(sys.argv[1:])
    if override:
        os.environ[_COMPUTE_CONFIG_FILE_ENV] = override
    sys.exit(pytest.main(remaining + ["-v", __file__]))
