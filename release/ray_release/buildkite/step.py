import copy
import os
from typing import Any, Dict, Optional

from ray_release.aws import RELEASE_AWS_BUCKET
from ray_release.buildkite.concurrency import CONCURRENY_GROUPS, get_concurrency_group
from ray_release.config import (
    DEFAULT_ANYSCALE_PROJECT,
    DEFAULT_CLOUD_ID,
    DEFAULT_PYTHON_VERSION,
    Test,
    as_smoke_test,
    parse_python_version,
)
from ray_release.env import DEFAULT_ENVIRONMENT, load_environment
from ray_release.exception import ReleaseTestConfigError
from ray_release.template import get_test_env_var
from ray_release.util import python_version_str, DeferredEnvVar

DEFAULT_ARTIFACTS_DIR_HOST = "/tmp/ray_release_test_artifacts"

RELEASE_QUEUE_DEFAULT = DeferredEnvVar("RELEASE_QUEUE_DEFAULT", "release_queue_small")
RELEASE_QUEUE_CLIENT = DeferredEnvVar("RELEASE_QUEUE_CLIENT", "release_queue_small")

DEFAULT_STEP_TEMPLATE: Dict[str, Any] = {
    "env": {
        "ANYSCALE_CLOUD_ID": str(DEFAULT_CLOUD_ID),
        "ANYSCALE_PROJECT": str(DEFAULT_ANYSCALE_PROJECT),
        "RELEASE_AWS_BUCKET": str(RELEASE_AWS_BUCKET),
        "RELEASE_AWS_LOCATION": "dev",
        "RELEASE_AWS_DB_NAME": "ray_ci",
        "RELEASE_AWS_DB_TABLE": "release_test_result",
        "AWS_REGION": "us-west-2",
    },
    "agents": {"queue": str(RELEASE_QUEUE_DEFAULT)},
    "plugins": [
        {
            "docker#v5.2.0": {
                "image": "rayproject/ray",
                "propagate-environment": True,
                "volumes": [
                    "/var/lib/buildkite/builds:/var/lib/buildkite/builds",
                    "/usr/local/bin/buildkite-agent:/usr/local/bin/buildkite-agent",
                    f"{DEFAULT_ARTIFACTS_DIR_HOST}:{DEFAULT_ARTIFACTS_DIR_HOST}",
                ],
                "environment": ["BUILDKITE_BUILD_PATH=/var/lib/buildkite/builds"],
            }
        }
    ],
    "artifact_paths": [f"{DEFAULT_ARTIFACTS_DIR_HOST}/**/*"],
    "priority": 0,
}


def get_step(
    test: Test,
    report: bool = False,
    smoke_test: bool = False,
    ray_wheels: Optional[str] = None,
    env: Optional[Dict] = None,
    priority_val: int = 0,
):
    env = env or {}

    step = copy.deepcopy(DEFAULT_STEP_TEMPLATE)

    cmd = f"./release/run_release_test.sh \"{test['name']}\" "

    if report and not bool(int(os.environ.get("NO_REPORT_OVERRIDE", "0"))):
        cmd += " --report"

    if smoke_test:
        cmd += " --smoke-test"

    if ray_wheels:
        cmd += f" --ray-wheels {ray_wheels}"

    step["command"] = cmd

    env_to_use = test.get("env", DEFAULT_ENVIRONMENT)
    env_dict = load_environment(env_to_use)
    env_dict.update(env)

    step["env"].update(env_dict)

    if "python" in test:
        python_version = parse_python_version(test["python"])
    else:
        python_version = DEFAULT_PYTHON_VERSION

    step["plugins"][0]["docker#v5.2.0"][
        "image"
    ] = f"rayproject/ray:nightly-py{python_version_str(python_version)}"

    commit = get_test_env_var("RAY_COMMIT")
    branch = get_test_env_var("RAY_BRANCH")
    label = commit[:7] if commit else branch

    concurrency_group = test.get("concurrency_group", None)
    if concurrency_group:
        if concurrency_group not in CONCURRENY_GROUPS:
            raise ReleaseTestConfigError(
                f"Unknown concurrency group: {concurrency_group}"
            )
        concurrency_limit = CONCURRENY_GROUPS[concurrency_group]
    else:
        if smoke_test:
            concurrency_test = as_smoke_test(test)
        else:
            concurrency_test = test
        concurrency_group, concurrency_limit = get_concurrency_group(concurrency_test)

    step["concurrency_group"] = concurrency_group
    step["concurrency"] = concurrency_limit

    step["priority"] = priority_val

    # Set queue to QUEUE_CLIENT for client tests
    # (otherwise keep default QUEUE_DEFAULT)
    if test.get("run", {}).get("type") == "client":
        step["agents"]["queue"] = str(RELEASE_QUEUE_CLIENT)

    # If a test is not stable, allow to soft fail
    stable = test.get("stable", True)
    if not stable:
        step["soft_fail"] = True
        full_label = "[unstable] "
    else:
        full_label = ""

    full_label += test["name"]
    if smoke_test:
        full_label += " [smoke test] "
    full_label += f" ({label})"

    step["label"] = full_label

    return step
