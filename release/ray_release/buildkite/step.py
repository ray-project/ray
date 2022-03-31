import copy
import os
from typing import Optional, Dict, Any

from ray_release.buildkite.concurrency import CONCURRENY_GROUPS, get_concurrency_group
from ray_release.config import Test, get_test_env_var
from ray_release.exception import ReleaseTestConfigError

DEFAULT_ARTIFACTS_DIR_HOST = "/tmp/ray_release_test_artifacts"

DEFAULT_STEP_TEMPLATE: Dict[str, Any] = {
    "env": {
        "ANYSCALE_CLOUD_ID": "cld_4F7k8814aZzGG8TNUGPKnc",
        "ANYSCALE_PROJECT": "prj_2xR6uT6t7jJuu1aCwWMsle",
        "RELEASE_AWS_BUCKET": "ray-release-automation-results",
        "RELEASE_AWS_LOCATION": "dev",
        "RELEASE_AWS_DB_NAME": "ray_ci",
        "RELEASE_AWS_DB_TABLE": "release_test_result",
        "AWS_REGION": "us-west-2",
    },
    "agents": {"queue": "runner_queue_branch"},
    "plugins": [
        {
            "docker#v3.9.0": {
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
    step["env"].update(env)

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
        concurrency_group, concurrency_limit = get_concurrency_group(test)

    step["concurrency_group"] = concurrency_group
    step["concurrency"] = concurrency_limit

    step["priority"] = priority_val

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
