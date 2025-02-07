import copy
import os
from typing import Any, Dict, Optional, List, Tuple

from ray_release.aws import RELEASE_AWS_BUCKET
from ray_release.buildkite.concurrency import get_concurrency_group
from ray_release.test import Test, TestState
from ray_release.config import DEFAULT_ANYSCALE_PROJECT, DEFAULT_CLOUD_ID, as_smoke_test
from ray_release.env import DEFAULT_ENVIRONMENT, load_environment
from ray_release.template import get_test_env_var
from ray_release.util import DeferredEnvVar

DEFAULT_ARTIFACTS_DIR_HOST = "/tmp/ray_release_test_artifacts"

# TODO (can): unify release_queue_small and runner_queue_small_branch queues
# having too many type of queues make them difficult to maintain
RELEASE_QUEUE_DEFAULT = DeferredEnvVar("RELEASE_QUEUE_DEFAULT", "release_queue_small")
RELEASE_QUEUE_CLIENT = DeferredEnvVar("RELEASE_QUEUE_CLIENT", "release_queue_small")

DOCKER_PLUGIN_KEY = "docker#v5.2.0"

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
            DOCKER_PLUGIN_KEY: {
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
    "retry": {
        "automatic": [
            {
                "exit_status": os.environ.get("BUILDKITE_RETRY_CODE", 79),
                "limit": os.environ.get("BUILDKITE_MAX_RETRIES", 1),
            }
        ]
    },
}


def get_step_for_test_group(
    grouped_tests: Dict[str, List[Tuple[Test, bool]]],
    minimum_run_per_test: int = 1,
    test_collection_file: List[str] = None,
    env: Optional[Dict] = None,
    priority: int = 0,
    global_config: Optional[str] = None,
    is_concurrency_limit: bool = True,
):
    steps = []
    for group in sorted(grouped_tests):
        tests = grouped_tests[group]
        group_steps = []
        for test, smoke_test in tests:
            for run_id in range(max(test.get("repeated_run", 1), minimum_run_per_test)):
                step = get_step(
                    test,
                    test_collection_file,
                    run_id=run_id,
                    # Always report performance data to databrick. Since the data is
                    # indexed by branch and commit hash, we can always filter data later
                    report=True,
                    smoke_test=smoke_test,
                    env=env,
                    priority_val=priority,
                    global_config=global_config,
                )

                if not is_concurrency_limit:
                    step.pop("concurrency", None)
                    step.pop("concurrency_group", None)

                group_steps.append(step)

        group_step = {"group": group, "steps": group_steps}
        steps.append(group_step)

    return steps


def get_step(
    test: Test,
    test_collection_file: List[str] = None,
    run_id: int = 1,
    report: bool = False,
    smoke_test: bool = False,
    env: Optional[Dict] = None,
    priority_val: int = 0,
    global_config: Optional[str] = None,
):
    env = env or {}

    step = copy.deepcopy(DEFAULT_STEP_TEMPLATE)

    cmd = [
        "./release/run_release_test.sh",
        test["name"],
        "--log-streaming-limit",
        "100",
    ]

    for file in test_collection_file or []:
        cmd += ["--test-collection-file", file]

    if global_config:
        cmd += ["--global-config", global_config]

    if report and not bool(int(os.environ.get("NO_REPORT_OVERRIDE", "0"))):
        cmd += ["--report"]

    if smoke_test:
        cmd += ["--smoke-test"]

    step["plugins"][0][DOCKER_PLUGIN_KEY]["command"] = cmd

    env_to_use = test.get("env", DEFAULT_ENVIRONMENT)
    env_dict = load_environment(env_to_use)
    env_dict.update(env)

    step["env"].update(env_dict)
    step["plugins"][0][DOCKER_PLUGIN_KEY]["image"] = "python:3.9"

    commit = get_test_env_var("RAY_COMMIT")
    branch = get_test_env_var("RAY_BRANCH")
    label = commit[:7] if commit else branch

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
    clone_test = copy.deepcopy(test)  # avoid modifying the original test
    clone_test.update_from_s3()
    jailed = clone_test.get_state() == TestState.JAILED
    full_label = ""
    if not stable:
        step["soft_fail"] = True
    if not stable:
        full_label += "[unstable]"
    if jailed:
        full_label += "[jailed]"

    full_label += test["name"]
    if smoke_test:
        full_label += " [smoke test] "
    full_label += f" ({label}) ({run_id})"

    step["label"] = full_label

    return step
