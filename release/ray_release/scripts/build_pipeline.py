import json
import os
import shutil
import subprocess
import sys
import tempfile
from typing import Optional

import click

from ray_release.buildkite.filter import filter_tests, group_tests
from ray_release.buildkite.settings import get_pipeline_settings
from ray_release.buildkite.step import get_step
from ray_release.config import (
    read_and_validate_release_test_collection,
    DEFAULT_WHEEL_WAIT_TIMEOUT,
    parse_python_version,
)
from ray_release.exception import ReleaseTestCLIError
from ray_release.logger import logger
from ray_release.wheels import (
    find_and_wait_for_ray_wheels_url,
    find_ray_wheels_url,
    get_buildkite_repo_branch,
)
from ray_release.util import write_json

PIPELINE_ARTIFACT_PATH = "/tmp/pipeline_artifacts"


@click.command()
@click.option(
    "--test-collection-file",
    default=None,
    type=str,
    help="File containing test configurations",
)
def main(test_collection_file: Optional[str] = None):
    settings = get_pipeline_settings()

    repo = settings["ray_test_repo"]
    branch = settings["ray_test_branch"]
    tmpdir = None

    env = {}
    if repo:
        # If the Ray test repo is set, we clone that repo to fetch
        # the test configuration file. Otherwise we might be missing newly
        # added test.
        repo = settings["ray_test_repo"]
        tmpdir = tempfile.mktemp()

        clone_cmd = f"git clone --depth 1 --branch {branch} {repo} {tmpdir}"
        try:
            subprocess.check_output(clone_cmd, shell=True)
        except Exception as e:
            raise ReleaseTestCLIError(
                f"Could not clone test repository " f"{repo} (branch {branch}): {e}"
            ) from e
        test_collection_file = os.path.join(tmpdir, "release", "release_tests.yaml")
        env = {
            "RAY_TEST_REPO": repo,
            "RAY_TEST_BRANCH": branch,
        }
    else:
        test_collection_file = test_collection_file or os.path.join(
            os.path.dirname(__file__), "..", "..", "release_tests.yaml"
        )
    test_collection = read_and_validate_release_test_collection(test_collection_file)

    if tmpdir:
        shutil.rmtree(tmpdir, ignore_errors=True)

    frequency = settings["frequency"]
    prefer_smoke_tests = settings["prefer_smoke_tests"]
    test_attr_regex_filters = settings["test_attr_regex_filters"]
    ray_wheels = settings["ray_wheels"]
    priority = settings["priority"]

    logger.info(
        f"Found the following buildkite pipeline settings:\n\n"
        f"  frequency =               {settings['frequency']}\n"
        f"  prefer_smoke_tests =      {settings['prefer_smoke_tests']}\n"
        f"  test_attr_regex_filters = {settings['test_attr_regex_filters']}\n"
        f"  ray_wheels =              {settings['ray_wheels']}\n"
        f"  ray_test_repo =           {settings['ray_test_repo']}\n"
        f"  ray_test_branch =         {settings['ray_test_branch']}\n"
        f"  priority =                {settings['priority']}\n"
        f"  no_concurrency_limit =    {settings['no_concurrency_limit']}\n"
    )

    filtered_tests = filter_tests(
        test_collection,
        frequency=frequency,
        test_attr_regex_filters=test_attr_regex_filters,
        prefer_smoke_tests=prefer_smoke_tests,
    )
    logger.info(f"Found {len(filtered_tests)} tests to run.")
    if len(filtered_tests) == 0:
        raise ReleaseTestCLIError(
            "Empty test collection. The selected frequency or filter did "
            "not return any tests to run. Adjust your filters."
        )
    grouped_tests = group_tests(filtered_tests)

    group_str = ""
    for group, tests in grouped_tests.items():
        group_str += f"\n{group}:\n"
        for test, smoke in tests:
            group_str += f"  {test['name']}"
            if smoke:
                group_str += " [smoke test]"
            group_str += "\n"

    logger.info(f"Tests to run:\n{group_str}")

    # Wait for wheels here so we have them ready before we kick off
    # the other workers
    ray_wheels_url = find_and_wait_for_ray_wheels_url(
        ray_wheels, timeout=DEFAULT_WHEEL_WAIT_TIMEOUT
    )
    logger.info(f"Starting pipeline for Ray wheel: {ray_wheels_url}")

    no_concurrency_limit = settings["no_concurrency_limit"]
    if no_concurrency_limit:
        logger.warning("Concurrency is not limited for this run!")

    # Report if REPORT=1 or BUILDKITE_SOURCE=schedule or it's a release branch (i.e.
    # both the buildkite wheel branch and the test branch started with 'releases/')
    _, buildkite_branch = get_buildkite_repo_branch()
    report = (
        bool(int(os.environ.get("REPORT", "0")))
        or os.environ.get("BUILDKITE_SOURCE", "manual") == "schedule"
        or (branch.startswith("releases/") and buildkite_branch.startswith("releases/"))
    )

    steps = []
    for group in sorted(grouped_tests):
        tests = grouped_tests[group]
        group_steps = []
        for test, smoke_test in tests:
            # If the python version is defined, we need a different Ray wheels URL
            if "python" in test:
                python_version = parse_python_version(test["python"])
                this_ray_wheels_url = find_ray_wheels_url(
                    ray_wheels, python_version=python_version
                )
            else:
                this_ray_wheels_url = ray_wheels_url

            step = get_step(
                test,
                report=report,
                smoke_test=smoke_test,
                ray_wheels=this_ray_wheels_url,
                env=env,
                priority_val=priority.value,
            )

            if no_concurrency_limit:
                step.pop("concurrency", None)
                step.pop("concurrency_group", None)

            group_steps.append(step)

        group_step = {"group": group, "steps": group_steps}
        steps.append(group_step)

    if "BUILDKITE" in os.environ:
        if os.path.exists(PIPELINE_ARTIFACT_PATH):
            shutil.rmtree(PIPELINE_ARTIFACT_PATH)

        os.makedirs(PIPELINE_ARTIFACT_PATH, exist_ok=True, mode=0o755)

        write_json(steps, os.path.join(PIPELINE_ARTIFACT_PATH, "pipeline.json"))

        settings["frequency"] = settings["frequency"].value
        settings["priority"] = settings["priority"].value
        write_json(settings, os.path.join(PIPELINE_ARTIFACT_PATH, "settings.json"))

    steps_str = json.dumps(steps)
    print(steps_str)


if __name__ == "__main__":
    sys.exit(main())
