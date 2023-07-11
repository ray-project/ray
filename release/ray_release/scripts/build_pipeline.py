import json
import os
import shutil
import sys
from typing import Optional
from pathlib import Path

import click

from ray_release.buildkite.filter import filter_tests, group_tests
from ray_release.buildkite.settings import get_pipeline_settings
from ray_release.buildkite.step import get_step
from ray_release.byod.build import (
    build_anyscale_base_byod_images,
    build_anyscale_custom_byod_image,
)
from ray_release.config import (
    read_and_validate_release_test_collection,
    DEFAULT_WHEEL_WAIT_TIMEOUT,
    parse_python_version,
)
from ray_release.configs.global_config import init_global_config
from ray_release.exception import ReleaseTestCLIError, ReleaseTestConfigError
from ray_release.logger import logger
from ray_release.wheels import (
    find_and_wait_for_ray_wheels_url,
    find_ray_wheels_url,
    get_buildkite_repo_branch,
    parse_commit_from_wheel_url,
)

PIPELINE_ARTIFACT_PATH = "/tmp/pipeline_artifacts"


@click.command()
@click.option(
    "--test-collection-file",
    default=None,
    type=str,
    help="File containing test configurations",
)
@click.option(
    "--run-jailed-tests",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Will run jailed tests."),
)
@click.option(
    "--run-unstable-tests",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Will run unstable tests."),
)
@click.option(
    "--global-config",
    default="oss_config.yaml",
    type=click.Choice(
        [x.name for x in (Path(__file__).parent.parent / "configs").glob("*.yaml")]
    ),
    help="Global config to use for test execution.",
)
def main(
    test_collection_file: Optional[str] = None,
    run_jailed_tests: bool = False,
    run_unstable_tests: bool = False,
    global_config: str = "oss_config.yaml",
):
    global_config_file = os.path.join(
        os.path.dirname(__file__), "..", "configs", global_config
    )
    init_global_config(global_config_file)
    settings = get_pipeline_settings()

    tmpdir = None

    env = {}
    test_collection_file = test_collection_file or os.path.join(
        os.path.dirname(__file__), "..", "..", "release_tests.yaml"
    )

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

    try:
        test_collection = read_and_validate_release_test_collection(
            test_collection_file
        )
    except ReleaseTestConfigError as e:
        raise ReleaseTestConfigError(
            "Cannot load test yaml file.\nHINT: If you're kicking off tests for a "
            "specific commit on Buildkite to test Ray wheels, after clicking "
            "'New build', leave the commit at HEAD, and only specify the commit "
            "in the dialog that asks for the Ray wheels."
        ) from e

    if tmpdir:
        shutil.rmtree(tmpdir, ignore_errors=True)

    filtered_tests = filter_tests(
        test_collection,
        frequency=frequency,
        test_attr_regex_filters=test_attr_regex_filters,
        prefer_smoke_tests=prefer_smoke_tests,
        run_jailed_tests=run_jailed_tests,
        run_unstable_tests=run_unstable_tests,
    )
    logger.info(f"Found {len(filtered_tests)} tests to run.")
    if len(filtered_tests) == 0:
        raise ReleaseTestCLIError(
            "Empty test collection. The selected frequency or filter did "
            "not return any tests to run. Adjust your filters."
        )
    tests = [test for test, _ in filtered_tests]
    logger.info("Build anyscale base BYOD images")
    build_anyscale_base_byod_images(tests)
    logger.info("Build anyscale custom BYOD images")
    for test in tests:
        build_anyscale_custom_byod_image(test)
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
        or buildkite_branch.startswith("releases/")
    )
    if os.environ.get("REPORT_TO_RAY_TEST_DB", False):
        env["REPORT_TO_RAY_TEST_DB"] = "1"

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

            ray_commit = parse_commit_from_wheel_url(this_ray_wheels_url)
            if ray_commit:
                env.update({"RAY_COMMIT_OF_WHEEL": ray_commit})
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

        with open(os.path.join(PIPELINE_ARTIFACT_PATH, "pipeline.json"), "wt") as fp:
            json.dump(steps, fp)

        settings["frequency"] = settings["frequency"].value
        settings["priority"] = settings["priority"].value
        with open(os.path.join(PIPELINE_ARTIFACT_PATH, "settings.json"), "wt") as fp:
            json.dump(settings, fp)

    steps_str = json.dumps(steps)
    print(steps_str)


if __name__ == "__main__":
    sys.exit(main())
