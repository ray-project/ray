import os
from typing import Tuple
from pathlib import Path
import sys
import shutil
import json
import click

from ray_release.buildkite.filter import filter_tests, group_tests
from ray_release.buildkite.settings import (
    get_pipeline_settings,
    get_test_filters,
    get_frequency,
)
from ray_release.config import (
    read_and_validate_release_test_collection,
    RELEASE_TEST_CONFIG_FILES,
)
from ray_release.buildkite.step import generate_block_step, get_step_for_test_group
from ray_release.configs.global_config import init_global_config
from ray_release.exception import ReleaseTestConfigError, ReleaseTestCLIError
from ray_release.logger import logger
from ray_release.custom_byod_build_init_helper import create_custom_build_yaml

_bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")
PIPELINE_ARTIFACT_PATH = "/tmp/pipeline_artifacts"


@click.command(
    help="Create a rayci yaml file for building custom BYOD images based on tests."
)
@click.option(
    "--test-collection-file",
    type=str,
    multiple=True,
    help="Test collection file, relative path to ray repo.",
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
@click.option(
    "--frequency",
    default=None,
    type=click.Choice(["manual", "nightly", "nightly-3x", "weekly"]),
    help="Run frequency of the test",
)
@click.option(
    "--test-filters",
    default=None,
    type=str,
    help="Test filters by prefix/regex",
)
@click.option(
    "--run-per-test",
    default=1,
    type=int,
    help=("The number of time we run test on the same commit"),
)
@click.option(
    "--custom-build-jobs-output-file",
    type=str,
    help="The output file for the custom build yaml file",
)
@click.option(
    "--test-jobs-output-file",
    type=str,
    help="The output file for the test jobs json file",
)
def main(
    test_collection_file: Tuple[str],
    run_jailed_tests: bool = False,
    run_unstable_tests: bool = False,
    global_config: str = "oss_config.yaml",
    frequency: str = None,
    test_filters: str = None,
    run_per_test: int = 1,
    custom_build_jobs_output_file: str = None,
    test_jobs_output_file: str = None,
):
    global_config_file = os.path.join(
        os.path.dirname(__file__), "..", "configs", global_config
    )
    init_global_config(global_config_file)
    settings = get_pipeline_settings()
    env = {}

    frequency = get_frequency(frequency) if frequency else settings["frequency"]
    prefer_smoke_tests = settings["prefer_smoke_tests"]
    test_filters = get_test_filters(test_filters) or settings["test_filters"]
    priority = settings["priority"]

    try:
        test_collection = read_and_validate_release_test_collection(
            test_collection_file or RELEASE_TEST_CONFIG_FILES
        )
    except ReleaseTestConfigError as e:
        logger.info("Error: %s", e)
        raise ReleaseTestConfigError(
            "Cannot load test yaml file.\nHINT: If you're kicking off tests for a "
            "specific commit on Buildkite to test Ray wheels, after clicking "
            "'New build', leave the commit at HEAD, and only specify the commit "
            "in the dialog that asks for the Ray wheels."
        ) from e

    filtered_tests = filter_tests(
        test_collection,
        frequency=frequency,
        test_filters=test_filters,
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
    # Generate custom image build steps
    create_custom_build_yaml(
        os.path.join(_bazel_workspace_dir, custom_build_jobs_output_file),
        tests,
    )

    # Generate test job steps
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

    no_concurrency_limit = settings["no_concurrency_limit"]
    if no_concurrency_limit:
        logger.warning("Concurrency is not limited for this run!")

    if os.environ.get("REPORT_TO_RAY_TEST_DB", False):
        env["REPORT_TO_RAY_TEST_DB"] = "1"

    # Pipe through RAYCI_BUILD_ID from the forge step.
    # TODO(khluu): convert the steps to rayci steps and stop passing through
    # RAYCI_BUILD_ID.
    build_id = os.environ.get("RAYCI_BUILD_ID")
    if build_id:
        env["RAYCI_BUILD_ID"] = build_id

    # If the build is manually triggered and there are more than 5 tests
    # Ask user to confirm before launching the tests.
    block_step = None
    if test_filters and len(tests) >= 5 and os.environ.get("AUTOMATIC", "") != "1":
        block_step = generate_block_step(len(tests))

    steps = get_step_for_test_group(
        grouped_tests,
        minimum_run_per_test=run_per_test,
        test_collection_file=test_collection_file,
        env=env,
        priority=priority.value,
        global_config=global_config,
        is_concurrency_limit=not no_concurrency_limit,
        block_step_key=block_step["key"] if block_step else None,
    )
    steps = [{"group": "block", "steps": [block_step]}] + steps if block_step else steps

    if "BUILDKITE" in os.environ:
        if os.path.exists(PIPELINE_ARTIFACT_PATH):
            shutil.rmtree(PIPELINE_ARTIFACT_PATH)
        os.makedirs(PIPELINE_ARTIFACT_PATH, exist_ok=True, mode=0o755)

        with open(os.path.join(PIPELINE_ARTIFACT_PATH, "pipeline.json"), "wt") as fp:
            json.dump(steps, fp)
        with open(
            os.path.join(_bazel_workspace_dir, test_jobs_output_file),
            "wt",
        ) as fp:
            json.dump(steps, fp)

        settings["frequency"] = settings["frequency"].value
        settings["priority"] = settings["priority"].value
        with open(os.path.join(PIPELINE_ARTIFACT_PATH, "settings.json"), "wt") as fp:
            json.dump(settings, fp)


if __name__ == "__main__":
    sys.exit(main())
