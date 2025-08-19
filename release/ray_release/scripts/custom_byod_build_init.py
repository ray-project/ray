import os
from typing import Tuple
from pathlib import Path
import sys

import click

from ray_release.buildkite.filter import filter_tests
from ray_release.buildkite.settings import get_pipeline_settings
from ray_release.config import (
    RELEASE_PACKAGE_DIR,
    read_and_validate_release_test_collection,
    RELEASE_TEST_CONFIG_FILES,
)
from ray_release.configs.global_config import init_global_config
from ray_release.exception import ReleaseTestConfigError, ReleaseTestCLIError
from ray_release.logger import logger
from ray_release.custom_byod_build_init_helper import create_custom_build_yaml

DEFAULT_INSTALL_COMMANDS = [
    "aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 029272617770.dkr.ecr.us-west-2.amazonaws.com",
]


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
def main(
    test_collection_file: Tuple[str],
    run_jailed_tests: bool = False,
    run_unstable_tests: bool = False,
    global_config: str = "oss_config.yaml",
):
    global_config_file = os.path.join(
        os.path.dirname(__file__), "..", "configs", global_config
    )
    init_global_config(global_config_file)
    settings = get_pipeline_settings()

    frequency = settings["frequency"]
    prefer_smoke_tests = settings["prefer_smoke_tests"]
    test_attr_regex_filters = settings["test_attr_regex_filters"]

    try:
        test_collection = read_and_validate_release_test_collection(
            test_collection_file or RELEASE_TEST_CONFIG_FILES
        )
    except ReleaseTestConfigError as e:
        raise ReleaseTestConfigError(
            "Cannot load test yaml file.\nHINT: If you're kicking off tests for a "
            "specific commit on Buildkite to test Ray wheels, after clicking "
            "'New build', leave the commit at HEAD, and only specify the commit "
            "in the dialog that asks for the Ray wheels."
        ) from e

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
    custom_byod_images = set()
    for test in tests:
        if not test.require_custom_byod_image():
            continue
        custom_byod_image_build = (
            test.get_anyscale_byod_image(),
            test.get_anyscale_base_byod_image(),
            test.get_byod_post_build_script(),
        )
        logger.info(f"To be built: {custom_byod_image_build[0]}")
        custom_byod_images.add(custom_byod_image_build)
    create_custom_build_yaml(
        ".buildkite/release/custom_byod_build.rayci.yml", list(custom_byod_images)
    )


if __name__ == "__main__":
    sys.exit(main())
