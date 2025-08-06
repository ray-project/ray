import os
import shutil
from typing import Tuple, List, Dict
from pathlib import Path
import sys

import click

from ray_release.buildkite.filter import filter_tests, group_tests
from ray_release.buildkite.settings import get_pipeline_settings
from ray_release.byod.build import _image_exist
from ray_release.config import RELEASE_PACKAGE_DIR, read_and_validate_release_test_collection, RELEASE_TEST_CONFIG_FILES
from ray_release.configs.global_config import init_global_config
from ray_release.exception import ReleaseTestConfigError, ReleaseTestCLIError
from ray_release.logger import logger
from ray_release.byod.build import (
    build_anyscale_base_byod_images,
    build_anyscale_custom_byod_image,
)
bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")
RELEASE_BYOD_DIR = (
    os.path.join(bazel_workspace_dir, "release/ray_release/byod")
    if bazel_workspace_dir
    else os.path.join(RELEASE_PACKAGE_DIR, "ray_release/byod")
)

@click.command()
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
    "--run-per-test",
    default=1,
    type=int,
    help=("The number of time we run test on the same commit"),
)
def main(
    test_collection_file: Tuple[str],
    run_jailed_tests: bool = False,
    run_unstable_tests: bool = False,
    global_config: str = "oss_config.yaml",
    run_per_test: int = 1,
):
    global_config_file = os.path.join(
        os.path.dirname(__file__), "..", "configs", global_config
    )
    init_global_config(global_config_file)
    settings = get_pipeline_settings()

    tmpdir = None

    env = {}
    frequency = settings["frequency"]
    prefer_smoke_tests = settings["prefer_smoke_tests"]
    test_attr_regex_filters = settings["test_attr_regex_filters"]
    priority = settings["priority"]

    logger.info(
        f"Found the following buildkite pipeline settings:\n\n"
        f"  frequency =               {settings['frequency']}\n"
        f"  prefer_smoke_tests =      {settings['prefer_smoke_tests']}\n"
        f"  test_attr_regex_filters = {settings['test_attr_regex_filters']}\n"
        f"  ray_test_repo =           {settings['ray_test_repo']}\n"
        f"  ray_test_branch =         {settings['ray_test_branch']}\n"
        f"  priority =                {settings['priority']}\n"
        f"  no_concurrency_limit =    {settings['no_concurrency_limit']}\n"
    )

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
    custom_byod_images = set()
    for test in tests:
        if not test.require_custom_byod_image():
            continue
        custom_byod_image_build = (
            test.get_anyscale_byod_image(),
            test.get_anyscale_base_byod_image(),
            test.get_byod_post_build_script(),
        )
        custom_byod_images.add(custom_byod_image_build)
    create_custom_build_yaml(list(custom_byod_images))

def create_custom_build_yaml(custom_byod_images: List[Tuple[str, str, str]]) -> None:
    """Create a yaml file for building custom BYOD images."""
    import yaml

    if not custom_byod_images:
        return

    build_config = {
        "group": "custom BYOD build",
        "steps": []
    }

    for image, base_image, post_build_script in custom_byod_images:
        step = {
            "label": f":tapioca: build custom: {image}",
            "key": f"custom_build_" + image.replace("/", "_").replace(":", "_").replace(".", "_")[-40:],
            "instance_type": "release-medium",
            "commands": [
                f"pip3 install --user --no-deps -e release/",
                f"python release/ray_release/scripts/custom_byod_build.py --image-name {image} --base-image {base_image} --post-build-script {post_build_script}"
            ],
        }
        build_config["steps"].append(step)

    with open(".buildkite/release/custom_byod_build.rayci.yml", "w") as f:
        yaml.dump(build_config, f, default_flow_style=False, sort_keys=False)

if __name__ == "__main__":
    sys.exit(main())
