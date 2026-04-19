import glob
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import click

from ray_release.buildkite.filter import filter_tests, group_tests
from ray_release.buildkite.settings import (
    get_frequency,
    get_pipeline_settings,
    get_test_filters,
)
from ray_release.buildkite.step import generate_block_step, get_step_for_test_group
from ray_release.config import (
    RELEASE_TEST_CONFIG_FILES,
    read_and_validate_release_test_collection,
)
from ray_release.configs.global_config import init_global_config
from ray_release.custom_byod_build_init_helper import (
    build_short_gpu_map,
    collect_rayci_select_keys,
    create_custom_build_yaml,
)
from ray_release.exception import ReleaseTestCLIError, ReleaseTestConfigError
from ray_release.logger import logger

_bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")
PIPELINE_ARTIFACT_PATH = "/tmp/pipeline_artifacts"

# Buildkite rejects single pipeline uploads above an organization limit (500
# at time of writing). We split below that with headroom so future growth
# or step multipliers we don't account for don't trip the limit again.
DEFAULT_MAX_JOBS_PER_UPLOAD = 450


def _group_job_count(group: Dict[str, Any]) -> int:
    """Count Buildkite jobs for a group step, including the group itself."""
    total = 1
    for step in group.get("steps", []):
        if isinstance(step, dict):
            parallelism = step.get("parallelism")
            if isinstance(parallelism, int) and parallelism > 1:
                total += parallelism
                continue
        total += 1
    return total


def _split_into_batches(
    steps: List[Dict[str, Any]], max_jobs: int
) -> List[List[Dict[str, Any]]]:
    """Greedy-pack top-level group steps into batches of <= max_jobs.

    Groups are atomic: we never split a single group across batches, so if
    any group alone exceeds max_jobs the caller must split that group.
    """
    batches: List[List[Dict[str, Any]]] = [[]]
    current_count = 0
    for group in steps:
        n = _group_job_count(group)
        if n > max_jobs:
            raise ValueError(
                f"group {group.get('group', '<unnamed>')!r} has {n} jobs, "
                f"exceeds the limit of {max_jobs}; split this group into "
                f"smaller groups"
            )
        if current_count + n > max_jobs and batches[-1]:
            batches.append([])
            current_count = 0
        batches[-1].append(group)
        current_count += n
    return batches


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
    help=(
        "Base path for the test jobs JSON output. The actual output files are "
        "chunked as <stem>_0.json, <stem>_1.json, ... to stay under the "
        "Buildkite per-upload job limit; callers should upload each chunk in "
        "order."
    ),
)
@click.option(
    "--max-jobs-per-upload",
    default=DEFAULT_MAX_JOBS_PER_UPLOAD,
    type=int,
    help=(
        "Maximum Buildkite jobs per output chunk file. Counts each group as 1 "
        "job plus its steps, with `parallelism: N` steps expanding to N jobs."
    ),
)
@click.option(
    "--rayci-select-output-file",
    type=str,
    help="Output file for RAYCI_SELECT (comma-separated base-image publish step keys).",
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
    max_jobs_per_upload: int = DEFAULT_MAX_JOBS_PER_UPLOAD,
    rayci_select_output_file: str = None,
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

    gpu_map = build_short_gpu_map(os.path.join(_bazel_workspace_dir, "ray-images.json"))

    # Generate custom image build steps
    create_custom_build_yaml(
        os.path.join(_bazel_workspace_dir, custom_build_jobs_output_file),
        tests,
        gpu_map,
    )

    rayci_select_keys = collect_rayci_select_keys(tests, gpu_map)

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
        gpu_map=gpu_map,
    )
    steps = [{"group": "block", "steps": [block_step]}] + steps if block_step else steps

    if "BUILDKITE" in os.environ:
        if os.path.exists(PIPELINE_ARTIFACT_PATH):
            shutil.rmtree(PIPELINE_ARTIFACT_PATH)
        os.makedirs(PIPELINE_ARTIFACT_PATH, exist_ok=True, mode=0o755)

        with open(os.path.join(PIPELINE_ARTIFACT_PATH, "pipeline.json"), "wt") as fp:
            json.dump(steps, fp)

        batches = _split_into_batches(steps, max_jobs_per_upload)
        output_stem, output_ext = os.path.splitext(test_jobs_output_file)
        for stale in glob.glob(
            os.path.join(_bazel_workspace_dir, f"{output_stem}_*{output_ext}")
        ):
            os.remove(stale)
        for i, batch in enumerate(batches):
            chunk_path = os.path.join(
                _bazel_workspace_dir, f"{output_stem}_{i}{output_ext}"
            )
            with open(chunk_path, "wt") as fp:
                json.dump(batch, fp)
        logger.info(
            f"Wrote {len(batches)} chunk file(s) for "
            f"{sum(_group_job_count(g) for g in steps)} total jobs."
        )

        # Only emit RAYCI_SELECT when a filter narrows the test set; an unfiltered
        # run (e.g. full nightly) wants the complete image pipeline.
        if rayci_select_output_file and test_filters:
            with open(
                os.path.join(_bazel_workspace_dir, rayci_select_output_file),
                "wt",
            ) as fp:
                fp.write(",".join(sorted(rayci_select_keys)))

        settings["frequency"] = settings["frequency"].value
        settings["priority"] = settings["priority"].value
        with open(os.path.join(PIPELINE_ARTIFACT_PATH, "settings.json"), "wt") as fp:
            json.dump(settings, fp)


if __name__ == "__main__":
    sys.exit(main())
