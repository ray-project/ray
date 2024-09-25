import os
import sys
from typing import Optional, Tuple
from pathlib import Path

import click
from ray_release.aws import maybe_fetch_api_token
from ray_release.config import (
    as_smoke_test,
    find_test,
    read_and_validate_release_test_collection,
)
from ray_release.configs.global_config import init_global_config
from ray_release.env import DEFAULT_ENVIRONMENT, load_environment, populate_os_env
from ray_release.exception import ReleaseTestCLIError, ReleaseTestError
from ray_release.glue import run_release_test
from ray_release.logger import logger
from ray_release.reporter.artifacts import ArtifactsReporter
from ray_release.reporter.db import DBReporter
from ray_release.reporter.ray_test_db import RayTestDBReporter
from ray_release.reporter.log import LogReporter
from ray_release.result import Result
from ray_release.anyscale_util import LAST_LOGS_LENGTH


@click.command()
@click.argument("test_name", required=True, type=str)
@click.option(
    "--test-collection-file",
    multiple=True,
    type=str,
    help="Test collection file, relative path to ray repo.",
)
@click.option(
    "--smoke-test",
    default=False,
    type=bool,
    is_flag=True,
    help="Finish quickly for testing",
)
@click.option(
    "--report",
    default=False,
    type=bool,
    is_flag=True,
    help="Report results to database",
)
@click.option(
    "--cluster-id",
    default=None,
    type=str,
    help="Cluster ID of existing cluster to be re-used.",
)
@click.option(
    "--cluster-env-id",
    default=None,
    type=str,
    help="Cluster env ID of existing cluster env to be re-used.",
)
@click.option(
    "--env",
    default=None,
    # Get the names without suffixes of all files in "../environments"
    type=click.Choice(
        [x.stem for x in (Path(__file__).parent.parent / "environments").glob("*.env")]
    ),
    help="Environment to use. Will overwrite environment used in test config.",
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
    "--no-terminate",
    default=False,
    type=bool,
    is_flag=True,
    help=(
        "Do not terminate cluster after test. "
        "Will switch `anyscale_job` run type to `job` (Ray Job)."
    ),
)
@click.option(
    "--test-definition-root",
    default=None,
    type=str,
    help="Root of the test definition files. Default is the root of the repo.",
)
@click.option(
    "--log-streaming-limit",
    default=LAST_LOGS_LENGTH,
    type=int,
    help="Limit of log streaming in number of lines. Set to -1 to stream all logs.",
)
def main(
    test_name: str,
    test_collection_file: Tuple[str],
    smoke_test: bool = False,
    report: bool = False,
    cluster_id: Optional[str] = None,
    cluster_env_id: Optional[str] = None,
    env: Optional[str] = None,
    global_config: str = "oss_config.yaml",
    no_terminate: bool = False,
    test_definition_root: Optional[str] = None,
    log_streaming_limit: int = LAST_LOGS_LENGTH,
):
    global_config_file = os.path.join(
        os.path.dirname(__file__), "..", "configs", global_config
    )
    init_global_config(global_config_file)
    test_collection = read_and_validate_release_test_collection(
        test_collection_file or ["release/release_tests.yaml"],
        test_definition_root,
    )
    test = find_test(test_collection, test_name)

    if not test:
        raise ReleaseTestCLIError(
            f"Test `{test_name}` not found in collection file: "
            f"{test_collection_file}"
        )

    if smoke_test:
        test = as_smoke_test(test)

    env_to_use = env or test.get("env", DEFAULT_ENVIRONMENT)
    env_dict = load_environment(env_to_use)
    populate_os_env(env_dict)
    anyscale_project = os.environ.get("ANYSCALE_PROJECT", None)
    if not anyscale_project:
        raise ReleaseTestCLIError(
            "You have to set the ANYSCALE_PROJECT environment variable!"
        )

    maybe_fetch_api_token()

    result = Result()

    reporters = [LogReporter()]

    if "BUILDKITE" in os.environ:
        reporters.append(ArtifactsReporter())

    if report:
        reporters.append(DBReporter())

    # TODO(can): this env var is used as a feature flag, in case we need to turn this
    # off quickly. We should remove this when the new db reporter is stable.
    if os.environ.get("REPORT_TO_RAY_TEST_DB", False):
        reporters.append(RayTestDBReporter())

    try:
        result = run_release_test(
            test,
            anyscale_project=anyscale_project,
            result=result,
            reporters=reporters,
            smoke_test=smoke_test,
            cluster_id=cluster_id,
            cluster_env_id=cluster_env_id,
            no_terminate=no_terminate,
            test_definition_root=test_definition_root,
            log_streaming_limit=log_streaming_limit,
        )
        return_code = result.return_code
    except ReleaseTestError as e:
        logger.exception(e)
        return_code = e.exit_code.value
    logger.info(
        f"Release test pipeline for test {test['name']} completed. "
        f"Returning with exit code = {return_code}"
    )
    sys.exit(return_code)


if __name__ == "__main__":
    main()
