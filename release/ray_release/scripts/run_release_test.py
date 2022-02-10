import logging
import os
import sys
from typing import Optional

import click

from ray_release.aws import maybe_fetch_api_token
from ray_release.config import (
    read_and_validate_release_test_collection,
    find_test,
    as_smoke_test,
    DEFAULT_WHEEL_WAIT_TIMEOUT,
)
from ray_release.exception import ReleaseTestCLIError
from ray_release.glue import run_release_test
from ray_release.logger import logger
from ray_release.reporter.legacy_rds import LegacyRDSReporter
from ray_release.reporter.log import LogReporter
from ray_release.result import Result
from ray_release.wheels import find_and_wait_for_ray_wheels_url


@click.command()
@click.argument("test_name", required=True, type=str)
@click.option(
    "--test-collection-file",
    default=None,
    type=str,
    help="File containing test configurations",
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
    "--ray-wheels",
    default=None,
    type=str,
    help=(
        "Commit hash or URL to Ray wheels to be used for testing. "
        "If empty, defaults to the BUILDKITE_COMMIT env variable. "
        "Can be e.g. `master` to fetch latest wheels from the "
        "Ray master branch. Can also be `<repo_url>:<branch>` or "
        "`<repo_url>:<commit>` to specify a different repository to "
        "fetch wheels from, if available."
    ),
)
@click.option(
    "--cluster-id",
    default=None,
    type=str,
    help="Cluster ID of existing cluster to be re-used.",
)
@click.option(
    "--no-terminate",
    default=False,
    type=bool,
    is_flag=True,
    help="Do not terminate cluster after test.",
)
def main(
    test_name: str,
    test_collection_file: Optional[str] = None,
    smoke_test: bool = False,
    report: bool = False,
    ray_wheels: Optional[str] = None,
    cluster_id: Optional[str] = None,
    no_terminate: bool = False,
):
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        fmt="[%(levelname)s %(asctime)s] " "%(filename)s: %(lineno)d  " "%(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    test_collection_file = test_collection_file or os.path.join(
        os.path.dirname(__file__), "..", "..", "release_tests.yaml"
    )
    test_collection = read_and_validate_release_test_collection(test_collection_file)
    test = find_test(test_collection, test_name)

    if not test:
        raise ReleaseTestCLIError(
            f"Test `{test_name}` not found in collection file: "
            f"{test_collection_file}"
        )

    if smoke_test:
        test = as_smoke_test(test)

    ray_wheels_url = find_and_wait_for_ray_wheels_url(
        ray_wheels, timeout=DEFAULT_WHEEL_WAIT_TIMEOUT
    )

    anyscale_project = os.environ.get("ANYSCALE_PROJECT", None)
    if not anyscale_project:
        raise ReleaseTestCLIError(
            "You have to set the ANYSCALE_PROJECT environment variable!"
        )

    maybe_fetch_api_token()

    result = Result()

    reporters = [LogReporter()]
    if report:
        reporters.append(LegacyRDSReporter())

    result = run_release_test(
        test,
        anyscale_project=anyscale_project,
        result=result,
        ray_wheels_url=ray_wheels_url,
        reporters=reporters,
        cluster_id=cluster_id,
        no_terminate=no_terminate,
    )

    logger.info(
        f"Release test pipeline for test {test['name']} completed. "
        f"Returning with exit code = {result.return_code}"
    )
    return result.return_code


if __name__ == "__main__":
    sys.exit(main())

#
#
#
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description=__doc__,
#         formatter_class=argparse.RawDescriptionHelpFormatter)
#     parser.add_argument(
#         "--test-config", required=False, type=str, help="Test config file")
#     parser.add_argument("--test-name", type=str, help="Test name in config")
#
#     parser.add_argument(
#         "--ray-wheels", required=False, type=str, help="URL to ray wheels")
#
#     parser.add_argument(
#         "--no-terminate",
#         action="store_true",
#         default=False,
#         help="Don't terminate session after failure")
#     parser.add_argument(
#         "--report",
#         action="store_true",
#         default=False,
#         help="Do not report any results or upload to S3")
#     parser.add_argument(
#         "--category",
#         type=str,
#         default="unspecified",
#         help="Category name, e.g. `release-1.3.0` (will be saved in database)")
#     parser.add_argument(
#         "--smoke-test", action="store_true",
#         help="Finish quickly for testing")
#     parser.add_argument(
#         "--session-name",
#         required=False,
#         type=str,
#         help="Name of the session to run this test.")
#     parser.add_argument(
#         "--app-config-id-override",
#         required=False,
#         type=str,
#         help=("An app config ID, which will override the test config app "
#               "config."))
#     args, _ = parser.parse_known_args()
