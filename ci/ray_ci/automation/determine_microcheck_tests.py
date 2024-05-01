import click
from typing import List, Set, Dict

from ci.ray_ci.utils import logger, ci_init
from ray_release.configs.global_config import get_global_config
from ray_release.test import Test
from ray_release.result import ResultStatus

# The s3 prefix for the tests that run on Linux. It comes from the bazel prefix rule
# linux:// with the character "/" replaced by "_" for s3 compatibility
LINUX_TEST_PREFIX = "linux:__"


@click.command()
@click.argument("team", required=True, type=str)
@click.argument("coverage", required=True, type=int)
@click.option("--test-history-length", default=100, type=int)
@click.option("--test-prefix", default=LINUX_TEST_PREFIX, type=str)
@click.option("--production", is_flag=True, default=False)
def main(
    team: str,
    coverage: int,
    test_history_length: int,
    test_prefix: str,
    production: bool,
) -> None:
    """
    This script determines the tests that need to be run to cover a certain percentage
    of PR failures, based on historical data
    """
    assert coverage > 0 and coverage <= 100, "Coverage must be between 0 and 100"

    ci_init()
    tests = [
        test for test in Test.gen_from_s3(test_prefix) if test.get_oncall() == team
    ]
    logger.info(f"Analyzing {len(tests)} tests for team {team}")

    test_to_prs = {
        test.get_name(): _get_failed_prs(test, test_history_length) for test in tests
    }
    high_impact_tests = _get_test_with_minimal_coverage(test_to_prs, coverage)
    if production:
        _update_high_impact_tests(tests, high_impact_tests)

    logger.info(
        f"To cover {coverage}% of PRs, run the following tests: {high_impact_tests}"
    )


def _update_high_impact_tests(tests: List[Test], high_impact_tests: Set[str]) -> None:
    for test in tests:
        test_name = test.get_name()
        test[Test.KEY_IS_HIGH_IMPACT] = (
            "true" if test_name in high_impact_tests else "false"
        )
        logger.info(
            f"Mark test {test_name} as high impact: {test[Test.KEY_IS_HIGH_IMPACT]}"
        )
        test.persist_to_s3()


def _get_test_with_minimal_coverage(
    test_to_prs: Dict[str, Set[str]], coverage: int
) -> Set[str]:
    """
    Get the minimal set of tests that cover a certain percentage of PRs
    """
    all_prs = set()
    high_impact_tests = set()
    for prs in test_to_prs.values():
        all_prs.update(prs)
    if not all_prs:
        return set()

    covered_prs = set()
    covered_pr_count = 0
    while 100 * len(covered_prs) / len(all_prs) < coverage:
        most_impact_test = _get_most_impact_test(test_to_prs, covered_prs)
        high_impact_tests.add(most_impact_test)
        covered_prs.update(test_to_prs[most_impact_test])
        assert covered_pr_count < len(covered_prs), "No progress in coverage"
        covered_pr_count = len(covered_prs)

    return high_impact_tests


def _get_most_impact_test(
    test_to_prs: Dict[str, Set[str]],
    already_covered_prs: Set[str],
) -> str:
    """
    Get the test that covers the most PRs, excluding the PRs that have already been
    covered
    """
    most_impact_test = None
    for test, prs in test_to_prs.items():
        if most_impact_test is None or len(prs - already_covered_prs) > len(
            test_to_prs[most_impact_test] - already_covered_prs
        ):
            most_impact_test = test

    return most_impact_test


def _get_failed_prs(test: Test, test_history_length: int) -> Set[str]:
    """
    Get the failed PRs for a test. Currently we use the branch name as an identifier
    for a PR.

    TODO (can): Use the PR number instead of the branch name
    """
    logger.info(f"Analyzing test {test.get_name()}")
    results = [
        result
        for result in test.get_test_results(
            limit=test_history_length,
            aws_bucket=get_global_config()["state_machine_pr_aws_bucket"],
        )
        if result.status == ResultStatus.ERROR.value
    ]
    return {result.branch for result in results if result.branch}


if __name__ == "__main__":
    main()
