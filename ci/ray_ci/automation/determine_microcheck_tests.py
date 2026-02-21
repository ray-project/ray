from typing import Dict, List, Set

import click

from ci.ray_ci.utils import ci_init, logger

from ray_release.configs.global_config import get_global_config
from ray_release.result import ResultStatus
from ray_release.test import Test
from ray_release.test_automation.ci_state_machine import CITestStateMachine

# The s3 prefix for the tests that run on Linux. It comes from the bazel prefix rule
# linux:// with the character "/" replaced by "_" for s3 compatibility
LINUX_TEST_PREFIX = "linux:__"


@click.command()
@click.argument("team", required=True, type=str)
@click.argument("coverage", required=True, type=int)
@click.option("--test-history-length", default=500, type=int)
@click.option("--test-prefix", default=LINUX_TEST_PREFIX, type=str)
@click.option("--production", is_flag=True, default=False)
@click.option("--consider-master-branch", is_flag=True, default=False)
def main(
    team: str,
    coverage: int,
    test_history_length: int,
    test_prefix: str,
    production: bool,
    consider_master_branch: bool,
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

    test_to_commits = {
        test.get_name(): _get_failed_commits(test, test_history_length)
        for test in tests
    }
    flaky_tests = _get_flaky_tests(tests, test_history_length)
    high_impact_tests = _get_test_with_minimal_coverage(
        test_to_commits, flaky_tests, coverage
    )
    if consider_master_branch:
        high_impact_tests = high_impact_tests.union(
            _get_failed_tests_from_master_branch(tests, test_history_length)
        )
    if production:
        _update_high_impact_tests(tests, high_impact_tests)

    logger.info(
        f"To cover {coverage}% of PRs, run the following tests: {high_impact_tests}"
    )


def _update_high_impact_tests(tests: List[Test], high_impact_tests: Set[str]) -> None:
    for test in tests:
        test.update_from_s3()
        test_name = test.get_name()
        test[Test.KEY_IS_HIGH_IMPACT] = (
            "true" if test_name in high_impact_tests else "false"
        )
        logger.info(
            f"Mark test {test_name} as high impact: {test[Test.KEY_IS_HIGH_IMPACT]}"
        )
        test.persist_to_s3()


def _get_failed_tests_from_master_branch(
    tests: List[Test], test_history_length: int
) -> Set[str]:
    """
    Get the tests that failed on the master branch
    """
    failed_tests = set()
    for test in tests:
        results = [
            result
            for result in test.get_test_results(
                limit=test_history_length,
                aws_bucket=get_global_config()["state_machine_branch_aws_bucket"],
                use_async=True,
                refresh=True,
            )
            if result.branch == "master"
        ]
        consecutive_failures = 0
        # If a test fails 2 times in a row, we consider it as a failed test
        for result in results:
            if result.status == ResultStatus.ERROR.value:
                consecutive_failures += 1
            else:
                consecutive_failures = 0
            if consecutive_failures == 2:
                failed_tests.add(test.get_name())
                break

    return failed_tests


def _get_flaky_tests(tests: List[Test], test_history_length: int) -> Set[str]:
    """
    Get the tests that are flaky
    """
    flaky_tests = set()
    for test in tests:
        result_history = [
            result
            for result in test.get_test_results(
                limit=test_history_length,
                aws_bucket=get_global_config()["state_machine_branch_aws_bucket"],
                use_async=True,
                refresh=True,
            )
            if result.branch == "master"
        ]
        if CITestStateMachine.is_flaky_result_history(result_history):
            flaky_tests.add(test.get_name())

    return flaky_tests


def _get_test_with_minimal_coverage(
    test_to_commits: Dict[str, Set[str]], flaky_tests: Set[str], coverage: int
) -> Set[str]:
    """
    Get the minimal set of tests that cover a certain percentage of PRs
    """
    all_commits = set()
    high_impact_tests = set()
    for commits in test_to_commits.values():
        all_commits.update(commits)
    if not all_commits:
        return set()

    covered_commits = set()
    covered_commit_count = 0
    while 100 * len(covered_commits) / len(all_commits) < coverage:
        most_impact_test = _get_most_impact_test(
            test_to_commits, flaky_tests, covered_commits
        )
        high_impact_tests.add(most_impact_test)
        covered_commits.update(test_to_commits[most_impact_test])
        assert covered_commit_count < len(covered_commits), "No progress in coverage"
        covered_commit_count = len(covered_commits)

    return high_impact_tests


def _get_most_impact_test(
    test_to_commits: Dict[str, Set[str]],
    flaky_tests: Set[str],
    already_covered_commits: Set[str],
) -> str:
    """
    Get the test that covers the most PR revisions, excluding the revisions that have
    already been covered
    """
    impact_none_flaky_test = None
    impact_none_flaky_coverage = 0
    impact_flaky_test = None
    impact_flaky_coverage = 0

    for test, prs in test_to_commits.items():
        new_coverage = len(prs - already_covered_commits)

        if new_coverage == 0:
            # Skip tests that cover no new PRs
            continue

        # find more impact non-flaky test
        if test not in flaky_tests:
            if (
                impact_none_flaky_test is None
                or new_coverage > impact_none_flaky_coverage
            ):
                impact_none_flaky_test = test
                impact_none_flaky_coverage = new_coverage

        # find more impact flaky test
        if test in flaky_tests:
            if impact_flaky_test is None or new_coverage > impact_flaky_coverage:
                impact_flaky_test = test
                impact_flaky_coverage = new_coverage

    # return flaky test only there are no more non-flaky test that can increase the
    # coverage
    return impact_none_flaky_test or impact_flaky_test


def _get_failed_commits(test: Test, test_history_length: int) -> Set[str]:
    """
    Get the failed PRs for a test. We use the commit to account for all revisions
    of a PR.
    """
    logger.info(f"Analyzing test {test.get_name()}")
    results = [
        result
        for result in test.get_test_results(
            limit=test_history_length,
            aws_bucket=get_global_config()["state_machine_pr_aws_bucket"],
            use_async=True,
            refresh=True,
        )
        if result.status == ResultStatus.ERROR.value
    ]
    return {result.commit for result in results if result.commit}


if __name__ == "__main__":
    main()
