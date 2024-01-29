from datetime import datetime, timedelta

from ray_release.test_automation.state_machine import (
    TestStateMachine,
    DEFAULT_ISSUE_OWNER,
    WEEKLY_RELEASE_BLOCKER_TAG,
)
from ray_release.test import Test, TestState
from ray_release.logger import logger


MAX_BISECT_PER_DAY = 10  # Max number of bisects to run per day for all tests
BUILDKITE_ORGANIZATION = "ray-project"
BUILDKITE_BISECT_PIPELINE = "release-tests-bisect"
CONTINUOUS_FAILURE_TO_JAIL = 3  # Number of continuous failures before jailing
UNSTABLE_RELEASE_TEST_TAG = "unstable-release-test"


class ReleaseTestStateMachine(TestStateMachine):
    """
    State transition logic
    """

    def _move_hook(self, from_state: TestState, to_state: TestState) -> None:
        change = (from_state, to_state)
        if change == (TestState.PASSING, TestState.CONSITENTLY_FAILING):
            self._create_github_issue()
        elif change == (TestState.FAILING, TestState.CONSITENTLY_FAILING):
            self._create_github_issue()
        elif change == (TestState.CONSITENTLY_FAILING, TestState.PASSING):
            self._close_github_issue()
        elif change == (TestState.PASSING, TestState.FAILING):
            self._trigger_bisect()
        elif change == (TestState.CONSITENTLY_FAILING, TestState.JAILED):
            self._jail_test()
        elif change == (TestState.JAILED, TestState.PASSING):
            self._close_github_issue()

    def _state_hook(self, state: TestState) -> None:
        if state == TestState.JAILED:
            self._keep_github_issue_open()
        if state == TestState.PASSING:
            self.test.pop(Test.KEY_BISECT_BUILD_NUMBER, None)
            self.test.pop(Test.KEY_BISECT_BLAMED_COMMIT, None)

    def _consistently_failing_to_jailed(self) -> bool:
        return len(self.test_results) >= CONTINUOUS_FAILURE_TO_JAIL and all(
            result.is_failing()
            for result in self.test_results[:CONTINUOUS_FAILURE_TO_JAIL]
        )

    def _passing_to_flaky(self) -> bool:
        return False

    def _consistently_failing_to_flaky(self) -> bool:
        return False

    def _flaky_to_passing(self) -> bool:
        return False

    def _flaky_to_jailed(self) -> bool:
        return False

    """
    Action hooks performed during state transitions
    """

    def _create_github_issue(self) -> None:
        labels = [
            "P0",
            "bug",
            "release-test",
            "ray-test-bot",
            "triage",
            self.test.get_oncall(),
        ]
        if not self.test.is_stable():
            labels.append(UNSTABLE_RELEASE_TEST_TAG)
        else:
            labels.append(WEEKLY_RELEASE_BLOCKER_TAG)
        issue_number = self.ray_repo.create_issue(
            title=f"Release test {self.test.get_name()} failed",
            body=(
                f"Release test **{self.test.get_name()}** failed. "
                f"See {self.test_results[0].url} for more details.\n\n"
                f"Managed by OSS Test Policy"
            ),
            labels=labels,
            assignee=DEFAULT_ISSUE_OWNER,
        ).number
        self.test[Test.KEY_GITHUB_ISSUE_NUMBER] = issue_number

    def _bisect_rate_limit_exceeded(self) -> bool:
        """
        Check if we have exceeded the rate limit of bisects per day.
        """
        builds = self.ray_buildkite.builds().list_all_for_pipeline(
            BUILDKITE_ORGANIZATION,
            BUILDKITE_BISECT_PIPELINE,
            created_from=datetime.now() - timedelta(days=1),
            branch="master",
        )
        return len(builds) >= MAX_BISECT_PER_DAY

    def _trigger_bisect(self) -> None:
        if self._bisect_rate_limit_exceeded():
            logger.info(f"Skip bisect {self.test.get_name()} due to rate limit")
            return
        build = self.ray_buildkite.builds().create_build(
            BUILDKITE_ORGANIZATION,
            BUILDKITE_BISECT_PIPELINE,
            "HEAD",
            "master",
            message=f"[ray-test-bot] {self.test.get_name()} failing",
            env={
                "UPDATE_TEST_STATE_MACHINE": "1",
            },
        )
        failing_commit = self.test_results[0].commit
        passing_commits = [r.commit for r in self.test_results if r.is_passing()]
        if not passing_commits:
            logger.info(f"Skip bisect {self.test.get_name()} due to no passing commit")
            return
        passing_commit = passing_commits[0]
        self.ray_buildkite.jobs().unblock_job(
            BUILDKITE_ORGANIZATION,
            BUILDKITE_BISECT_PIPELINE,
            build["number"],
            build["jobs"][0]["id"],  # first job is the blocked job
            fields={
                "test-name": self.test.get_name(),
                "passing-commit": passing_commit,
                "failing-commit": failing_commit,
                "concurrency": "3",
                "run-per-commit": "1",
            },
        )
        self.test[Test.KEY_BISECT_BUILD_NUMBER] = build["number"]

    def comment_blamed_commit_on_github_issue(self) -> None:
        """
        Comment the blamed commit on the github issue.
        """
        blamed_commit = self.test.get(Test.KEY_BISECT_BLAMED_COMMIT)
        issue_number = self.test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        bisect_build_number = self.test.get(Test.KEY_BISECT_BUILD_NUMBER)
        if not issue_number or not bisect_build_number or not blamed_commit:
            logger.info(
                "Skip commenting blamed commit on github issue "
                f"for {self.test.get_name()}"
            )
            return
        issue = self.ray_repo.get_issue(issue_number)
        issue.create_comment(
            f"Blamed commit: {blamed_commit} "
            f"found by bisect job https://buildkite.com/{BUILDKITE_ORGANIZATION}/"
            f"{BUILDKITE_BISECT_PIPELINE}/builds/{bisect_build_number}"
        )
