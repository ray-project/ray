from datetime import datetime, timedelta

from github import Github
from pybuildkite.buildkite import Buildkite

from ray_release.test import (
    Test,
    TestState,
)
from ray_release.logger import logger
from ray_release.aws import get_secret_token

RAY_REPO = "ray-project/ray"
AWS_SECRET_GITHUB = "ray_ci_github_token"
AWS_SECRET_BUILDKITE = "ray_ci_buildkite_token"
MAX_BISECT_PER_DAY = 10  # Max number of bisects to run per day for all tests
BUILDKITE_ORGANIZATION = "ray-project"
BUILDKITE_BISECT_PIPELINE = "release-tests-bisect"


class TestStateMachine:
    """
    State machine that computes the next state of a test based on the current state and
    perform actions accordingly during the state transition. For example:
    - passing -[two last results failed]-> failing: create github issue
    - failing -[last result passed]-> passing: close github issue
    - jailed -[latest result passed]-> passing: update the test's oncall
    ...
    """

    ray_repo = None
    ray_buildkite = None

    def __init__(self, test: Test) -> None:
        self.test = test
        self.test_results = test.get_test_results()
        if not self.ray_repo:
            github_token = get_secret_token(AWS_SECRET_GITHUB)
            self.ray_repo = Github(github_token).get_repo(RAY_REPO)
        if not self.ray_buildkite:
            buildkite_token = get_secret_token(AWS_SECRET_BUILDKITE)
            self.ray_buildkite = Buildkite()
            self.ray_buildkite.set_access_token(buildkite_token)

    def move(self) -> None:
        """
        Move the test to the next state.
        """
        from_state = self.test.get_state()
        to_state = self._next_state(from_state)
        self.test.set_state(to_state)
        self._move_hook(from_state, to_state)

    def _next_state(self, current_state) -> TestState:
        """
        Compute the next state of the test based on the current state and the test
        """
        if current_state == TestState.PASSING:
            if self._passing_to_consistently_failing():
                return TestState.CONSITENTLY_FAILING
            if self._passing_to_failing():
                return TestState.FAILING

        if current_state == TestState.FAILING:
            if self._failing_to_consistently_failing():
                return TestState.CONSITENTLY_FAILING
            if self._failing_to_passing():
                return TestState.PASSING

        return current_state

    def _move_hook(self, from_state: TestState, to_state: TestState) -> None:
        """
        Action performed when test transitions to a different state. This is where we do
        things like creating and closing github issues, trigger bisects, etc.
        """
        change = (from_state, to_state)
        if change == (TestState.PASSING, TestState.CONSITENTLY_FAILING):
            self._create_github_issue()
        elif change == (TestState.FAILING, TestState.CONSITENTLY_FAILING):
            self._create_github_issue()
        elif change == (TestState.CONSITENTLY_FAILING, TestState.PASSING):
            self._close_github_issue()
        elif change == (TestState.PASSING, TestState.FAILING):
            self._trigger_bisect()

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

    def _create_github_issue(self) -> None:
        issue_number = self.ray_repo.create_issue(
            title=f"Release test {self.test.get_name()} failed",
            body=(
                f"Release test {self.test.get_name()} failed.\n"
                f"See {self.test_results[0].url} for more details.\n"
                f"cc @{self.test.get_oncall()}\n\n"
                "\t -- created by ray-test-bot"
            ),
            labels=["P0", "bug", "release-test"],
            assignee="can-anyscale",
        ).number
        self.test[Test.KEY_GITHUB_ISSUE_NUMBER] = issue_number

    def _close_github_issue(self) -> None:
        # TODO(can): implement this
        pass

    def _passing_to_failing(self) -> bool:
        return (
            len(self.test_results) > 0
            and self.test_results[0].is_failing()
            and not self._passing_to_consistently_failing()
        )

    def _passing_to_consistently_failing(self) -> bool:
        return (
            len(self.test_results) > 1
            and self.test_results[0].is_failing()
            and self.test_results[1].is_failing()
        )

    def _failing_to_passing(self) -> bool:
        return len(self.test_results) > 0 and self.test_results[0].is_passing()

    def _failing_to_consistently_failing(self) -> bool:
        return self._passing_to_consistently_failing()
