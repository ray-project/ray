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
CONTINUOUS_FAILURE_TO_JAIL = 3  # Number of continuous failures before jailing
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
        TestStateMachine._init_ray_repo()
        TestStateMachine._init_ray_buildkite()

    @classmethod
    def _init_ray_repo(cls):
        if not cls.ray_repo:
            github_token = get_secret_token(AWS_SECRET_GITHUB)
            cls.ray_repo = Github(github_token).get_repo(RAY_REPO)

    @classmethod
    def get_ray_repo(cls):
        cls._init_ray_repo()
        return cls.ray_repo

    @classmethod
    def _init_ray_buildkite(cls):
        if not cls.ray_buildkite:
            buildkite_token = get_secret_token(AWS_SECRET_BUILDKITE)
            cls.ray_buildkite = Buildkite()
            cls.ray_buildkite.set_access_token(buildkite_token)

    def move(self) -> None:
        """
        Move the test to the next state.
        """
        from_state = self.test.get_state()
        to_state = self._next_state(from_state)
        self.test.set_state(to_state)
        self._move_hook(from_state, to_state)
        self._state_hook(to_state)

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

        if current_state == TestState.CONSITENTLY_FAILING:
            if self._consistently_failing_to_jailed():
                return TestState.JAILED
            if self._consistently_failing_to_passing():
                return TestState.PASSING

        if current_state == TestState.JAILED:
            if self._jailed_to_passing():
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
        elif change == (TestState.CONSITENTLY_FAILING, TestState.JAILED):
            self._jail_test()
        elif change == (TestState.JAILED, TestState.PASSING):
            self._close_github_issue()

    def _state_hook(self, state: TestState) -> None:
        """
        Action performed when test is in a particular state. This is where we do things
        to keep an invariant for a state. For example, we can keep the github issue open
        if the test is failing.
        """
        if state == TestState.JAILED:
            self._keep_github_issue_open()
        if state == TestState.PASSING:
            self.test.pop(Test.KEY_BISECT_BUILD_NUMBER, None)
            self.test.pop(Test.KEY_BISECT_BLAMED_COMMIT, None)

    def _jail_test(self) -> None:
        """
        Notify github issue owner that the test is jailed
        """
        github_issue_number = self.test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        if not github_issue_number:
            return
        issue = self.ray_repo.get_issue(github_issue_number)
        issue.create_comment("Test has been failing for far too long. Jailing.")
        labels = ["jailed-test"] + [label.name for label in issue.get_labels()]
        issue.edit(labels=labels)

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

    def _create_github_issue(self) -> None:
        labels = ["P0", "bug", "release-test", self.test.get_oncall()]
        if not self.test.is_stable():
            labels.append("unstable-release-test")
        issue_number = self.ray_repo.create_issue(
            title=f"Release test {self.test.get_name()} failed",
            body=(
                f"Release test **{self.test.get_name()}** failed. "
                f"See {self.test_results[0].url} for more details.\n\n"
                f"Managed by OSS Test Policy"
            ),
            labels=labels,
            assignee="can-anyscale",
        ).number
        self.test[Test.KEY_GITHUB_ISSUE_NUMBER] = issue_number

    def _close_github_issue(self) -> None:
        github_issue_number = self.test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        if not github_issue_number:
            return
        issue = self.ray_repo.get_issue(github_issue_number)
        issue.create_comment(f"Test passed on latest run: {self.test_results[0].url}")
        issue.edit(state="closed")
        self.test.pop(Test.KEY_GITHUB_ISSUE_NUMBER, None)

    def _keep_github_issue_open(self) -> None:
        github_issue_number = self.test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        if not github_issue_number:
            return
        issue = self.ray_repo.get_issue(github_issue_number)
        if issue.state == "open":
            return
        issue.edit(state="open")
        issue.create_comment(
            "Re-opening issue as test is still failing. "
            f"Latest run: {self.test_results[0].url}"
        )

    def _jailed_to_passing(self) -> bool:
        return len(self.test_results) > 0 and self.test_results[0].is_passing()

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
        return self._passing_to_consistently_failing() or self.test.get(
            Test.KEY_BISECT_BLAMED_COMMIT
        )

    def _consistently_failing_to_passing(self) -> bool:
        return self._failing_to_passing()

    def _consistently_failing_to_jailed(self) -> bool:
        return len(self.test_results) >= CONTINUOUS_FAILURE_TO_JAIL and all(
            result.is_failing()
            for result in self.test_results[:CONTINUOUS_FAILURE_TO_JAIL]
        )
