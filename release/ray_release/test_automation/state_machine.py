import abc
from datetime import datetime, timedelta
from typing import List

import github
from github import Github
from pybuildkite.buildkite import Buildkite

from ray_release.aws import get_secret_token
from ray_release.logger import logger
from ray_release.test import (
    Test,
    TestState,
)

RAY_REPO = "ray-project/ray"
BUILDKITE_ORGANIZATION = "ray-project"
BUILDKITE_BISECT_PIPELINE = "release-tests-bisect"
AWS_SECRET_GITHUB = "ray_ci_github_token"
AWS_SECRET_BUILDKITE = "ray_ci_buildkite_token"
WEEKLY_RELEASE_BLOCKER_TAG = "weekly-release-blocker"
NO_TEAM = "none"
TEAM = [
    "core",
    "data",
    "kuberay",
    "ml",
    "rllib",
    "serve",
]
MAX_BISECT_PER_DAY = 10  # Max number of bisects to run per day for all tests


class TestStateMachine(abc.ABC):
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

    def __init__(
        self, test: Test, history_length: int = 10, dry_run: bool = False
    ) -> None:
        self.test = test
        self.test_results = test.get_test_results(limit=history_length)
        self.dry_run = dry_run
        TestStateMachine._init_ray_repo()
        TestStateMachine._init_ray_buildkite()

    @classmethod
    def _init_ray_repo(cls):
        if not cls.ray_repo:
            cls.ray_repo = cls.get_github().get_repo(RAY_REPO)

    @classmethod
    def get_github(cls):
        return Github(get_secret_token(AWS_SECRET_GITHUB))

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

    @classmethod
    def get_release_blockers(cls) -> List[github.Issue.Issue]:
        repo = cls.get_ray_repo()
        blocker_label = repo.get_label(WEEKLY_RELEASE_BLOCKER_TAG)
        return list(repo.get_issues(state="open", labels=[blocker_label]))

    @classmethod
    def get_issue_owner(cls, issue: github.Issue.Issue) -> str:
        labels = issue.get_labels()
        for label in labels:
            if label.name in TEAM:
                return label.name

        return NO_TEAM

    def move(self) -> None:
        """
        Move the test to the next state.
        """
        if not self.test_results:
            # No result to move the state
            return
        from_state = self.test.get_state()
        to_state = self._next_state(from_state)
        self.test.set_state(to_state)
        if self.dry_run:
            # Don't perform any action if dry run
            return
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
            if self._passing_to_flaky():
                return TestState.FLAKY

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
            if self._consistently_failing_to_flaky():
                return TestState.FLAKY

        if current_state == TestState.FLAKY:
            if self._flaky_to_passing():
                return TestState.PASSING
            if self._flaky_to_jailed():
                return TestState.JAILED

        if current_state == TestState.JAILED:
            if self._jailed_to_passing():
                return TestState.PASSING

        return current_state

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

    """
    Abstract methods
    """

    @abc.abstractmethod
    def _move_hook(self, from_state: TestState, to_state: TestState) -> None:
        """
        Action performed when test transitions to a different state. This is where we do
        things like creating and closing github issues, trigger bisects, etc.
        """
        pass

    @abc.abstractmethod
    def _state_hook(self, state: TestState) -> None:
        """
        Action performed when test is in a particular state. This is where we do things
        to keep an invariant for a state. For example, we can keep the github issue open
        if the test is failing.
        """
        pass

    @abc.abstractmethod
    def _consistently_failing_to_jailed(self) -> bool:
        """
        Condition to jail a test. This is an abstract method since different state
        machine implements this logic differently.
        """
        pass

    @abc.abstractmethod
    def _passing_to_flaky(self) -> bool:
        pass

    @abc.abstractmethod
    def _consistently_failing_to_flaky(self) -> bool:
        pass

    @abc.abstractmethod
    def _flaky_to_passing(self) -> bool:
        pass

    @abc.abstractmethod
    def _flaky_to_jailed(self) -> bool:
        pass

    """
    Common helper methods
    """

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

    def _close_github_issue(self) -> None:
        github_issue_number = self.test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        if not github_issue_number:
            return
        issue = self.ray_repo.get_issue(github_issue_number)
        issue.create_comment(f"Test passed on latest run: {self.test_results[0].url}")
        issue.edit(state="closed")

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

    def comment_blamed_commit_on_github_issue(self) -> bool:
        """
        Comment the blamed commit on the github issue.

        Returns: True if the comment is made, False otherwise
        """
        blamed_commit = self.test.get(Test.KEY_BISECT_BLAMED_COMMIT)
        issue_number = self.test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        bisect_build_number = self.test.get(Test.KEY_BISECT_BUILD_NUMBER)
        if not issue_number or not bisect_build_number or not blamed_commit:
            logger.info(
                "Skip commenting blamed commit on github issue "
                f"for {self.test.get_name()}. The following fields should be set: "
                f" blamed_commit={blamed_commit}, issue_number={issue_number}, "
                f" bisect_build_number={bisect_build_number}"
            )
            return False
        issue = self.ray_repo.get_issue(issue_number)
        issue.create_comment(
            f"Blamed commit: {blamed_commit} "
            f"found by bisect job https://buildkite.com/{BUILDKITE_ORGANIZATION}/"
            f"{BUILDKITE_BISECT_PIPELINE}/builds/{bisect_build_number}"
        )
        return True

    def _trigger_bisect(self) -> None:
        if self._bisect_rate_limit_exceeded():
            logger.info(f"Skip bisect {self.test.get_name()} due to rate limit")
            return
        test_type = self.test.get_test_type().value
        build = self.ray_buildkite.builds().create_build(
            BUILDKITE_ORGANIZATION,
            BUILDKITE_BISECT_PIPELINE,
            "HEAD",
            "master",
            message=f"[ray-test-bot] {self.test.get_name()} failing",
            env={
                "UPDATE_TEST_STATE_MACHINE": "1",
                "RAYCI_TEST_TYPE": test_type,
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
                "test-type": test_type,
            },
        )
        self.test[Test.KEY_BISECT_BUILD_NUMBER] = build["number"]

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
        builds = [
            build
            for build in builds
            if build["env"].get("RAYCI_TEST_TYPE") == self.test.get_test_type().value
        ]
        return len(builds) >= self.test.get_bisect_daily_rate_limit()
