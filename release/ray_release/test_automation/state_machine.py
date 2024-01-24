import abc
from typing import List

import github
from github import Github
from pybuildkite.buildkite import Buildkite

from ray_release.test import (
    Test,
    TestState,
)
from ray_release.aws import get_secret_token

RAY_REPO = "ray-project/ray"
AWS_SECRET_GITHUB = "ray_ci_github_token"
AWS_SECRET_BUILDKITE = "ray_ci_buildkite_token"
DEFAULT_ISSUE_OWNER = "can-anyscale"
WEEKLY_RELEASE_BLOCKER_TAG = "weekly-release-blocker"
NO_TEAM = "none"
TEAM = [
    "core",
    "data",
    "kuberay",
    "ml",
    "rllib",
    "serve",
    "serverless",
]


class TestStateMachine(abc.ABC):
    """
    State machine that computes the next state of a test based on the current state and
    perform actions accordingly during the state transition. For example:
    - passing -[two last results failed]-> failing: create github issue
    - failing -[last result passed]-> passing: close github issue
    - jailed -[latest result passed]-> passing: update the test's oncall
    ...
    """

    ray_user = None
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
    def get_ray_user(cls):
        if not cls.ray_user:
            cls.ray_user = cls.get_github().get_user()
        return cls.ray_user

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
        user = cls.get_ray_user()
        blocker_label = cls.get_ray_repo().get_label(WEEKLY_RELEASE_BLOCKER_TAG)

        return user.get_issues(state="open", labels=[blocker_label])

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
