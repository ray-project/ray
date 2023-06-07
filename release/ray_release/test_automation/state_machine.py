import boto3
from github import Github

from ray_release.test import (
    Test,
    TestState,
)

RAY_REPO = "ray-project/ray"
AWS_SECRET_GITHUB = "ray_ci_github_token"


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

    def __init__(self, test: Test) -> None:
        self.test = test
        self.test_results = test.get_test_results()
        self.state_machine = {
            # jailed -> passing
            TestState.JAILED: TestState.PASSING
            if self._jailed_to_passing()
            else TestState.JAILED,
            # passing -> failing
            TestState.PASSING: TestState.FAILING
            if self._passing_to_failing()
            else TestState.PASSING,
            # failing -> passing OR failing -> jailed
            TestState.FAILING: TestState.PASSING
            if self._failing_to_passing()
            else (TestState.JAILED if self._failing_to_jailed() else TestState.FAILING),
        }
        if not self.ray_repo:
            github_token = TestStateMachine.github_token = boto3.client(
                "secretsmanager", region_name="us-west-2"
            ).get_secret_value(SecretId=AWS_SECRET_GITHUB)["SecretString"]
            self.ray_repo = Github(github_token).get_repo(RAY_REPO)

    def move(self) -> None:
        """
        Move the test to the next state.
        """
        from_state = self.test.get_state()
        to_state = self.state_machine[from_state]
        self.test.set_state(to_state)
        self._move_hook(from_state, to_state)

    def _move_hook(self, from_state: TestState, to_state: TestState) -> None:
        """
        Action performed when test transitions to a different state. This is where we do
        things like creating and closing github issues, trigger bisects, etc.
        """
        if (from_state, to_state) == (TestState.PASSING, TestState.FAILING):
            self._create_github_issue()
        elif (from_state, to_state) == (TestState.FAILING, TestState.PASSING):
            self._close_github_issue()

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

    def _jailed_to_passing(self) -> bool:
        # TODO(can): implement this
        return False

    def _passing_to_failing(self) -> bool:
        return (
            len(self.test_results) > 1
            and self.test_results[0].is_failing()
            and self.test_results[1].is_failing()
        )

    def _failing_to_passing(self) -> bool:
        # TODO(can): implement this
        return True

    def _failing_to_jailed(self) -> bool:
        # TODO(can): implement this
        return True
