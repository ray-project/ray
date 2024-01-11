from ray_release.test_automation.state_machine import (
    TestStateMachine,
    DEFAULT_ISSUE_OWNER,
)

from ray_release.test import Test, TestState


CONTINUOUS_FAILURE_TO_FLAKY = 10  # Number of continuous failures before flaky
CONTINUOUS_PASSING_TO_PASSING = 10  # Number of continuous passing before passing
FAILING_TO_FLAKY_MESSAGE = "This test is now considered as flaky because it has been "
"failing on postmerge for too long. Flaky tests do not run on premerge."


class CITestStateMachine(TestStateMachine):
    def _move_hook(self, from_state: TestState, to_state: TestState) -> None:
        change = (from_state, to_state)
        if change == (TestState.PASSING, TestState.CONSITENTLY_FAILING):
            self._create_github_issue()
        elif change == (TestState.FAILING, TestState.CONSITENTLY_FAILING):
            self._create_github_issue()
        elif change == (TestState.CONSITENTLY_FAILING, TestState.PASSING):
            self._close_github_issue()
        elif change == (TestState.CONSITENTLY_FAILING, TestState.FLAKY):
            self._comment_github_issue(FAILING_TO_FLAKY_MESSAGE)
        elif change == (TestState.FLAKY, TestState.PASSING):
            self._close_github_issue()

    def _state_hook(self, _: TestState) -> None:
        pass

    def _comment_github_issue(self, comment: str) -> bool:
        github_issue_number = self.test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        if not github_issue_number:
            return False
        issue = self.ray_repo.get_issue(github_issue_number)
        issue.create_comment(comment)
        return True

    def _create_github_issue(self) -> None:
        labels = [
            "P0",
            "bug",
            "ci-test",
            "ray-test-bot",
            "triage",
            self.test.get_oncall(),
        ]
        issue_number = self.ray_repo.create_issue(
            title=f"CI test {self.test.get_name()} failed",
            body=(
                f"CI test **{self.test.get_name()}** failed. "
                f"See {self.test_results[0].url} for more details.\n\n"
                f"Managed by OSS Test Policy"
            ),
            labels=labels,
            assignee=DEFAULT_ISSUE_OWNER,
        ).number
        self.test[Test.KEY_GITHUB_ISSUE_NUMBER] = issue_number

    def _consistently_failing_to_jailed(self) -> bool:
        # CI tests go to flaky first after failed instead of jailed
        return False

    def _passing_to_flaky(self) -> bool:
        # TODO(can): if flaky percentage is too high, to be implemented
        return False

    def _consistently_failing_to_flaky(self) -> bool:
        return len(self.test_results) >= CONTINUOUS_FAILURE_TO_FLAKY and all(
            result.is_failing()
            for result in self.test_results[:CONTINUOUS_FAILURE_TO_FLAKY]
        )

    def _flaky_to_passing(self) -> bool:
        return len(self.test_results) >= CONTINUOUS_PASSING_TO_PASSING and all(
            result.is_passing()
            for result in self.test_results[:CONTINUOUS_PASSING_TO_PASSING]
        )

    def _flaky_to_jailed(self) -> bool:
        # TODO(can): if the issue owner add the 'jail' tag, to be implemented
        return False
