from ray_release.test_automation.state_machine import (
    TestStateMachine,
    DEFAULT_ISSUE_OWNER,
)

from ray_release.test import Test, TestState


class CITestStateMachine(TestStateMachine):
    def _move_hook(self, from_state: TestState, to_state: TestState) -> None:
        change = (from_state, to_state)
        if change == (TestState.PASSING, TestState.CONSITENTLY_FAILING):
            self._create_github_issue()
        elif change == (TestState.FAILING, TestState.CONSITENTLY_FAILING):
            self._create_github_issue()
        elif change == (TestState.CONSITENTLY_FAILING, TestState.PASSING):
            self._close_github_issue()

    def _state_hook(self, state: TestState) -> None:
        pass

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
