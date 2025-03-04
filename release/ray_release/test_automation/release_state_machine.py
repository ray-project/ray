from ray_release.test_automation.state_machine import (
    TestStateMachine,
    WEEKLY_RELEASE_BLOCKER_TAG,
)
from ray_release.test import Test, TestState


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
            "stability",
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
        ).number
        self.test[Test.KEY_GITHUB_ISSUE_NUMBER] = issue_number
