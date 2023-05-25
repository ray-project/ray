from ray_release.test import (
    Test,
    TestState,
)


class TestStateMachine:
    def __init__(self, test: Test) -> None:
        self.test = test
        self.test_results = test.get_test_results()

    def move(self) -> None:
        state_machine = {
            TestState.JAILED: TestState.PASSING
            if self._jailed_to_passing()
            else (TestState.FAILING if self._jailed_to_failing() else TestState.JAILED),
            TestState.PASSING: TestState.FAILING
            if self._passing_to_failing()
            else (TestState.JAILED if self._passing_to_jailed() else TestState.PASSING),
            TestState.FAILING: TestState.PASSING
            if self._failing_to_passing()
            else (TestState.JAILED if self._failing_to_jailed() else TestState.FAILING),
        }
        next_state = state_machine[self.test.get_state()]
        self.test.set_state(next_state)

    def _jailed_to_passing(self) -> bool:
        # TODO(can): implement this
        return False

    def _jailed_to_failing(self) -> bool:
        # never happen
        return False

    def _passing_to_failing(self) -> bool:
        return (
            len(self.test_results) > 1
            and self.test_results[0].is_failing()
            and self.test_results[1].is_failing()
        )

    def _passing_to_jailed(self) -> bool:
        # never happen
        return False

    def _failing_to_passing(self) -> bool:
        # TODO(can): implement this
        return True

    def _failing_to_jailed(self) -> bool:
        # TODO(can): implement this
        return True
