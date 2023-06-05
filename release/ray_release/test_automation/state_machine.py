from ray_release.test import (
    Test,
    TestState,
)


class TestStateMachine:
    """
    State machine that computes the next state of a test based on the current state and
    perform actions accordingly during the state transition. For example:
    - passing -[two last results failed]-> failing: create github issue
    - failing -[last result passed]-> passing: close github issue
    - jailed -[latest result passed]-> passing: update the test's oncall
    ...
    """

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

    def move(self) -> None:
        """
        Move the test to the next state.
        """
        next_state = self.state_machine[self.test.get_state()]
        self.test.set_state(next_state)

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
