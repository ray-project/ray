from ray_release.test_automation.state_machine import (
    TestStateMachine,
    DEFAULT_ISSUE_OWNER,
    WEEKLY_RELEASE_BLOCKER_TAG,
)

from ray_release.test import Test, TestState


CONTINUOUS_FAILURE_TO_FLAKY = 10  # Number of continuous failures before flaky
CONTINUOUS_PASSING_TO_PASSING = 20  # Number of continuous passing before passing
FLAKY_PERCENTAGE_THRESHOLD = 5  # Percentage threshold to be considered as flaky
FAILING_TO_FLAKY_MESSAGE = (
    "This test is now considered as flaky because it has been "
    "failing on postmerge for too long. Flaky tests do not run on premerge."
)
JAILED_MESSAGE = (
    "This test is confirmed to be jailed because of the presence of the jailed tag. "
    "Jailed tests are no longer release blocking. To unjail this test, close this "
    "issue."
)
JAILED_TAG = "jailed-test"
MAX_REPORT_FAILURE_NO = 5


class CITestStateMachine(TestStateMachine):
    def __init__(self, test: Test, dry_run: bool = False) -> None:
        # Need long enough test history to detect flaky tests
        super().__init__(test, dry_run=dry_run, history_length=100)

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
        elif change == (TestState.PASSING, TestState.FLAKY):
            self._create_github_issue()
        elif change == (TestState.FLAKY, TestState.PASSING):
            self._close_github_issue()
        elif change == (TestState.FLAKY, TestState.JAILED):
            self._comment_github_issue(JAILED_MESSAGE)

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
            "bug",
            "ci-test",
            "ray-test-bot",
            "flaky-tracker",
            "triage",
            self.test.get_oncall(),
            WEEKLY_RELEASE_BLOCKER_TAG,
        ]
        recent_failures = [
            result for result in self.test_results if result.is_failing()
        ][:MAX_REPORT_FAILURE_NO]
        body = (
            f"CI test **{self.test.get_name()}** is {self.test.get_state().value}. "
            "Recent failures: \n"
        )
        for failure in recent_failures:
            body += f"\t- {failure.url}\n"
        # This line is to match the regex in https://shorturl.at/aiK25
        body += f"\nDataCaseName-{self.test.get_name()}-END\n"
        body += "Managed by OSS Test Policy"
        issue_number = self.ray_repo.create_issue(
            title=f"CI test {self.test.get_name()} is {self.test.get_state().value}",
            body=body,
            labels=labels,
            assignee=DEFAULT_ISSUE_OWNER,
        ).number
        self.test[Test.KEY_GITHUB_ISSUE_NUMBER] = issue_number

    def _consistently_failing_to_jailed(self) -> bool:
        return False

    def _passing_to_flaky(self) -> bool:
        # A test is flaky if it has been changing from passing to failing for
        # a certain percentage of time in the test history. However, if it has been
        # recently stable, then it is not flaky.
        if self._is_recently_stable():
            return False
        transition = 0
        for i in range(0, len(self.test_results) - 1):
            if (
                self.test_results[i].is_failing()
                and self.test_results[i + 1].is_passing()
            ):
                transition += 1

        if transition >= FLAKY_PERCENTAGE_THRESHOLD * len(self.test_results) / 100:
            return True
        return False

    def _consistently_failing_to_flaky(self) -> bool:
        return len(self.test_results) >= CONTINUOUS_FAILURE_TO_FLAKY and all(
            result.is_failing()
            for result in self.test_results[:CONTINUOUS_FAILURE_TO_FLAKY]
        )

    def _flaky_to_passing(self) -> bool:
        return self._is_recently_stable()

    def _is_recently_stable(self) -> bool:
        return len(self.test_results) >= CONTINUOUS_PASSING_TO_PASSING and all(
            result.is_passing()
            for result in self.test_results[:CONTINUOUS_PASSING_TO_PASSING]
        )

    def _flaky_to_jailed(self) -> bool:
        # If a human has confirmed that this test is jailed (by adding the jailed tag),
        # then it is jailed
        github_issue_number = self.test.get(Test.KEY_GITHUB_ISSUE_NUMBER)
        if not github_issue_number:
            return False
        issue = self.ray_repo.get_issue(github_issue_number)
        return JAILED_TAG in [label.name for label in issue.get_labels()]
