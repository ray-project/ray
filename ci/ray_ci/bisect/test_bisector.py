import sys
from unittest import mock

import pytest

from ci.ray_ci.bisect.bisector import Bisector
from ci.ray_ci.bisect.macos_validator import MacOSValidator
from ci.ray_ci.bisect.validator import Validator

from ray_release.test import Test


class MockValidator(Validator):
    def __init__(self, return_value: bool) -> None:
        self.return_value = return_value

    def run(self, test: Test, revision: str) -> bool:
        return self.return_value


@mock.patch("ci.ray_ci.bisect.bisector.Bisector._checkout_and_validate")
@mock.patch("ci.ray_ci.bisect.bisector.Bisector._get_revision_lists")
def test_run(mock_get_revision_lists, mock_checkout_and_validate):
    def _mock_checkout_and_validate(revision):
        return True if revision in ["1", "2", "3"] else False

    mock_checkout_and_validate.side_effect = _mock_checkout_and_validate
    mock_get_revision_lists.return_value = ["1", "2", "3", "4", "5"]

    # Test case 1: T T T F F
    assert Bisector(Test(), "1", "5", MacOSValidator(), "dir").run() == "4"

    # Test case 2: T F
    assert Bisector(Test(), "3", "4", MacOSValidator(), "dir").run() == "4"

    # Test case 3: T F F
    assert Bisector(Test(), "3", "5", MacOSValidator(), "dir").run() == "4"


@mock.patch("subprocess.check_call")
def test_checkout_and_validate(mock_check_call):
    assert Bisector(
        Test(), "1", "5", MockValidator(True), "dir"
    )._checkout_and_validate("1")
    assert not Bisector(
        Test(), "1", "5", MockValidator(False), "dir"
    )._checkout_and_validate("1")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
