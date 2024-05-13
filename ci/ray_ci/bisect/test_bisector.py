import sys
import pytest
from unittest import mock

from ci.ray_ci.bisect.bisector import Bisector
from ci.ray_ci.bisect.macos_validator import MacOSValidator
from ray_release.test import Test


@mock.patch("ci.ray_ci.bisect.bisector.Bisector._checkout_and_validate")
@mock.patch("ci.ray_ci.bisect.bisector.Bisector._get_revision_lists")
def test_run(mock_get_revision_lists, mock_checkout_and_validate):
    def _mock_checkout_and_validate(revision):
        return True if revision in ["1", "2", "3"] else False

    mock_checkout_and_validate.side_effect = _mock_checkout_and_validate
    mock_get_revision_lists.return_value = ["1", "2", "3", "4", "5"]

    # Test case 1: P P P F F
    assert Bisector(Test(), "1", "5", MacOSValidator(), "dir").run() == "3"

    # Test case 2: P F
    assert Bisector(Test(), "3", "4", MacOSValidator(), "dir").run() == "3"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
