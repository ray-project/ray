import sys
import pytest
from unittest import mock

from ci.ray_ci.macos_bisector import MacOSBisector


@mock.patch("ci.ray_ci.bisector.Bisector._checkout_and_validate")
@mock.patch("ci.ray_ci.bisector.Bisector._get_revision_lists")
def test_run(mock_get_revision_lists, mock_checkout_and_validate):
    def _mock_checkout_and_validate(revision):
        return True if revision in ["1", "2", "3"] else False

    mock_checkout_and_validate.side_effect = _mock_checkout_and_validate
    mock_get_revision_lists.return_value = ["1", "2", "3", "4", "5"]
    assert MacOSBisector("test", "1", "5").run() == "3"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
