import pytest


class TestBackwardsCompatibility:
    # List of ray public releases that included job submission functionality
    # that we want to test compatibility against
    RAY_VERSION_LIST = ["1.9.0"]
    # List of previous job submission API versions, the value of
    # CURRENT_VERSION in job/common.py
    SDK_VERSION_LIST = ["0"]

    def test_cli(self):
        pass

    def test_http(self):
        pass