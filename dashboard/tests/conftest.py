import os
import pytest
from ray.tests.conftest import *  # noqa


@pytest.fixture
def enable_test_module():
    os.environ["RAY_DASHBOARD_MODULE_TEST"] = "true"


@pytest.fixture
def disable_test_module():
    os.environ.pop("RAY_DASHBOARD_MODULE_TEST", None)
