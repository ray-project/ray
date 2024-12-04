# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
import copy

import pytest

import ray
from ray.tests.conftest import pytest_runtest_makereport  # noqa


@pytest.fixture
def restore_data_context(request):
    """Restore any DataContext changes after the test runs"""
    original = copy.deepcopy(ray.data.context.DataContext.get_current())
    yield
    ray.data.context.DataContext._set_current(original)
