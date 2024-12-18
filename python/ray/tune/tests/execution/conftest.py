# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
from ray.tests.conftest import propagate_logs  # noqa
from ray.tests.conftest import pytest_runtest_makereport  # noqa
