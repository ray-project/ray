# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
import copy
import os

import pytest

import ray
from ray.tests.conftest import (
    _isolate_token_auth_state,  # noqa: F401
    _restore_token_auth_env,  # noqa: F401
    _token_auth_env_baseline,  # noqa: F401
    pytest_runtest_makereport,  # noqa
)

# Keep the Parquet footer-reader pool tiny for these tests. The production
# default of 32 actors times out under CI parallelism; tests that need a larger
# pool can override with monkeypatch.setenv. Mirrored in
# python/ray/air/BUILD.bazel for bazel test targets.
os.environ.setdefault("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")


@pytest.fixture
def restore_data_context(request):
    """Restore any DataContext changes after the test runs"""
    original = copy.deepcopy(ray.data.context.DataContext.get_current())
    yield
    ray.data.context.DataContext._set_current(original)


@pytest.fixture
def disable_fallback_to_object_extension(request, restore_data_context):
    """Disables fallback to ArrowPythonObjectType"""
    ray.data.context.DataContext.get_current().enable_fallback_to_arrow_object_ext_type = (
        False
    )
