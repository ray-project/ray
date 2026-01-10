# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
try:
    from ray.tests.conftest import pytest_runtest_makereport  # noqa: F401
except ImportError:
    # If ray.tests is not available (e.g., when running tests without full Ray installation),
    # define a no-op hook to avoid import errors
    import pytest

    @pytest.hookimpl(tryfirst=True, hookwrapper=True)
    def pytest_runtest_makereport(item, call):
        outcome = yield
        outcome.get_result()
