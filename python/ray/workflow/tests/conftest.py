from contextlib import contextmanager
import pytest
import ray

from ray.tests.conftest import get_default_fixture_ray_kwargs
from ray._private.test_utils import simulate_storage

# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
from ray.tests.conftest import pytest_runtest_makereport  # noqa
from ray.workflow.tests import utils


@contextmanager
def _workflow_start(storage_url, shared, **kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    init_kwargs["storage"] = storage_url
    if ray.is_initialized():
        ray.shutdown()
    # Sometimes pytest does not cleanup all global variables.
    # we have to manually reset the workflow storage. This
    # should not be an issue for normal use cases, because global variables
    # are freed after the driver exits.
    address_info = ray.init(**init_kwargs)
    utils.clear_marks()
    ray.workflow.init()
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture(scope="function")
def workflow_start_regular(storage_type, request):
    param = getattr(request, "param", {})
    with simulate_storage(storage_type) as storage_url, _workflow_start(
        storage_url, False, **param
    ) as res:
        yield res


@pytest.fixture(scope="module")
def workflow_start_regular_shared(storage_type, request):
    param = getattr(request, "param", {})
    with simulate_storage(storage_type) as storage_url, _workflow_start(
        storage_url, True, **param
    ) as res:
        yield res


def pytest_generate_tests(metafunc):
    if "storage_type" in metafunc.fixturenames:
        metafunc.parametrize("storage_type", ["s3", "fs"], scope="session")
