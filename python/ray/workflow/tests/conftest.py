from contextlib import contextmanager
import subprocess
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


@pytest.fixture(scope="function")
def workflow_start_cluster(storage_type, request):
    # This code follows the design of "call_ray_start" fixture.
    with simulate_storage(storage_type) as storage_url:
        utils.clear_marks()
        parameter = getattr(
            request,
            "param",
            "ray start --head --num-cpus=1 --min-worker-port=0 "
            "--max-worker-port=0 --port 0 --storage=" + storage_url,
        )
        command_args = parameter.split(" ")
        out = ray._private.utils.decode(
            subprocess.check_output(command_args, stderr=subprocess.STDOUT)
        )
        # Get the redis address from the output.
        address_prefix = "--address='"
        address_location = out.find(address_prefix) + len(address_prefix)
        address = out[address_location:]
        address = address.split("'")[0]

        yield address, storage_url

        # Disconnect from the Ray cluster.
        ray.shutdown()
        # Kill the Ray cluster.
        subprocess.check_call(["ray", "stop"])


def pytest_generate_tests(metafunc):
    if "storage_type" in metafunc.fixturenames:
        metafunc.parametrize("storage_type", ["s3", "fs"], scope="session")
