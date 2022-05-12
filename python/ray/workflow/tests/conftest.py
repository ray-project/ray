from contextlib import contextmanager
import subprocess
import pytest
import ray

from ray.tests.conftest import get_default_fixture_ray_kwargs
from ray._private.test_utils import simulate_storage

# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
from ray.tests.conftest import pytest_runtest_makereport  # noqa
from ray.workflow.tests import utils
from ray.cluster_utils import Cluster


@contextmanager
def _workflow_start(storage_url, shared, client_mode, **kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    test_namespace = init_kwargs.pop("namespace")
    init_kwargs["storage"] = storage_url
    if ray.is_initialized():
        ray.shutdown()
    # Sometimes pytest does not cleanup all global variables.
    # we have to manually reset the workflow storage. This
    # should not be an issue for normal use cases, because global variables
    # are freed after the driver exits.
    cluster = Cluster()
    cluster.add_node(**init_kwargs)
    address_info = cluster.address
    if client_mode:
        cluster.head_node._ray_params.ray_client_server_port = "10004"
        cluster.head_node.start_ray_client_server()
        address_info = "ray://localhost:10004"
    ray.init(address=address_info, namespace=test_namespace)
    utils.clear_marks()
    ray.workflow.init()
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture(scope="function")
def workflow_start_regular(storage_type, client_mode, request):
    param = getattr(request, "param", {})
    with simulate_storage(storage_type) as storage_url, _workflow_start(
        storage_url, False, client_mode, **param
    ) as res:
        yield res


@pytest.fixture(scope="module")
def workflow_start_regular_shared(storage_type, client_mode, request):
    param = getattr(request, "param", {})
    with simulate_storage(storage_type) as storage_url, _workflow_start(
        storage_url, True, client_mode, **param
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
    if "client_mode" in metafunc.fixturenames:
        metafunc.parametrize("client_mode", [True, False], scope="session")
