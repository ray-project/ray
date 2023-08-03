from contextlib import contextmanager
import subprocess
import pytest
import ray

from ray.tests.conftest import get_default_fixture_ray_kwargs
from ray._private.test_utils import simulate_storage
from ray.cluster_utils import Cluster

# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
from ray.tests.conftest import pytest_runtest_makereport  # noqa
from ray.workflow.tests import utils


@contextmanager
def _init_cluster(storage_url, **params):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(**params)
    init_kwargs["storage"] = storage_url

    # Sometimes pytest does not cleanup all global variables.
    # we have to manually reset the workflow storage. This
    # should not be an issue for normal use cases, because global variables
    # are freed after the driver exits.
    ray.shutdown()
    subprocess.check_call(["ray", "stop", "--force"])
    init_kwargs["ray_client_server_port"] = 10001
    cluster = Cluster()
    init_kwargs.pop("namespace")  # we do not need namespace in workflow tests
    cluster.add_node(**init_kwargs)
    utils.clear_marks()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


@contextmanager
def _workflow_start(storage_url, shared, use_ray_client, **kwargs):
    assert use_ray_client in {"no_ray_client", "ray_client"}
    with _init_cluster(storage_url, **kwargs) as cluster:
        if use_ray_client == "ray_client":
            address = f"ray://{cluster.address.split(':')[0]}:10001"
        else:
            address = cluster.address

        ray.init(address=address)

        yield address


@pytest.fixture(scope="function")
def workflow_start_regular(storage_type, use_ray_client: str, request):
    param = getattr(request, "param", {})
    with simulate_storage(storage_type) as storage_url, _workflow_start(
        storage_url, False, use_ray_client, **param
    ) as res:
        yield res


@pytest.fixture(scope="module")
def workflow_start_regular_shared(storage_type, use_ray_client: str, request):
    param = getattr(request, "param", {})
    with simulate_storage(storage_type) as storage_url, _workflow_start(
        storage_url, True, use_ray_client, **param
    ) as res:
        yield res


@contextmanager
def _workflow_start_serve(storage_url, shared, use_ray_client, **kwargs):
    with _workflow_start(storage_url, True, use_ray_client, **kwargs) as address_info:
        ray.serve.start(detached=True)
        yield address_info

        # The code after the yield will run as teardown code.
        ray.serve.shutdown()


@pytest.fixture(scope="module")
def workflow_start_regular_shared_serve(storage_type, use_ray_client: str, request):
    param = getattr(request, "param", {})
    with simulate_storage(storage_type) as storage_url, _workflow_start_serve(
        storage_url, True, use_ray_client, **param
    ) as res:
        yield res


def _start_cluster_and_get_address(parameter: str) -> str:
    command_args = parameter.split(" ")
    out = ray._private.utils.decode(
        subprocess.check_output(command_args, stderr=subprocess.STDOUT)
    )
    # Get the redis address from the output.
    address_prefix = "--address='"
    address_location = out.find(address_prefix) + len(address_prefix)
    address = out[address_location:]
    address = address.split("'")[0]
    return address


@pytest.fixture(scope="function")
def workflow_start_cluster(storage_type, request):
    # This code follows the design of "call_ray_start" fixture.
    param = getattr(request, "param", {})
    with simulate_storage(storage_type) as storage_url:
        with _init_cluster(storage_url, **param) as cluster:
            yield cluster.address, storage_url


def pytest_generate_tests(metafunc):
    if "storage_type" in metafunc.fixturenames:
        metafunc.parametrize("storage_type", ["s3", "fs"], scope="session")
    if "use_ray_client" in metafunc.fixturenames:
        metafunc.parametrize(
            "use_ray_client", ["no_ray_client", "ray_client"], scope="session"
        )
