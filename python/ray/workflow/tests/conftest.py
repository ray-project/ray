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
def _workflow_start(storage_url, shared, use_ray_client, **kwargs):
    assert use_ray_client in {"no_ray_client", "ray_client"}
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    init_kwargs["storage"] = storage_url
    ray.shutdown()
    if use_ray_client:
        # Kill the Ray cluster.
        subprocess.check_call(["ray", "stop"])
    # Sometimes pytest does not cleanup all global variables.
    # we have to manually reset the workflow storage. This
    # should not be an issue for normal use cases, because global variables
    # are freed after the driver exits.
    if use_ray_client == "ray_client":
        subprocess.check_call(["ray", "stop", "--force"])
        init_kwargs["ray_client_server_port"] = 10001
        cluster = Cluster()
        namespace = init_kwargs.pop("namespace")
        cluster.add_node(**init_kwargs)
        address_info = ray.init(
            address=f"ray://{cluster.address.split(':')[0]}:10001", namespace=namespace
        )
    else:
        address_info = ray.init(**init_kwargs)
    utils.clear_marks()
    ray.workflow.init()
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    if use_ray_client == "ray_client":
        cluster.shutdown()
    subprocess.check_call(["ray", "stop"])


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
    with simulate_storage(storage_type) as storage_url:
        utils.clear_marks()
        parameter = getattr(
            request,
            "param",
            "ray start --head --num-cpus=1 --min-worker-port=0 "
            "--max-worker-port=0 --port 0 --storage=" + storage_url,
        )
        address = _start_cluster_and_get_address(parameter)

        yield address, storage_url

        # Disconnect from the Ray cluster.
        ray.shutdown()
        # Kill the Ray cluster.
        subprocess.check_call(["ray", "stop"])


def pytest_generate_tests(metafunc):
    if "storage_type" in metafunc.fixturenames:
        metafunc.parametrize("storage_type", ["s3", "fs"], scope="session")
    if "use_ray_client" in metafunc.fixturenames:
        metafunc.parametrize(
            "use_ray_client", ["no_ray_client", "ray_client"], scope="session"
        )
