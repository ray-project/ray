import pytest
import subprocess
import sys

import ray
import ray.util.client.server.server as ray_client_server
import ray.client_builder as client_builder

from ray.cluster_utils import Cluster
from ray.test_utils import run_string_as_driver


@pytest.mark.parametrize("address", [
    "localhost:1234", "localhost:1234/url?params",
    "1.2.3.4/cluster-1?test_param=param1?"
])
def test_split_address(address):
    assert client_builder._split_address(address) == ("ray", address)

    specified_module = f"ray://{address}"
    assert client_builder._split_address(specified_module) == ("ray", address)

    specified_other_module = f"module://{address}"
    assert client_builder._split_address(specified_other_module) == ("module",
                                                                     address)


@pytest.mark.parametrize(
    "address",
    ["localhost", "1.2.3.4:1200", "ray://1.2.3.4:1200", "local", None])
def test_client(address):
    builder = client_builder.client(address)
    assert isinstance(builder, client_builder.ClientBuilder)
    if address in ("local", None):
        assert isinstance(builder, client_builder._LocalClientBuilder)
    else:
        assert type(builder) == client_builder.ClientBuilder
        assert builder.address == address.replace("ray://", "")


def test_namespace():
    """
    Most of the "checks" in this test case rely on the fact that
    `run_string_as_driver` will throw an exception if the driver string exits
    with a non-zero exit code (e.g. when the driver scripts throws an
    exception). Since all of these drivers start named, detached actors, the
    most likely failure case would be a collision of named actors if they're
    put in the same namespace.

    This test checks that:
    * When two drivers don't specify a namespace, they are placed in different
      anonymous namespaces.
    * When two drivers specify a namespace, they collide.
    * The namespace name (as provided by the runtime context) is correct.
    """
    cluster = Cluster()
    cluster.add_node(num_cpus=4, ray_client_server_port=50055)
    cluster.wait_for_nodes(1)

    template = """
import ray
ray.client("localhost:50055").namespace({namespace}).connect()

@ray.remote
class Foo:
    def ping(self):
        return "pong"

a = Foo.options(lifetime="detached", name="abc").remote()
ray.get(a.ping.remote())
print(ray.get_runtime_context().namespace)
    """

    anon_driver = template.format(namespace="None")
    run_string_as_driver(anon_driver)
    # This second run will fail if the actors don't run in separate anonymous
    # namespaces.
    run_string_as_driver(anon_driver)

    run_in_namespace = template.format(namespace="'namespace'")
    script_namespace = run_string_as_driver(run_in_namespace)
    # The second run fails because the actors are run in the same namespace.
    with pytest.raises(subprocess.CalledProcessError):
        run_string_as_driver(run_in_namespace)

    assert script_namespace.strip() == "namespace"


def test_connect_to_cluster(ray_start_regular_shared):
    server = ray_client_server.serve("localhost:50055")
    client_info = ray.client("localhost:50055").connect()

    assert client_info.dashboard_url == ray.worker.get_dashboard_url()
    python_version = ".".join([str(x) for x in list(sys.version_info)[:3]])
    assert client_info.python_version == python_version
    assert client_info.ray_version == ray.__version__
    assert client_info.ray_commit == ray.__commit__
    protocol_version = ray.util.client.CURRENT_PROTOCOL_VERSION
    assert client_info.protocol_version == protocol_version

    server.stop(0)
