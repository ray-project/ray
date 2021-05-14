import pytest
import sys

import ray
import ray.util.client.server.server as ray_client_server
import ray.client_builder as client_builder


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
