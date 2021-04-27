from contextlib import contextmanager

import ray as real_ray
import ray.cloudpickle as pickle
import ray.util.client.server.server as ray_client_server
from ray.util.client import ray
from ray._private.client_mode_hook import enable_client_mode


@contextmanager
def ray_start_client_server(metadata=None, ray_connect_handler=None):
    with ray_start_client_server_pair(
            metadata=metadata,
            ray_connect_handler=ray_connect_handler) as pair:
        client, server = pair
        yield client


@contextmanager
def ray_start_client_server_pair(metadata=None, ray_connect_handler=None):
    ray._inside_client_test = True
    server = ray_client_server.serve(
        "localhost:50051", ray_connect_handler=ray_connect_handler)
    ray.connect("localhost:50051", metadata=metadata)
    try:
        yield ray, server
    finally:
        ray._inside_client_test = False
        ray.disconnect()
        server.stop(0)


@contextmanager
def ray_start_cluster_client_server_pair(address):
    ray._inside_client_test = True

    def ray_connect_handler(job_config=None):
        real_ray.init(address=address)

    server = ray_client_server.serve(
        "localhost:50051", ray_connect_handler=ray_connect_handler)
    ray.connect("localhost:50051")
    try:
        yield ray, server
    finally:
        ray._inside_client_test = False
        ray.disconnect()
        server.stop(0)


class RayClientSerializationContext:
    # NOTE(simon): Used for registering custom serializers. We cannot directly
    # use the SerializationContext because it requires Ray workers. Please
    # make sure to keep the API consistent.

    def _unregister_cloudpickle_reducer(self, cls):
        pickle.CloudPickler.dispatch.pop(cls, None)

    def _register_cloudpickle_serializer(self, cls, custom_serializer,
                                         custom_deserializer):
        def _CloudPicklerReducer(obj):
            return custom_deserializer, (custom_serializer(obj), )

        # construct a reducer
        pickle.CloudPickler.dispatch[cls] = _CloudPicklerReducer


@contextmanager
def connect_to_client_or_not(connect_to_client: bool):
    """Utility for running test logic with and without a Ray client connection.

    If client_connect is True, will connect to Ray client in context.
    If client_connect is False, does nothing.

    How to use:
    Given a test of the following form:

    def test_<name>(args):
        <initialize a ray cluster>
        <use the ray cluster>

    Modify the test to

    @pytest.mark.parametrize("connect_to_client", [False, True])
    def test_<name>(args, connect_to_client)
    <initialize a ray cluster>
    with connect_to_client_or_not(connect_to_client):
        <use the ray cluster>

    Parameterize the argument connect over True, False to run the test with and
    without a Ray client connection.
    """

    if connect_to_client:
        with ray_start_client_server(), enable_client_mode():
            yield
    else:
        yield
