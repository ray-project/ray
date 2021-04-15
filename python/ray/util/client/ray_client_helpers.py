from contextlib import contextmanager

import ray as real_ray
import ray.cloudpickle as pickle
import ray.util.client.server.server as ray_client_server
from ray.util.client import ray


@contextmanager
def ray_start_client_server(metadata=None):
    with ray_start_client_server_pair(metadata=metadata) as pair:
        client, server = pair
        yield client


@contextmanager
def ray_start_client_server_pair(metadata=None):
    ray._inside_client_test = True
    server = ray_client_server.serve("localhost:50051")
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
