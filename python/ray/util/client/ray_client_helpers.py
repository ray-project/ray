import time
from contextlib import contextmanager
from typing import Any, Dict

import ray as real_ray
import ray.util.client.server.server as ray_client_server
from ray._common.network_utils import build_address, get_localhost_ip
from ray._private.authentication.authentication_token_setup import (
    enable_token_auth_by_default,
    ensure_token_if_auth_enabled,
)
from ray._private.client_mode_hook import disable_client_hook
from ray.job_config import JobConfig
from ray.util.client import ray


@contextmanager
def ray_start_client_server(metadata=None, ray_connect_handler=None, **kwargs):
    with ray_start_client_server_pair(
        metadata=metadata, ray_connect_handler=ray_connect_handler, **kwargs
    ) as pair:
        client, server = pair
        yield client


@contextmanager
def ray_start_client_server_for_address(address):
    """Starts a Ray client server that initializes drivers at the specified address."""

    def connect_handler(
        job_config: JobConfig = None, **ray_init_kwargs: Dict[str, Any]
    ):
        import ray

        with disable_client_hook():
            if not ray.is_initialized():
                return ray.init(address, job_config=job_config, **ray_init_kwargs)

    with ray_start_client_server(ray_connect_handler=connect_handler) as ray:
        yield ray


@contextmanager
def ray_start_client_server_pair(metadata=None, ray_connect_handler=None, **kwargs):
    ray._inside_client_test = True
    with disable_client_hook():
        assert not ray.is_initialized()
    if ray_connect_handler is None:
        # The default handler's bare ray.init() turns on token auth for this
        # process when RAY_AUTH_MODE is unset. Do it before the server and
        # client exist, so neither sees the auth mode change under it.
        enable_token_auth_by_default()
        ensure_token_if_auth_enabled()
    server = ray_client_server.serve(
        get_localhost_ip(), 50051, ray_connect_handler=ray_connect_handler
    )
    ray.connect(build_address(get_localhost_ip(), 50051), metadata=metadata, **kwargs)
    try:
        yield ray, server
    finally:
        ray._inside_client_test = False
        ray.disconnect()
        server.stop(0)
        del server
        start = time.monotonic()
        with disable_client_hook():
            while ray.is_initialized():
                time.sleep(1)
                if time.monotonic() - start > 30:
                    raise RuntimeError("Failed to terminate Ray")
        # Allow windows to close processes before moving on
        time.sleep(3)


@contextmanager
def ray_start_cluster_client_server_pair(address):
    ray._inside_client_test = True

    def ray_connect_handler(job_config=None, **ray_init_kwargs):
        real_ray.init(address=address)

    server = ray_client_server.serve(
        get_localhost_ip(), 50051, ray_connect_handler=ray_connect_handler
    )
    ray.connect(build_address(get_localhost_ip(), 50051))
    try:
        yield ray, server
    finally:
        ray._inside_client_test = False
        ray.disconnect()
        server.stop(0)
