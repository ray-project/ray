import os
from concurrent import futures
from typing import Any, Optional, Sequence, Tuple

import grpc
from grpc import aio as aiogrpc

import ray
from ray._private.authentication import authentication_utils
from ray._private.tls_utils import load_certs_from_env


def init_grpc_channel(
    address: str,
    options: Optional[Sequence[Tuple[str, Any]]] = None,
    asynchronous: bool = False,
    credentials: Optional[grpc.ChannelCredentials] = None,
):
    """Create a gRPC channel with authentication interceptors if token auth is enabled.

    This function handles:
    - TLS configuration via RAY_USE_TLS environment variable or custom credentials
    - Authentication interceptors when token auth is enabled
    - Keepalive settings from Ray config
    - Both synchronous and asynchronous channels

    Args:
        address: The gRPC server address (host:port)
        options: Optional gRPC channel options as sequence of (key, value) tuples
        asynchronous: If True, create async channel; otherwise sync
        credentials: Optional custom gRPC credentials for TLS. If provided, takes
            precedence over RAY_USE_TLS environment variable.

    Returns:
        grpc.Channel or grpc.aio.Channel: Configured gRPC channel with interceptors
    """
    grpc_module = aiogrpc if asynchronous else grpc

    options = options or []
    options_dict = dict(options)
    options_dict["grpc.keepalive_time_ms"] = options_dict.get(
        "grpc.keepalive_time_ms", ray._config.grpc_client_keepalive_time_ms()
    )
    options_dict["grpc.keepalive_timeout_ms"] = options_dict.get(
        "grpc.keepalive_timeout_ms", ray._config.grpc_client_keepalive_timeout_ms()
    )
    options = options_dict.items()

    # Build interceptors list
    interceptors = []
    if authentication_utils.is_token_auth_enabled():
        from ray._private.authentication.grpc_authentication_client_interceptor import (
            SyncAuthenticationMetadataClientInterceptor,
            get_async_auth_interceptors,
        )

        if asynchronous:
            interceptors.extend(get_async_auth_interceptors())
        else:
            interceptors.append(SyncAuthenticationMetadataClientInterceptor())

    # Determine channel type and credentials
    if credentials is not None:
        # Use provided custom credentials (takes precedence)
        channel_creator = grpc_module.secure_channel
        base_args = (address, credentials)
    elif os.environ.get("RAY_USE_TLS", "0").lower() in ("1", "true"):
        # Use TLS from environment variables
        server_cert_chain, private_key, ca_cert = load_certs_from_env()
        tls_credentials = grpc.ssl_channel_credentials(
            certificate_chain=server_cert_chain,
            private_key=private_key,
            root_certificates=ca_cert,
        )
        channel_creator = grpc_module.secure_channel
        base_args = (address, tls_credentials)
    else:
        # Insecure channel
        channel_creator = grpc_module.insecure_channel
        base_args = (address,)

    # Create channel (async channels get interceptors in constructor, sync via intercept_channel)
    if asynchronous:
        channel = channel_creator(
            *base_args, options=options, interceptors=interceptors
        )
    else:
        channel = channel_creator(*base_args, options=options)
        if interceptors:
            channel = grpc.intercept_channel(channel, *interceptors)

    return channel


def create_grpc_server_with_interceptors(
    max_workers: Optional[int] = None,
    thread_name_prefix: str = "grpc_server",
    options: Optional[Sequence[Tuple[str, Any]]] = None,
    asynchronous: bool = False,
):
    """Create a gRPC server with authentication interceptors if token auth is enabled.

    This function handles:
    - Authentication interceptors when token auth is enabled
    - Both synchronous and asynchronous servers
    - Thread pool configuration for sync servers

    Args:
        max_workers: Max thread pool workers (required for sync, ignored for async)
        thread_name_prefix: Thread name prefix for sync thread pool
        options: Optional gRPC server options as sequence of (key, value) tuples
        asynchronous: If True, create async server; otherwise sync

    Returns:
        grpc.Server or grpc.aio.Server: Configured gRPC server with interceptors
    """
    grpc_module = aiogrpc if asynchronous else grpc

    # Build interceptors list
    interceptors = []
    if authentication_utils.is_token_auth_enabled():
        if asynchronous:
            from ray._private.authentication.grpc_authentication_server_interceptor import (
                AsyncAuthenticationServerInterceptor,
            )

            interceptors.append(AsyncAuthenticationServerInterceptor())
        else:
            from ray._private.authentication.grpc_authentication_server_interceptor import (
                SyncAuthenticationServerInterceptor,
            )

            interceptors.append(SyncAuthenticationServerInterceptor())

    # Create server
    if asynchronous:
        server = grpc_module.server(
            interceptors=interceptors if interceptors else None,
            options=options,
        )
    else:
        if max_workers is None:
            raise ValueError("max_workers is required for synchronous gRPC servers")

        executor = futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
        )
        server = grpc_module.server(
            executor,
            interceptors=interceptors if interceptors else None,
            options=options,
        )

    return server
