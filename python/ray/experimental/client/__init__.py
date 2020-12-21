from typing import List, Tuple

import logging
import os

logger = logging.getLogger(__name__)


class RayAPIStub:
    """
    This class stands in as the replacement API for the `import ray` module.

    Much like the ray module, this mostly delegates the work to the
    _client_worker. As parts of the ray API are covered, they are piped through
    here or on the client worker API.
    """

    def __init__(self):
        from ray.experimental.client.api import ClientAPI
        self.api = ClientAPI()
        self.client_worker = None

    def connect(self,
                conn_str: str,
                secure: bool = False,
                metadata: List[Tuple[str, str]] = None) -> None:
        """
        Connect the Ray Client to a server.

        Args:
            conn_str: Connection string, in the form "[host]:port"
            secure: Whether to use a TLS secured gRPC channel
            metadata: gRPC metadata to send on connect
        """
        # Delay imports until connect to avoid circular imports.
        from ray.experimental.client.worker import Worker
        if self.client_worker is not None:
            raise Exception(
                "ray.connect() called, but ray client is already connected")
        self.client_worker = Worker(conn_str, secure=secure, metadata=metadata)
        self.api.worker = self.client_worker

    def disconnect(self):
        """
        Disconnect the Ray Client.
        """
        if self.client_worker is not None:
            self.client_worker.close()
        self.client_worker = None

    # remote can be called outside of a connection, which is why it
    # exists on the same API layer as connect() itself.
    def remote(self, *args, **kwargs):
        """
        remote is the hook stub passed on to replace `ray.remote`.

        This sets up remote functions or actors, as the decorator,
        but does not execute them.

        Args:
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        return self.api.remote(*args, **kwargs)

    def __getattr__(self, key: str):
        if not self.is_connected():
            raise Exception("Ray Client is not connected. "
                            "Please connect by calling `ray.connect`.")
        return getattr(self.api, key)

    def is_connected(self) -> bool:
        return self.api is not None

    def init(self, *args, **kwargs):
        if _is_client_test_env():
            global _test_server
            import ray.experimental.client.server.server as ray_client_server
            _test_server, address_info = ray_client_server.init_and_serve(
                "localhost:50051", test_mode=True, *args, **kwargs)
            self.connect("localhost:50051")
            return address_info
        else:
            raise NotImplementedError(
                "Please call ray.connect() in client mode")


ray = RayAPIStub()

_test_server = None


def _stop_test_server(*args):
    global _test_server
    _test_server.stop(*args)


def _is_client_test_env() -> bool:
    return os.environ.get("RAY_TEST_CLIENT_MODE") == "1"


# Someday we might add methods in this module so that someone who
# tries to `import ray_client as ray` -- as a module, instead of
# `from ray_client import ray` -- as the API stub
# still gets expected functionality. This is the way the ray package
# worked in the past.
#
# This really calls for PEP 562: https://www.python.org/dev/peps/pep-0562/
# But until Python 3.6 is EOL, here we are.
