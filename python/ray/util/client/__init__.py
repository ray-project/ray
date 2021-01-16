from typing import List, Tuple, Dict, Any

import logging

logger = logging.getLogger(__name__)


class RayAPIStub:
    """This class stands in as the replacement API for the `import ray` module.

    Much like the ray module, this mostly delegates the work to the
    _client_worker. As parts of the ray API are covered, they are piped through
    here or on the client worker API.
    """

    def __init__(self):
        from ray.util.client.api import ClientAPI
        self.api = ClientAPI()
        self.client_worker = None
        self._server = None
        self._connected_with_init = False
        self._inside_client_test = False

    def connect(self,
                conn_str: str,
                secure: bool = False,
                metadata: List[Tuple[str, str]] = None,
                connection_retries: int = 3) -> Dict[str, Any]:
        """Connect the Ray Client to a server.

        Args:
            conn_str: Connection string, in the form "[host]:port"
            secure: Whether to use a TLS secured gRPC channel
            metadata: gRPC metadata to send on connect

        Returns:
            Dictionary of connection info, e.g., {"num_clients": 1}.
        """
        # Delay imports until connect to avoid circular imports.
        from ray.util.client.worker import Worker
        import ray._private.client_mode_hook
        if self.client_worker is not None:
            if self._connected_with_init:
                return
            raise Exception(
                "ray.connect() called, but ray client is already connected")
        if not self._inside_client_test:
            # If we're calling a client connect specifically and we're not
            # currently in client mode, ensure we are.
            ray._private.client_mode_hook._explicitly_enable_client_mode()

        try:
            self.client_worker = Worker(
                conn_str,
                secure=secure,
                metadata=metadata,
                connection_retries=connection_retries)
            self.api.worker = self.client_worker
            return self.client_worker.connection_info()
        except Exception:
            self.disconnect()
            raise

    def disconnect(self):
        """Disconnect the Ray Client.
        """
        if self.client_worker is not None:
            self.client_worker.close()
        self.client_worker = None

    # remote can be called outside of a connection, which is why it
    # exists on the same API layer as connect() itself.
    def remote(self, *args, **kwargs):
        """remote is the hook stub passed on to replace `ray.remote`.

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
        return self.client_worker is not None

    def init(self, *args, **kwargs):
        if self._server is not None:
            raise Exception("Trying to start two instances of ray via client")
        import ray.util.client.server.server as ray_client_server
        self._server, address_info = ray_client_server.init_and_serve(
            "localhost:50051", *args, **kwargs)
        self.connect("localhost:50051")
        self._connected_with_init = True
        return address_info

    def shutdown(self, _exiting_interpreter=False):
        self.disconnect()
        import ray.util.client.server.server as ray_client_server
        if self._server is None:
            return
        ray_client_server.shutdown_with_server(self._server,
                                               _exiting_interpreter)
        self._server = None


ray = RayAPIStub()

# Someday we might add methods in this module so that someone who
# tries to `import ray_client as ray` -- as a module, instead of
# `from ray_client import ray` -- as the API stub
# still gets expected functionality. This is the way the ray package
# worked in the past.
#
# This really calls for PEP 562: https://www.python.org/dev/peps/pep-0562/
# But until Python 3.6 is EOL, here we are.
