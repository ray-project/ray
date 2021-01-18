from ray.util.client import ray

from ray._private.client_mode_hook import _enable_client_hook
from ray._private.client_mode_hook import _explicitly_enable_client_mode

from typing import List, Tuple, Dict, Any


def connect(conn_str: str,
            secure: bool = False,
            metadata: List[Tuple[str, str]] = None,
            connection_retries: int = 3) -> Dict[str, Any]:
    if ray.is_connected():
        raise RuntimeError("Ray Client is already connected. "
                           "Maybe you called ray.util.connect twice by "
                           "accident?")
    # Enable the same hooks that RAY_CLIENT_MODE does, as
    # calling ray.util.connect() is specifically for using client mode.
    _enable_client_hook(True)
    _explicitly_enable_client_mode()

    # TODO(barakmich): https://github.com/ray-project/ray/issues/13274
    # for supporting things like cert_path, ca_path, etc and creating
    # the correct metadata
    return ray.connect(
        conn_str, secure=secure, metadata=metadata, connection_retries=3)


def disconnect():
    if not ray.is_connected():
        raise RuntimeError("Ray Client is currently disconnected.")
    return ray.disconnect()
