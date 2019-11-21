import sys
if sys.version_info < (3, 0):
    raise ImportError("serve is Python 3 only.")

from ray.experimental.serve.api import (init, create_backend, create_endpoint,
                                        link, split, get_handle, stat,
                                        scale)  # noqa: E402

__all__ = [
    "init", "create_backend", "create_endpoint", "link", "split", "get_handle",
    "stat", "scale"
]
