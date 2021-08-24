from contextlib import closing
import socket
from pathlib import Path
from threading import Thread
from typing import Tuple, TYPE_CHECKING, Union

import ray

if TYPE_CHECKING:
    from ray.data import Dataset
    from ray.data.dataset_pipeline import DatasetPipeline

RayDataset = Union["Dataset", "DatasetPipeline"]


def get_address_and_port() -> Tuple[str, int]:
    """Returns the IP address and a free port on this node."""
    addr = ray.util.get_node_ip_address()

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]

    return addr, port


def construct_path(path: Path, parent_path: Path) -> Path:
    """Constructs a path relative to a parent.

    Args:
        path: A relative or absolute path.
        parent_path: A relative path or absolute path.

    Returns: An absolute path.
    """
    if path.expanduser().is_absolute():
        return path.expanduser().resolve()
    else:
        return parent_path.joinpath(path).expanduser().resolve()


class PropagatingThread(Thread):
    """A Thread subclass that stores exceptions and results."""

    def run(self):
        self.exc = None
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exc = e

    def join(self, timeout=None):
        super(PropagatingThread, self).join(timeout)
        if self.exc:
            raise self.exc
        return self.ret
