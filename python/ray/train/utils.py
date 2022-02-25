import abc
import os
import logging
from pathlib import Path
from threading import Thread

from typing import Tuple, Dict, List, Any, TYPE_CHECKING, Union

import ray
from ray.exceptions import RayActorError
from ray.types import ObjectRef
from ray.util.ml_utils.util import find_free_port

if TYPE_CHECKING:
    from ray.data import Dataset
    from ray.data.dataset_pipeline import DatasetPipeline

RayDataset = Union["Dataset", "DatasetPipeline"]

logger = logging.getLogger(__name__)


def check_for_failure(remote_values: List[ObjectRef]) -> Tuple[bool, List[int]]:
    """Check for actor failure when retrieving the remote values.

    Args:
        remote_values (list): List of object references from Ray actor methods.

    Returns:
        Returns Tuple of success boolean and list of workers indexes that fail.
    """
    unfinished = remote_values.copy()
    dead_worker_indexes = []  # Store the indexes of the failed workers.
    while len(unfinished) > 0:
        finished, unfinished = ray.wait(unfinished)
        # If a failure occurs the ObjectRef will be marked as finished.
        # Calling ray.get will expose the failure as a RayActorError.
        for object_ref in finished:
            try:
                ray.get(object_ref)
            except RayActorError as exc:
                logger.exception(str(exc))
                failed_actor_rank = remote_values.index(object_ref)
                logger.info(f"Worker {failed_actor_rank} has failed.")
                dead_worker_indexes.append(failed_actor_rank)
    if len(dead_worker_indexes) > 0:
        return False, dead_worker_indexes
    else:
        return True, []


def get_address_and_port() -> Tuple[str, int]:
    """Returns the IP address and a free port on this node."""
    addr = ray.util.get_node_ip_address()
    port = find_free_port()

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


def update_env_vars(env_vars: Dict[str, Any]):
    """Updates the environment variables on this worker process.

    Args:
        env_vars (Dict): Environment variables to set.
    """
    sanitized = {k: str(v) for k, v in env_vars.items()}
    os.environ.update(sanitized)


class Singleton(abc.ABCMeta):
    """Singleton Abstract Base Class

    https://stackoverflow.com/questions/33364070/implementing
    -singleton-as-metaclass-but-for-abstract-classes
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
