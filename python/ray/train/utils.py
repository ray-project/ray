import abc
from dataclasses import dataclass
import inspect
import os
import logging
from pathlib import Path
from threading import Thread

from typing import (
    Tuple,
    Dict,
    List,
    Any,
    TYPE_CHECKING,
    Union,
    Callable,
    TypeVar,
    Optional,
)

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayActorError
from ray.types import ObjectRef
from ray.util.ml_utils.util import find_free_port

if TYPE_CHECKING:
    from ray.data import Dataset
    from ray.data.dataset_pipeline import DatasetPipeline

RayDataset = Union["Dataset", "DatasetPipeline"]
T = TypeVar("T")

logger = logging.getLogger(__name__)


def check_for_failure(remote_values: List[ObjectRef]) -> bool:
    """Check for actor failure when retrieving the remote values.

    Args:
        remote_values (list): List of object references from Ray actor methods.

    Returns:
        True if evaluating all object references is successful, False otherwise.
    """
    unfinished = remote_values.copy()

    while len(unfinished) > 0:
        finished, unfinished = ray.wait(unfinished)

        # If a failure occurs the ObjectRef will be marked as finished.
        # Calling ray.get will expose the failure as a RayActorError.
        for object_ref in finished:
            # Everything in finished has either failed or completed
            # successfully.
            try:
                ray.get(object_ref)
            except RayActorError as exc:
                logger.exception(str(exc))
                failed_actor_rank = remote_values.index(object_ref)
                logger.info(f"Worker {failed_actor_rank} has failed.")
                return False

    return True


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


def construct_train_func(
    train_func: Union[Callable[[], T], Callable[[Dict[str, Any]], T]],
    config: Optional[Dict[str, Any]],
    fn_arg_name: Optional[str] = "train_func",
) -> Callable[[], T]:
    """Validates and constructs the training function to execute.
    Args:
        train_func (Callable): The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        config (Optional[Dict]): Configurations to pass into
            ``train_func``. If None then an empty Dict will be created.
        fn_arg_name (Optional[str]): The name of training function to use for error
            messages.
    Returns:
        A valid training function.
    Raises:
        ValueError: if the input ``train_func`` is invalid.
    """
    signature = inspect.signature(train_func)
    num_params = len(signature.parameters)
    if num_params > 1:
        err_msg = (
            f"{fn_arg_name} should take in 0 or 1 arguments, but it accepts "
            f"{num_params} arguments instead."
        )
        raise ValueError(err_msg)
    elif num_params == 1:
        config = {} if config is None else config
        return lambda: train_func(config)
    else:  # num_params == 0
        return train_func


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


class ActorWrapper:
    """Wraps an actor to provide same API as using the base class directly."""

    def __init__(self, actor: ActorHandle):
        self.actor = actor

    def __getattr__(self, item):
        # The below will fail if trying to access an attribute (not a method) from the
        # actor.
        actor_method = getattr(self.actor, item)
        return lambda *args, **kwargs: ray.get(actor_method.remote(*args, **kwargs))


@dataclass
class _RayDatasetSpec:
    """Configuration for Ray Datasets to pass to the training workers.

    dataset_or_dict: An optional Ray Dataset (or DatasetPipeline) or a dictionary of
        datasets to be sharded across all the training workers, which can be accessed
        from the training function via ``train.get_dataset_shard()``. Multiple Datasets
        can be passed in as a ``Dict`` that maps each name key to a Dataset value,
        and each Dataset can be accessed from the training function by passing in a
        `dataset_name` argument to ``train.get_dataset_shard()``.
    dataset_split_fn: An optional callbale to specify how the provided ``dataset``
        should be split across the training workers. It is expected to take in two
        arguments. The first one is the ``dataset``, just as is passed in to the
        ``_RayDatasetSpec``. The second argument is a list of the ActorHandles of the
        training workers (to use as locality hints). The Callable is expected to
        return a list of RayDatasets or a list of dictionaries of RayDatasets,
        with the length of the list equal to the length of the list of actor handles.
        If None is provided, the provided Ray Dataset(s) will be simply be split using
        the actor handles as locality hints.

    """

    dataset_or_dict: Optional[Union[RayDataset, Dict[str, RayDataset]]]
    dataset_split_fn: Optional[
        Callable[
            [Union[RayDataset, Dict[str, RayDataset]], List[ActorHandle]],
            List[Union[RayDataset, Dict[str, RayDataset]]],
        ]
    ] = None

    def _default_split_fn(
        self, training_worker_handles: List[ActorHandle]
    ) -> List[Optional[Union[RayDataset, Dict[str, RayDataset]]]]:
        def split_dataset(dataset_or_pipeline):
            return dataset_or_pipeline.split(
                len(training_worker_handles),
                equal=True,
                locality_hints=training_worker_handles,
            )

        if isinstance(self.dataset_or_dict, dict):
            # Return a smaller dict for each shard.
            dataset_shards = [{} for _ in range(len(self.worker_group))]
            for key, dataset in self.dataset_or_dict.items():
                split_datasets = split_dataset(dataset)
                assert len(split_datasets) == len(self.worker_group)
                for i in range(len(split_datasets)):
                    dataset_shards[i][key] = split_datasets[i]
            return dataset_shards
        else:
            # return a smaller RayDataset for each shard.
            return split_dataset(self.dataset_or_dict)

    def get_dataset_shards(
        self, training_worker_handles: List[ActorHandle]
    ) -> List[Optional[Union[RayDataset, Dict[str, RayDataset]]]]:
        """Returns Dataset splits based off the spec and the given training workers

        Args:
            training_worker_handles: A list of the training worker actor handles.

        Returns:
            A list of RayDataset shards or list of dictionaries of RayDataset shards,
                one for each training worker.

        """
        if self.dataset_or_dict is None:
            # If no Dataset is provided, return None for each shard.
            return [None] * len(training_worker_handles)

        if self.dataset_split_fn is None:
            return self._default_split_fn(training_worker_handles)
        else:
            splits = self.dataset_split_fn(
                self.dataset_or_dict, training_worker_handles
            )
            if not len(splits) == len(training_worker_handles):
                raise RuntimeError(
                    "The list of Datasets returned by the "
                    f"`dataset_split_fn`: {len(splits)} does not match "
                    f"the number of training workers: {len(training_worker_handles)}"
                )
            return splits
