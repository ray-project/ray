from abc import ABC, abstractmethod
from typing import Any, Dict

from ray.train.v2._internal.execution.context import (
    get_train_context as get_internal_train_context,
)
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI


@PublicAPI(stability="stable")
class TrainContext(ABC):
    """Abstract interface for training context."""

    @Deprecated
    def get_metadata(self) -> Dict[str, Any]:
        """[Deprecated] User metadata dict passed to the Trainer constructor."""
        from ray.train.context import _GET_METADATA_DEPRECATION_MESSAGE

        raise DeprecationWarning(_GET_METADATA_DEPRECATION_MESSAGE)

    @Deprecated
    def get_trial_name(self) -> str:
        """[Deprecated] Trial name for the corresponding trial."""
        from ray.train.context import _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE

        raise DeprecationWarning(
            _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_name")
        )

    @Deprecated
    def get_trial_id(self) -> str:
        """[Deprecated] Trial id for the corresponding trial."""
        from ray.train.context import _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE

        raise DeprecationWarning(
            _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_id")
        )

    @Deprecated
    def get_trial_resources(self):
        """[Deprecated] Trial resources for the corresponding trial."""
        from ray.train.context import _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE

        raise DeprecationWarning(
            _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_resources")
        )

    @Deprecated
    def get_trial_dir(self) -> str:
        """[Deprecated] Log directory corresponding to the trial directory for a Tune session.
        This is deprecated for Ray Train and should no longer be called in Ray Train workers.

        If this directory is needed, please pass it into the `train_loop_config` directly.
        """
        from ray.train.context import _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE

        raise DeprecationWarning(
            _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_dir")
        )

    @abstractmethod
    def get_experiment_name(self) -> str:
        """Experiment name for the corresponding trial.

        Returns:
            The experiment name, which is the ``name`` of the run's ``RunConfig``.
        """
        pass

    @abstractmethod
    def get_world_size(self) -> int:
        """Get the current world size (i.e. total number of workers) for this run.

        Examples:
            >>> import ray.train
            >>> from ray.train.torch import TorchTrainer
            >>> NUM_WORKERS = 2
            >>> def train_fn_per_worker(config):
            ...     assert ray.train.get_context().get_world_size() == NUM_WORKERS
            >>> trainer = TorchTrainer(
            ...     train_fn_per_worker,
            ...     scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
            ... )
            >>> print("[ignore]"); trainer.fit()  # doctest: +ELLIPSIS
            [ignore]...

        Returns:
            The total number of workers in this run.
        """
        pass

    @abstractmethod
    def get_world_rank(self) -> int:
        """Get the world rank of this worker.

        Examples:
            >>> import ray.train
            >>> from ray.train.torch import TorchTrainer
            >>> def train_fn_per_worker(config):
            ...     if ray.train.get_context().get_world_rank() == 0:
            ...         print("Worker 0")
            >>> trainer = TorchTrainer(
            ...     train_fn_per_worker,
            ...     scaling_config=ray.train.ScalingConfig(num_workers=2),
            ... )
            >>> print("[ignore]"); trainer.fit()  # doctest: +ELLIPSIS
            [ignore]...

        Returns:
            This worker's rank among all workers in this run.
        """
        pass

    @abstractmethod
    def get_local_rank(self) -> int:
        """Get the local rank of this worker (rank of the worker on its node).

        Examples:
            >>> import ray.train
            >>> from ray.train.torch import TorchTrainer
            >>> def train_fn_per_worker(config):
            ...     if ray.train.get_context().get_local_rank() == 0:
            ...         print("Local rank 0 worker")
            >>> trainer = TorchTrainer(
            ...     train_fn_per_worker,
            ...     scaling_config=ray.train.ScalingConfig(num_workers=2),
            ... )
            >>> print("[ignore]"); trainer.fit()  # doctest: +ELLIPSIS
            [ignore]...

        Returns:
            This worker's rank among the workers on its node.
        """
        pass

    @abstractmethod
    def get_local_world_size(self) -> int:
        """Get the local world size of this node (i.e. number of workers on this node).

        Examples:
            >>> import ray.train
            >>> from ray.train.torch import TorchTrainer
            >>> def train_fn_per_worker():
            ...     print(ray.train.get_context().get_local_world_size())
            >>> trainer = TorchTrainer(
            ...     train_fn_per_worker,
            ...     scaling_config=ray.train.ScalingConfig(num_workers=2),
            ... )
            >>> print("[ignore]"); trainer.fit()  # doctest: +ELLIPSIS
            [ignore]...

        Returns:
            The number of workers running on this node.
        """
        pass

    @abstractmethod
    def get_node_rank(self) -> int:
        """Get the rank of this node.

        Examples:
            >>> import ray.train
            >>> from ray.train.torch import TorchTrainer
            >>> def train_fn_per_worker():
            ...     print(ray.train.get_context().get_node_rank())
            >>> trainer = TorchTrainer(
            ...     train_fn_per_worker,
            ...     scaling_config=ray.train.ScalingConfig(num_workers=1),
            ... )
            >>> print("[ignore]"); trainer.fit()  # doctest: +ELLIPSIS
            [ignore]...

        Returns:
            The rank of this node among the nodes participating in training.
        """
        pass

    @DeveloperAPI
    @abstractmethod
    def get_storage(self):
        """Returns the :class:`~ray.train._internal.storage.StorageContext` storage
        context which gives advanced access to the filesystem and paths
        configured through `RunConfig`.

        NOTE: This is a DeveloperAPI, and the `StorageContext` interface may change
        without notice between minor versions.
        """
        pass


@DeveloperAPI
class DistributedTrainContext(TrainContext):
    """Implementation of TrainContext for distributed mode."""

    def get_experiment_name(self) -> str:
        return get_internal_train_context().get_experiment_name()

    def get_world_size(self) -> int:
        return get_internal_train_context().get_world_size()

    def get_world_rank(self) -> int:
        return get_internal_train_context().get_world_rank()

    def get_local_rank(self) -> int:
        return get_internal_train_context().get_local_rank()

    def get_local_world_size(self) -> int:
        return get_internal_train_context().get_local_world_size()

    def get_node_rank(self) -> int:
        return get_internal_train_context().get_node_rank()

    def get_storage(self):
        return get_internal_train_context().get_storage()
