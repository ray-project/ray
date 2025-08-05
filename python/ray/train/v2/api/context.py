from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import threading

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
        """Experiment name for the corresponding trial."""
        pass

    @abstractmethod
    def get_world_size(self) -> int:
        """Get the current world size (i.e. total number of workers) for this run.

        .. testcode::

            import ray
            from ray import train
            from ray.train import ScalingConfig
            from ray.train.tensorflow import TensorflowTrainer

            NUM_WORKERS = 2

            def train_loop_per_worker(config):
                assert train.get_context().get_world_size() == NUM_WORKERS

            trainer = TensorflowTrainer(
                train_loop_per_worker,
                scaling_config=ScalingConfig(num_workers=NUM_WORKERS),
            )
            trainer.fit()

        .. testoutput::
            :hide:

            ...
        """
        pass

    @abstractmethod
    def get_world_rank(self) -> int:
        """Get the world rank of this worker.

        .. testcode::

            import ray
            from ray import train
            from ray.train import ScalingConfig
            from ray.train.tensorflow import TensorflowTrainer

            def train_loop_per_worker(config):
                if train.get_context().get_world_rank() == 0:
                    print("Worker 0")

            trainer = TensorflowTrainer(
                train_loop_per_worker,
                scaling_config=ScalingConfig(num_workers=2),
            )
            trainer.fit()

        .. testoutput::
            :hide:

            ...
        """
        pass

    @abstractmethod
    def get_local_rank(self) -> int:
        """Get the local rank of this worker (rank of the worker on its node).

        .. testcode::

            import torch

            import ray
            from ray import train
            from ray.train import ScalingConfig
            from ray.train.torch import TorchTrainer

            def train_loop_per_worker(config):
                if torch.cuda.is_available():
                    torch.cuda.set_device(train.get_context().get_local_rank())
                ...

            trainer = TorchTrainer(
                train_loop_per_worker,
                scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
            )
            trainer.fit()

        .. testoutput::
            :hide:

            ...
        """
        pass

    @abstractmethod
    def get_local_world_size(self) -> int:
        """Get the local world size of this node (i.e. number of workers on this node).

        Example:

            .. testcode::

                import ray
                from ray import train
                from ray.train import ScalingConfig
                from ray.train.torch import TorchTrainer

                def train_loop_per_worker():
                    print(train.get_context().get_local_world_size())

                trainer = TorchTrainer(
                    train_loop_per_worker,
                    scaling_config=ScalingConfig(num_workers=1),
                )
                trainer.fit()

            .. testoutput::
                :hide:

                ...
        """
        pass

    @abstractmethod
    def get_node_rank(self) -> int:
        """Get the rank of this node.

        Example:

            .. testcode::

                import ray
                from ray import train
                from ray.train import ScalingConfig
                from ray.train.torch import TorchTrainer

                def train_loop_per_worker():
                    print(train.get_context().get_node_rank())

                trainer = TorchTrainer(
                    train_loop_per_worker,
                    scaling_config=ScalingConfig(num_workers=1),
                )
                trainer.fit()

            .. testoutput::
                :hide:

                ...
        """
        pass

    @DeveloperAPI
    @abstractmethod
    def get_storage(self):
        """Returns the :class:`~ray.train._internal.storage.StorageContext` storage
        context which gives advanced access to the filesystem and paths
        configured through `RunConfig`.

        NOTE: This is a developer API, and the `StorageContext` interface may change
        without notice between minor versions.
        """
        pass

    @DeveloperAPI
    @abstractmethod
    def is_running_in_local_mode(self) -> bool:
        """Returns whether the training is running in local mode, like launched by torchrun.
        """
        pass


class DistributedTrainContext(TrainContext):
    """Implementation of TrainContext for distributed training."""

    def get_experiment_name(self) -> str:
        """Experiment name for the corresponding trial."""
        return get_internal_train_context().get_experiment_name()

    def get_world_size(self) -> int:
        """Get the current world size (i.e. total number of workers) for this run."""
        return get_internal_train_context().get_world_size()

    def get_world_rank(self) -> int:
        """Get the world rank of this worker."""
        return get_internal_train_context().get_world_rank()

    def get_local_rank(self) -> int:
        """Get the local rank of this worker (rank of the worker on its node)."""
        return get_internal_train_context().get_local_rank()

    def get_local_world_size(self) -> int:
        """Get the local world size of this node (i.e. number of workers on this node)."""
        return get_internal_train_context().get_local_world_size()

    def get_node_rank(self) -> int:
        """Get the rank of this node."""
        return get_internal_train_context().get_node_rank()

    def get_storage(self):
        """Returns the storage context for distributed training."""
        return get_internal_train_context().get_storage()

    def is_running_in_local_mode(self) -> bool:
        return False


class LocalRunningTrainContext(TrainContext):
    """Implementation of TrainContext for local (non-distributed) training."""

    def __init__(
        self,
        experiment_name: str,
        local_world_size: int,
        local_rank: int,
    ):
        self.experiment_name = experiment_name
        self.local_rank = local_rank
        self.local_world_size = local_world_size

    def get_experiment_name(self) -> str:
        """Experiment name for the corresponding trial."""
        return self.experiment_name

    def get_world_size(self) -> int:
        """Get the current world size (always 1 for local training)."""
        return self.local_world_size

    def get_world_rank(self) -> int:
        """Get the world rank of this worker (always 0 for local training)."""
        return self.local_rank

    def get_local_rank(self) -> int:
        """Get the local rank of this worker (always 0 for local training)."""
        return self.local_rank

    def get_local_world_size(self) -> int:
        """Get the local world size of this node (always 1 for local training)."""
        return self.local_world_size

    def get_node_rank(self) -> int:
        """Get the rank of this node (always 0 for local training)."""
        return 0

    def get_storage(self):
        """Returns a basic storage context for local training."""
        # For local training, we might want to return a simple storage implementation
        # This would need to be implemented based on the actual StorageContext interface
        # For now, we'll raise NotImplementedError to indicate this needs implementation
        raise NotImplementedError(
            "Local storage context not yet implemented. "
            "Please use DistributedTrainContext for full storage support."
        )

    def is_running_in_local_mode(self) -> bool:
        return True

_train_context: Optional[TrainContext] = None
_context_lock = threading.Lock()

def set_train_context(context: TrainContext) -> None:
    global _train_context
    assert _train_context is None, "TrainContext cannot be reinitialized."
    with _context_lock:
        _train_context = context


@PublicAPI(stability="stable")
def get_context() -> TrainContext:
    """Get or create a singleton training context.

    The context is only available within a function passed to Ray Train.

    See the :class:`~ray.train.TrainContext` API reference to see available methods.
    """
    # TODO: Return a dummy train context on the controller and driver process
    # instead of raising an exception if the train context does not exist.
    global _train_context
    with _context_lock:
        if _train_context is not None:
            return _train_context
    return TrainContext()
