from ray.util.annotations import PublicAPI, Deprecated, DeveloperAPI
from ray.train.v2._internal.execution.context import (
    get_train_context as get_internal_train_context,
)
from typing import Any, Dict


@PublicAPI(stability="stable")
class TrainContext:
    @Deprecated
    def get_metadata(self) -> Dict[str, Any]:
        """User metadata dict passed to the Trainer constructor."""
        from ray.train.context import _GET_METADATA_DEPRECATION_MESSAGE

        raise DeprecationWarning(_GET_METADATA_DEPRECATION_MESSAGE)

    @Deprecated
    def get_trial_name(self) -> str:
        """Trial name for the corresponding trial."""
        from ray.train.context import _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE

        raise DeprecationWarning(
            _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_name")
        )

    @Deprecated
    def get_trial_id(self) -> str:
        """Trial id for the corresponding trial."""
        from ray.train.context import _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE

        raise DeprecationWarning(
            _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_id")
        )

    @Deprecated
    def get_trial_resources(self):
        """Trial resources for the corresponding trial."""
        from ray.train.context import _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE

        raise DeprecationWarning(
            _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_resources")
        )

    @Deprecated
    def get_trial_dir(self) -> str:
        """Log directory corresponding to the trial directory for a Tune session.
        This is deprecated for Ray Train and should no longer be called in Ray Train workers.

        If this directory is needed, please pass it into the `train_loop_config` directly.
        """
        from ray.train.context import _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE

        raise DeprecationWarning(
            _TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_dir")
        )

    def get_experiment_name(self) -> str:
        """Experiment name for the corresponding trial."""
        return get_internal_train_context().get_experiment_name()

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
        return get_internal_train_context().get_world_size()

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
        return get_internal_train_context().get_world_rank()

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
        return get_internal_train_context().get_local_rank()

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
        return get_internal_train_context().get_local_world_size()

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
        return get_internal_train_context().get_node_rank()

    @DeveloperAPI
    def get_storage(self):
        """Returns the :class:`~ray.train._internal.storage.StorageContext` storage
        context which gives advanced access to the filesystem and paths
        configured through `RunConfig`.

        NOTE: This is a developer API, and the `StorageContext` interface may change
        without notice between minor versions.
        """
        return get_internal_train_context().get_storage()
