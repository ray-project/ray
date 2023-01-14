import math
from typing import Any, List, Mapping, Type, Optional, Callable

import ray

from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.core.rl_trainer.rl_trainer import (
    RLTrainer,
    ParamOptimizerPairs,
    Optimizer,
)
from ray.rllib.core.rl_trainer.tf.tf_rl_trainer import TfRLTrainer
from ray.rllib.core.rl_trainer.torch.torch_rl_trainer import TorchRLTrainer
from ray.rllib.policy.sample_batch import MultiAgentBatch


from ray.air.config import ScalingConfig
from ray.train._internal.backend_executor import BackendExecutor


class TrainerRunner:
    """Coordinator of RLTrainers.
    Public API:
        .update(batch) -> updates the RLModule based on gradient descent algos.
        .additional_update() -> any additional non-gradient based updates will get
                                called from this entry point.
        .get_state() -> returns the state of the RLModule and RLOptimizer from
                        all of the RLTrainers
        .set_state() -> sets the state of all the RLTrainers

    TODO(avnishn):
        1. Add trainer runner with async operations
        2. Use fault tolerant actor manager to handle failures
        3. Add from_xxx constructor pattern. For example
           add a `from_policy_map(self.local_worker().policy_map, cfg)`
           constructor to make it easier to create a TrainerRunner from a
           rollout worker.

    """

    def __init__(
        self,
        trainer_class: Type[RLTrainer],
        trainer_config: Mapping[str, Any],
        compute_config: Mapping[str, Any],
    ):
        # TODO: trainer_config and compute_config should become dataclasses.
        # It's hard for the user to know what the trainer / compute parameters are
        # expected.
        self._trainer_config = trainer_config
        self._compute_config = compute_config

        # TODO: remove the _compute_necessary_resources and just use
        # trainer_config["use_gpu"] and trainer_config["num_workers"]
        resources = self._compute_necessary_resources()
        scaling_config = ScalingConfig(
            num_workers=resources["num_workers"],
            use_gpu=resources["use_gpu"],
        )
        # the only part of this class that is framework agnostic:
        if issubclass(trainer_class, TorchRLTrainer):
            from ray.train.torch import TorchConfig

            backend_config = TorchConfig()
        elif issubclass(trainer_class, TfRLTrainer):
            from ray.train.tensorflow import TensorflowConfig

            backend_config = TensorflowConfig()
        else:
            raise ValueError("framework must be either torch or tf")

        self.backend_executor = BackendExecutor(
            backend_config=backend_config,
            num_workers=scaling_config.num_workers,
            num_cpus_per_worker=scaling_config.num_cpus_per_worker,
            num_gpus_per_worker=scaling_config.num_gpus_per_worker,
            max_retries=0,  # TODO: make this configurable in trainer_config
            # with default 0
        )

        # TODO: let's not pass this into the config which will cause
        # information leakage into the SARLTrainer about other workers.
        scaling_config = {"world_size": resources["num_workers"]}
        trainer_config["scaling_config"] = scaling_config
        trainer_config["distributed"] = bool(self._compute_config["num_gpus"] > 1)
        self.backend_executor.start(
            train_cls=trainer_class, train_cls_kwargs=trainer_config
        )
        self.workers = [w.actor for w in self.backend_executor.worker_group.workers]

        ray.get([w.build.remote() for w in self.workers])

    def _compute_necessary_resources(self):
        num_gpus = self._compute_config.get("num_gpus", 0)
        num_workers = self._compute_config.get("num_training_workers", 0)
        if num_workers and num_gpus:
            assert num_workers == num_gpus, (
                "If num_training_workers and "
                "num_gpus are specified it must be equal to num_gpus"
            )

        elif num_gpus and not num_workers:
            num_workers = num_gpus

        elif not num_gpus and not num_workers:
            num_workers = 1

        return {"num_workers": num_workers, "use_gpu": bool(num_gpus)}

    def update(self, batch: MultiAgentBatch = None, **kwargs):
        """
        Example:
            >>> trainer_runner.update(batch) # updates the gradient
        """
        refs = []
        global_size = len(self.workers)
        batch_size = math.ceil(len(batch) / global_size)
        for i, worker in enumerate(self.workers):
            batch_to_send = {}
            for pid, sub_batch in batch.policy_batches.items():
                batch_size = math.ceil(len(sub_batch) / global_size)
                start = batch_size * i
                end = min(start + batch_size, len(sub_batch))
                batch_to_send[pid] = sub_batch[int(start) : int(end)]
            new_batch = MultiAgentBatch(batch_to_send, int(batch_size))
            refs.append(worker.update.remote(new_batch))

        return ray.get(refs)

    def additional_update(self, *args, **kwargs) -> List[Mapping[str, Any]]:
        """Apply additional non-gradient based updates to the RLTrainers.

        For example, this could be used to do a polyak averaging update
        of a target network in off policy algorithms like SAC or DQN.

        By default this is a pass through that calls `RLTrainer.additional_update`

        Args:
            *args: Arguments to pass to each RLTrainer.
            **kwargs: Keyword arguments to pass to each RLTrainer.

        Returns:
            A list of dictionaries of results from the updates from each worker.
        """
        refs = []
        for worker in self.workers:
            refs.append(worker.additional_update.remote(*args, **kwargs))
        return ray.get(refs)

    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_cls: Type[RLModule],
        module_kwargs: Mapping[str, Any],
        set_optimizer_fn: Optional[Callable[[RLModule], ParamOptimizerPairs]] = None,
        optimizer_cls: Optional[Type[Optimizer]] = None,
    ) -> None:
        """Add a module to the RLTrainers maintained by this TrainerRunner.

        Args:
            module_id: The id of the module to add.
            module_cls: The module class to add.
            module_kwargs: The config for the module.
            set_optimizer_fn: A function that takes in the module and returns a list of
                (param, optimizer) pairs. Each element in the tuple describes a
                parameter group that share the same optimizer object, if None, the
                default optimizer (obtained from the exiting optimizer dictionary) will
                be used.
            optimizer_cls: The optimizer class to use. If None, the set_optimizer_fn
                should be provided.
        """
        refs = []
        for worker in self.workers:
            ref = worker.add_module.remote(
                module_id=module_id,
                module_cls=module_cls,
                module_kwargs=module_kwargs,
                set_optimizer_fn=set_optimizer_fn,
                optimizer_cls=optimizer_cls,
            )
            refs.append(ref)
        ray.get(refs)

    def remove_module(self, module_id: ModuleID) -> None:
        """Remove a module from the RLTrainers maintained by this TrainerRunner.

        Args:
            module_id: The id of the module to remove.

        """
        refs = []
        for worker in self.workers:
            ref = worker.remove_module.remote(module_id)
            refs.append(ref)
        ray.get(refs)

    def get_state(self) -> List[Mapping[ModuleID, Mapping[str, Any]]]:
        """Get the states of the RLTrainers"""
        refs = []
        for worker in self.workers:
            refs.append(worker.get_state.remote())
        return ray.get(refs)

    def set_state(self, state: List[Mapping[ModuleID, Mapping[str, Any]]]):
        """Sets the states of the RLTrainers.

        Args:
            state: The state of the RLTrainers

        """
        refs = []
        for worker in self.workers:
            refs.append(worker.set_state.remote(state))
        return ray.get(refs)
