import math
from typing import Any, List, Mapping, Type, Optional, Callable, Dict, TYPE_CHECKING

import ray

from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    ModuleID,
    SingleAgentRLModuleSpec,
)
from ray.rllib.core.rl_trainer.rl_trainer import (
    RLTrainerSpec,
    ParamOptimizerPairs,
    Optimizer,
)
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.train._internal.backend_executor import BackendExecutor

if TYPE_CHECKING:
    from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer


def _get_backend_config(rl_trainer_class: Type["RLTrainer"]) -> str:
    if rl_trainer_class.framework == "torch":
        from ray.train.torch import TorchConfig

        backend_config = TorchConfig()
    elif rl_trainer_class.framework == "tf":
        from ray.train.tensorflow import TensorflowConfig

        backend_config = TensorflowConfig()
    else:
        raise ValueError("framework must be either torch or tf")

    return backend_config


class TrainerRunner:
    """Coordinator of RLTrainers.
    Public API:
        .update(batch) -> updates the RLModule based on gradient descent algos.
        .additional_update() -> any additional non-gradient based updates will get
                                called from this entry point.
        .get_state() -> returns the state of the RLModule and RLOptimizer from
                        all of the RLTrainers.
        .set_state() -> sets the state of all the RLTrainers.
        .add_module() -> add a new RLModule to the MultiAgentRLModule being trained by
                         this TrainerRunner.
        .remove_module() -> remove an RLModule from the MultiAgentRLModule being trained
                            by this TrainerRunner.

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
        rl_trainer_spec: RLTrainerSpec,
    ):
        scaling_config = rl_trainer_spec.trainer_scaling_config
        rl_trainer_class = rl_trainer_spec.rl_trainer_class

        # TODO (Kourosh): Go with a _remote flag instead of _is_local to be more
        # explicit
        self._is_local = scaling_config.num_workers == 0
        self._trainer = None
        self._workers = None

        if self._is_local:
            self._trainer = rl_trainer_class(**rl_trainer_spec.get_params_dict())
            self._trainer.build()
        else:
            backend_config = _get_backend_config(rl_trainer_class)
            backend_executor = BackendExecutor(
                backend_config=backend_config,
                num_workers=scaling_config.num_workers,
                num_cpus_per_worker=scaling_config.num_cpus_per_worker,
                num_gpus_per_worker=scaling_config.num_gpus_per_worker,
                max_retries=0,
            )

            backend_executor.start(
                train_cls=rl_trainer_class,
                train_cls_kwargs=rl_trainer_spec.get_params_dict(),
            )
            self._backend_executor = backend_executor

            self._workers = [w.actor for w in backend_executor.worker_group.workers]

            # run the neural network building code on remote workers
            ray.get([w.build.remote() for w in self._workers])

    @property
    def is_local(self) -> bool:
        return self._is_local

    def update(self, batch: MultiAgentBatch) -> List[Mapping[str, Any]]:
        """Do a gradient based update to the RLTrainer(s) maintained by this TrainerRunner.

        Args:
            batch: The data to use for the update.

        Returns:
            A list of dictionaries of results from the updates from the RLTrainer(s)
        """
        if self.is_local:
            return [self._trainer.update(batch)]
        else:
            return self._distributed_update(batch)

    def _distributed_update(self, batch: MultiAgentBatch) -> List[Mapping[str, Any]]:
        """Do a gradient based update to the RLTrainers using DDP training.

        Note: this function is used if the num_gpus this TrainerRunner is configured
            with is > 0. If _fake_gpus is True then this function will still be used
            for distributed training, but the workers will be configured to use a
            different backend than the cuda backend.

        Args:
            batch: The data to use for the update.

        Returns:
            A list of dictionaries of results from the updates from the RLTrainer(s)
        """
        refs = []
        global_size = len(self._workers)
        batch_size = math.ceil(len(batch) / global_size)
        for i, worker in enumerate(self._workers):
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

        if self.is_local:
            return [self._trainer.additional_update(*args, **kwargs)]
        else:
            refs = []
            for worker in self._workers:
                refs.append(worker.additional_update.remote(*args, **kwargs))
            return ray.get(refs)

    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
        set_optimizer_fn: Optional[Callable[[RLModule], ParamOptimizerPairs]] = None,
        optimizer_cls: Optional[Type[Optimizer]] = None,
    ) -> None:
        """Add a module to the RLTrainers maintained by this TrainerRunner.

        Args:
            module_id: The id of the module to add.
            module_spec:  #TODO (Kourosh) fill in here.
            set_optimizer_fn: A function that takes in the module and returns a list of
                (param, optimizer) pairs. Each element in the tuple describes a
                parameter group that share the same optimizer object, if None, the
                default optimizer (obtained from the exiting optimizer dictionary) will
                be used.
            optimizer_cls: The optimizer class to use. If None, the set_optimizer_fn
                should be provided.
        """
        if self.is_local:
            self._trainer.add_module(
                module_id=module_id,
                module_spec=module_spec,
                set_optimizer_fn=set_optimizer_fn,
                optimizer_cls=optimizer_cls,
            )
        else:
            refs = []
            for worker in self._workers:
                ref = worker.add_module.remote(
                    module_id=module_id,
                    module_spec=module_spec,
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
        if self.is_local:
            self._trainer.remove_module(module_id)
        else:
            refs = []
            for worker in self._workers:
                ref = worker.remove_module.remote(module_id)
                refs.append(ref)
            ray.get(refs)

    def get_weight(self) -> Dict:
        """Get the weights of the MARLModule.

        Returns:
            The weights of the neural networks that can be exchanged with the policy.
        """
        # TODO (Avnish): implement this.
        pass

    def get_state(self) -> List[Mapping[ModuleID, Mapping[str, Any]]]:
        """Get the states of the RLTrainers"""
        if self.is_local:
            return [self._trainer.get_state()]
        else:
            refs = []
            for worker in self._workers:
                refs.append(worker.get_state.remote())
            return ray.get(refs)

    def set_state(self, state: List[Mapping[ModuleID, Mapping[str, Any]]]) -> None:
        """Sets the states of the RLTrainers.

        Args:
            state: The state of the RLTrainers

        """
        if self.is_local:
            self._trainer.set_state(state)
        else:
            refs = []
            for worker in self._workers:
                refs.append(worker.set_state.remote(state))
            ray.get(refs)

    def shutdown(self):
        """Shuts down the TrainerRunner."""
        if not self._is_local:
            self._backend_executor.shutdown()

    def __del__(self):
        self.shutdown()
        super().__del__()
