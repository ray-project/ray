from collections import deque
from typing import Any, List, Mapping, Type, Optional, Callable, Set, TYPE_CHECKING

import ray

from ray.rllib.core.learner.reduce_result_dict_fn import _reduce_mean_results
from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    ModuleID,
    SingleAgentRLModuleSpec,
)
from ray.rllib.core.learner.learner import (
    LearnerSpec,
    ParamOptimizerPairs,
    Optimizer,
)
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.actor_manager import FaultTolerantActorManager
from ray.rllib.utils.minibatch_utils import ShardBatchIterator
from ray.rllib.utils.typing import ResultDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.train._internal.backend_executor import BackendExecutor

if TYPE_CHECKING:
    from ray.rllib.core.learner.learner import Learner


def _get_backend_config(learner_class: Type["Learner"]) -> str:
    if learner_class.framework == "torch":
        from ray.train.torch import TorchConfig

        backend_config = TorchConfig()
    elif learner_class.framework == "tf":
        from ray.train.tensorflow import TensorflowConfig

        backend_config = TensorflowConfig()
    else:
        raise ValueError("framework must be either torch or tf")

    return backend_config


def _is_module_trainable(module_id: ModuleID, batch: MultiAgentBatch) -> bool:
    """Default implemntation for is_module_trainable()

    It assumes that the module is trainable by default.
    """
    return True


class LearnerGroup:
    """Coordinator of Learners.
    Public API:
        .update(batch) -> updates the RLModule based on gradient descent algos.
        .additional_update() -> any additional non-gradient based updates will get
                                called from this entry point.
        .get_state() -> returns the state of the RLModule and RLOptimizer from
                        all of the Learners.
        .set_state() -> sets the state of all the Learners.
        .get_weights() -> returns the weights of the RLModule from the Learner(s).
        .set_weights() -> sets the weights of the RLModule in the Learner(s).
        .add_module() -> add a new RLModule to the MultiAgentRLModule being trained by
                         this LearnerGroup.
        .remove_module() -> remove an RLModule from the MultiAgentRLModule being trained
                            by this LearnerGroup.
    Args:
        learner_spec: The specification for constructing Learners.
        max_queue_len: The maximum number of batches to queue up if doing non-blocking
            updates (e.g. `self.update(batch, block=False)`). If the queue is full it
            will evict the oldest batch first.
    """

    def __init__(
        self,
        learner_spec: LearnerSpec,
        max_queue_len: int = 20,
    ):
        scaling_config = learner_spec.learner_scaling_config
        learner_class = learner_spec.learner_class

        # TODO (Kourosh): Go with a _remote flag instead of _is_local to be more
        # explicit
        self._is_local = scaling_config.num_workers == 0
        self._learner = None
        self._workers = None
        # if a user calls self.shutdown() on their own then this flag is set to true.
        # When del is called the backend executor isn't shutdown twice if this flag is
        # true. the backend executor would otherwise log a warning to the console from
        # ray train
        self._is_shut_down = False

        self._is_module_trainable = _is_module_trainable

        if self._is_local:
            self._learner = learner_class(**learner_spec.get_params_dict())
            self._learner.build()
            self._worker_manager = None
            self._in_queue = []
        else:
            backend_config = _get_backend_config(learner_class)
            backend_executor = BackendExecutor(
                backend_config=backend_config,
                num_workers=scaling_config.num_workers,
                num_cpus_per_worker=scaling_config.num_cpus_per_worker,
                num_gpus_per_worker=scaling_config.num_gpus_per_worker,
                max_retries=0,
            )
            backend_executor.start(
                train_cls=learner_class,
                train_cls_kwargs=learner_spec.get_params_dict(),
            )
            self._backend_executor = backend_executor

            self._workers = [w.actor for w in backend_executor.worker_group.workers]

            # run the neural network building code on remote workers
            ray.get([w.build.remote() for w in self._workers])
            # use only 1 max in flight request per worker since training workers have to
            # be synchronously executed.
            self._worker_manager = FaultTolerantActorManager(
                self._workers,
                max_remote_requests_in_flight_per_actor=1,
            )
            self._in_queue = deque(maxlen=max_queue_len)

    @property
    def in_queue_size(self) -> int:
        """Returns the number of batches currently in the in queue to be processed.

        If the queue is reaching its max size, then this learner group likely needs
        more workers to process incoming batches.
        """
        return len(self._in_queue)

    @property
    def is_local(self) -> bool:
        return self._is_local

    def update(
        self,
        batch: MultiAgentBatch,
        *,
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        reduce_fn: Callable[[ResultDict], ResultDict] = _reduce_mean_results,
        block: bool = True,
    ) -> List[Mapping[str, Any]]:
        """Do one gradient based update to the Learner(s).

        Args:
            batch: The data to use for the update.
            minibatch_size: The minibatch size to use for the update.
            num_iters: The number of complete passes over all the sub-batches in the
                input multi-agent batch.
            reduce_fn: A function to reduce the results from a list of Learner Actors
                into a single result. This can be any arbitrary function that takes a
                list of dictionaries and returns a single dictionary. For example you
                can either take an average (default) or concatenate the results (for
                example for metrics) or be more selective about you want to report back
                to the algorithm's training_step. If None is passed, the results will
                not get reduced.
            block: Whether to block until the update is complete.

        Returns:
            A list of dictionaries of results from the updates from the Learner(s)
        """

        # Construct a multi-agent batch with only the trainable modules.
        train_batch = {}
        for module_id in batch.policy_batches.keys():
            if self._is_module_trainable(module_id, batch):
                train_batch[module_id] = batch.policy_batches[module_id]
        train_batch = MultiAgentBatch(train_batch, batch.count)

        if self.is_local:
            if not block:
                raise ValueError(
                    "Cannot run update in non-blocking mode when running in local "
                    "mode with num_workers=0."
                )
            results = [
                self._learner.update(
                    train_batch,
                    minibatch_size=minibatch_size,
                    num_iters=num_iters,
                    reduce_fn=reduce_fn,
                )
            ]
        else:
            results = self._distributed_update(
                train_batch,
                minibatch_size=minibatch_size,
                num_iters=num_iters,
                reduce_fn=reduce_fn,
                block=block,
            )

        # TODO (Kourosh): Maybe we should use LearnerInfoBuilder() here?
        if reduce_fn is None or not results:
            return results
        return reduce_fn(results)

    def _distributed_update(
        self,
        batch: MultiAgentBatch,
        *,
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        reduce_fn: Callable[[ResultDict], ResultDict] = _reduce_mean_results,
        block: bool = True,
    ) -> List[Mapping[str, Any]]:
        """Do a gradient based update to the Learners using DDP training.

        Note: this function is used if the num_gpus this LearnerGroup is configured
            with is > 0. If _fake_gpus is True then this function will still be used
            for distributed training, but the workers will be configured to use a
            different backend than the cuda backend.

        Args:
            See `.update()` docstring.

        Returns:
            A list of dictionaries of results from the updates from the Learner(s)
        """

        if block:
            results = self._worker_manager.foreach_actor(
                [
                    lambda w: w.update(
                        b,
                        minibatch_size=minibatch_size,
                        num_iters=num_iters,
                        reduce_fn=reduce_fn,
                    )
                    for b in ShardBatchIterator(batch, len(self._workers))
                ]
            )
        else:
            if batch is not None:
                self._in_queue.append(batch)
            results = self._worker_manager.fetch_ready_async_reqs()
            if self._worker_manager_ready() and self._in_queue:
                batch = self._in_queue.popleft()
                self._worker_manager.foreach_actor_async(
                    [
                        lambda w: w.update(
                            b,
                            minibatch_size=minibatch_size,
                            num_iters=num_iters,
                            reduce_fn=reduce_fn,
                        )
                        for b in ShardBatchIterator(batch, len(self._workers))
                    ]
                )

        return self._get_results(results)

    def _worker_manager_ready(self):
        return self._worker_manager.num_outstanding_async_reqs() == 0

    def _get_results(self, results):
        processed_results = []
        for result in results:
            result_or_error = result.get()
            if result.ok:
                processed_results.append(result_or_error)
            else:
                raise result_or_error
        return processed_results

    def additional_update(
        self,
        *,
        reduce_fn: Optional[Callable[[ResultDict], ResultDict]] = _reduce_mean_results,
        **kwargs,
    ) -> List[Mapping[str, Any]]:
        """Apply additional non-gradient based updates to the Learners.

        For example, this could be used to do a polyak averaging update
        of a target network in off policy algorithms like SAC or DQN.

        By default this is a pass through that calls `Learner.additional_update`

        Args:
            reduce_fn: See `update()` documentation for more details.
            **kwargs: Keyword arguments to pass to each Learner.

        Returns:
            A list of dictionaries of results from the updates from each worker.
        """

        if self.is_local:
            results = [self._learner.additional_update(**kwargs)]
        else:
            results = self._worker_manager.foreach_actor(
                [lambda w: w.additional_update(**kwargs) for worker in self._workers]
            )
            results = self._get_results(results)
            if reduce_fn is None:
                return results
            return reduce_fn(results)

    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
        set_optimizer_fn: Optional[Callable[[RLModule], ParamOptimizerPairs]] = None,
        optimizer_cls: Optional[Type[Optimizer]] = None,
    ) -> None:
        """Add a module to the Learners maintained by this LearnerGroup.

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
            self._learner.add_module(
                module_id=module_id,
                module_spec=module_spec,
                set_optimizer_fn=set_optimizer_fn,
                optimizer_cls=optimizer_cls,
            )
        else:
            results = self._worker_manager.foreach_actor(
                lambda w: w.add_module(
                    module_id=module_id,
                    module_spec=module_spec,
                    set_optimizer_fn=set_optimizer_fn,
                    optimizer_cls=optimizer_cls,
                )
            )
            return self._get_results(results)

    def remove_module(self, module_id: ModuleID) -> None:
        """Remove a module from the Learners maintained by this LearnerGroup.

        Args:
            module_id: The id of the module to remove.

        """
        if self.is_local:
            self._learner.remove_module(module_id)
        else:
            refs = []
            for worker in self._workers:
                ref = worker.remove_module.remote(module_id)
                refs.append(ref)
            ray.get(refs)

    def set_weights(self, weights) -> None:
        # TODO (Kourosh) Set / get weight has to be thoroughly
        # tested across actors and multi-gpus
        if self.is_local:
            self._learner.set_weights(weights)
        else:
            results_or_errors = self._worker_manager.foreach_actor(
                lambda w: w.set_weights(weights)
            )
            # raise errors if any
            self._get_results(results_or_errors)

    def get_weights(self, module_ids: Optional[Set[str]] = None) -> Mapping[str, Any]:
        if self.is_local:
            weights = self._learner.get_weights(module_ids)
        else:
            worker = self._worker_manager.healthy_actor_ids()[0]
            assert len(self._workers) == self._worker_manager.num_healthy_actors()
            weights = self._worker_manager.foreach_actor(
                lambda w: w.get_weights(module_ids), remote_actor_ids=[worker]
            )
            weights = self._get_results(weights)[0]

        return convert_to_numpy(weights)

    def get_state(self) -> Mapping[ModuleID, Mapping[str, Any]]:
        """Get the states of the first Learners.

        This should be the same across Learners
        """
        if self.is_local:
            return self._learner.get_state()
        else:
            worker = self._worker_manager.healthy_actor_ids()[0]
            assert len(self._workers) == self._worker_manager.num_healthy_actors()
            results = self._worker_manager.foreach_actor(
                lambda w: w.get_state(), remote_actor_ids=[worker]
            )
            return self._get_results(results)[0]

    def set_state(self, state: List[Mapping[ModuleID, Mapping[str, Any]]]) -> None:
        """Sets the states of the Learners.

        Args:
            state: The state of the Learners

        """
        if self.is_local:
            self._learner.set_state(state)
        else:
            self._worker_manager.foreach_actor(lambda w: w.set_state(state))

    def set_is_module_trainable(
        self, is_module_trainable: Callable[[ModuleID, MultiAgentBatch], bool] = None
    ) -> None:
        """Sets the function that determines whether a module is trainable.

        Args:
            is_module_trainable: A function that takes in a module id and a batch
                and returns a boolean indicating whether the module should be trained
                on the batch.
        """
        if is_module_trainable is not None:
            self._is_module_trainable = is_module_trainable

    def shutdown(self):
        """Shuts down the LearnerGroup."""
        if not self._is_local:
            self._backend_executor.shutdown()
            self._is_shut_down = True

    def __del__(self):
        if not self._is_shut_down:
            self.shutdown()
