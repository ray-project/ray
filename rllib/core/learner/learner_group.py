from collections import defaultdict, deque
from functools import partial
import pathlib
from typing import (
    Any,
    Callable,
    List,
    Mapping,
    Optional,
    Set,
    Type,
    TYPE_CHECKING,
    Union,
)
import uuid

import ray
from ray.rllib.core.learner.reduce_result_dict_fn import _reduce_mean_results
from ray.rllib.core.rl_module.rl_module import (
    ModuleID,
    SingleAgentRLModuleSpec,
    RLMODULE_STATE_DIR_NAME,
)
from ray.rllib.core.learner.learner import LearnerSpec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.actor_manager import FaultTolerantActorManager
from ray.rllib.utils.minibatch_utils import ShardBatchIterator
from ray.rllib.utils.typing import ResultDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.train._internal.backend_executor import BackendExecutor
from ray.tune.utils.file_transfer import sync_dir_between_nodes


if TYPE_CHECKING:
    from ray.rllib.core.learner.learner import Learner


def _get_backend_config(learner_class: Type["Learner"]) -> str:
    if learner_class.framework == "torch":
        from ray.train.torch import TorchConfig

        backend_config = TorchConfig()
    elif learner_class.framework == "tf2":
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

    Args:
        learner_spec: The specification for constructing Learners.
        max_queue_len: The maximum number of batches to queue up if doing async_update
            If the queue is full itwill evict the oldest batch first.

    """

    def __init__(
        self,
        learner_spec: LearnerSpec,
        max_queue_len: int = 20,
    ):
        scaling_config = learner_spec.learner_group_scaling_config
        learner_class = learner_spec.learner_class

        # TODO (Kourosh): Go with a _remote flag instead of _is_local to be more
        #  explicit.
        self._is_local = scaling_config.num_workers == 0
        self._learner = None
        self._workers = None
        # If a user calls self.shutdown() on their own then this flag is set to true.
        # When del is called the backend executor isn't shutdown twice if this flag is
        # true. the backend executor would otherwise log a warning to the console from
        # ray train.
        self._is_shut_down = False

        self._is_module_trainable = _is_module_trainable

        # How many timesteps had to be dropped due to a full input queue?
        self._in_queue_ts_dropped = 0

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

            # Run the neural network building code on remote workers.
            ray.get([w.build.remote() for w in self._workers])

            self._worker_manager = FaultTolerantActorManager(
                self._workers,
                # TODO (sven): This probably works even without any restriction
                #  (allowing for any arbitrary number of requests in-flight). Test with
                #  3 first, then with unlimited, and if both show the same behavior on
                #  an async algo, remove this restriction entirely.
                max_remote_requests_in_flight_per_actor=3,
            )
            # This is a list of the tags for asynchronous update requests that are
            # inflight, and is used for grouping together the results of requests
            # that were sent to the workers at the same time.
            self._inflight_request_tags: Set[str] = set()
            self._in_queue = deque(maxlen=max_queue_len)

    def get_in_queue_stats(self) -> Mapping[str, Any]:
        """Returns the current stats for the input queue for this learner group."""
        return {
            "learner_group_queue_size": len(self._in_queue),
            "learner_group_queue_ts_dropped": self._in_queue_ts_dropped,
        }

    @property
    def is_local(self) -> bool:
        return self._is_local

    def update(
        self,
        batch: MultiAgentBatch,
        *,
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        reduce_fn: Optional[Callable[[List[Mapping[str, Any]]], ResultDict]] = (
            _reduce_mean_results
        ),
    ) -> Union[Mapping[str, Any], List[Mapping[str, Any]]]:
        """Do one or more gradient based updates to the Learner(s) based on given data.

        Args:
            batch: The data batch to use for the update.
            minibatch_size: The minibatch size to use for the update.
            num_iters: The number of complete passes over all the sub-batches in the
                input multi-agent batch.
            reduce_fn: An optional callable to reduce the results from a list of the
                Learner actors into a single result. This can be any arbitrary function
                that takes a list of dictionaries and returns a single dictionary. For
                example you can either take an average (default) or concatenate the
                results (for example for metrics) or be more selective about you want to
                report back to the algorithm's training_step. If None is passed, the
                results will not get reduced.

        Returns:
            A dictionary with the reduced results of the updates from the Learner(s) or
            a list of dictionaries of results from the updates from the Learner(s).
        """

        # Construct a multi-agent batch with only the trainable modules.
        train_batch = {}
        for module_id in batch.policy_batches.keys():
            if self._is_module_trainable(module_id, batch):
                train_batch[module_id] = batch.policy_batches[module_id]
        train_batch = MultiAgentBatch(train_batch, batch.count)

        if self.is_local:
            results = [
                self._learner.update(
                    train_batch,
                    minibatch_size=minibatch_size,
                    num_iters=num_iters,
                    reduce_fn=reduce_fn,
                )
            ]
        else:

            def _learner_update(learner, minibatch):
                return learner.update(
                    minibatch,
                    minibatch_size=minibatch_size,
                    num_iters=num_iters,
                    reduce_fn=reduce_fn,
                )

            results = self._get_results(
                self._worker_manager.foreach_actor(
                    [
                        partial(_learner_update, minibatch=minibatch)
                        for minibatch in ShardBatchIterator(batch, len(self._workers))
                    ]
                )
            )

        # TODO(sven): Move reduce_fn to the training_step
        if reduce_fn is None:
            return results
        else:
            return reduce_fn(results)

    def async_update(
        self,
        batch: MultiAgentBatch,
        *,
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        reduce_fn: Optional[Callable[[List[Mapping[str, Any]]], ResultDict]] = (
            _reduce_mean_results
        ),
    ) -> Union[List[Mapping[str, Any]], List[List[Mapping[str, Any]]]]:
        """Asnychronously do gradient based updates to the Learner(s) with `batch`.

        Args:
            batch: The data batch to use for the update.
            minibatch_size: The minibatch size to use for the update.
            num_iters: The number of complete passes over all the sub-batches in the
                input multi-agent batch.
            reduce_fn: An optional callable to reduce the results from a list of the
                Learner actors into a single result. This can be any arbitrary function
                that takes a list of dictionaries and returns a single dictionary. For
                example you can either take an average (default) or concatenate the
                results (for example for metrics) or be more selective about you want to
                report back to the algorithm's training_step. If None is passed, the
                results will not get reduced.

        Returns:
            A list of list of dictionaries of results, where the outer list
            corresponds to separate calls to `async_update`, and the inner
            list corresponds to the results from each Learner(s). Or if the results
            are reduced, a list of dictionaries of the reduced results from each
            call to async_update that is ready.
        """
        if self.is_local:
            raise ValueError(
                "Cannot call `async_update` when running in local mode with "
                "num_workers=0."
            )
        else:
            if minibatch_size is not None:
                minibatch_size //= len(self._workers)

            def _learner_update(learner, minibatch):
                return learner.update(
                    minibatch,
                    minibatch_size=minibatch_size,
                    num_iters=num_iters,
                    reduce_fn=reduce_fn,
                )

            # Queue the new batches.
            # If queue is full, kick out the oldest item (and thus add its
            # length to the "dropped ts" counter).
            if len(self._in_queue) == self._in_queue.maxlen:
                self._in_queue_ts_dropped += len(self._in_queue[0])

            self._in_queue.append(batch)

            # Retrieve all ready results (kicked off by prior calls to this method).
            results = self._worker_manager.fetch_ready_async_reqs(
                tags=list(self._inflight_request_tags)
            )
            # Only if there are no more requests in-flight on any of the learners,
            # we can send in one new batch for sharding and parallel learning.
            if self._worker_manager_ready():
                count = 0
                # TODO (sven): This probably works even without any restriction
                #  (allowing for any arbitrary number of requests in-flight). Test with
                #  3 first, then with unlimited, and if both show the same behavior on
                #  an async algo, remove this restriction entirely.
                while len(self._in_queue) > 0 and count < 3:
                    # Pull a single batch from the queue (from the left side, meaning:
                    # use the oldest one first).
                    update_tag = str(uuid.uuid4())
                    self._inflight_request_tags.add(update_tag)
                    batch = self._in_queue.popleft()
                    self._worker_manager.foreach_actor_async(
                        [
                            partial(_learner_update, minibatch=minibatch)
                            for minibatch in ShardBatchIterator(
                                batch, len(self._workers)
                            )
                        ],
                        tag=update_tag,
                    )
                    count += 1

            # NOTE: There is a strong assumption here that the requests launched to
            # learner workers will return at the same time, since they are have a
            # barrier inside of themselves for gradient aggregation. Therefore results
            # should be a list of lists where each inner list should be the length of
            # the number of learner workers, if results from an  non-blocking update are
            # ready.
            results = self._get_async_results(results)

            # TODO(sven): Move reduce_fn to the training_step
            if reduce_fn is None:
                return results
            else:
                return [reduce_fn(r) for r in results]

    def _worker_manager_ready(self):
        # TODO (sven): This probably works even without any restriction (allowing for
        #  any arbitrary number of requests in-flight). Test with 3 first, then with
        #  unlimited, and if both show the same behavior on an async algo, remove
        #  this method entirely.
        return (
            self._worker_manager.num_outstanding_async_reqs()
            <= self._worker_manager.num_actors() * 2
        )

    def _get_results(self, results):
        processed_results = []
        for result in results:
            result_or_error = result.get()
            if result.ok:
                processed_results.append(result_or_error)
            else:
                raise result_or_error
        return processed_results

    def _get_async_results(self, results):
        """Get results from the worker manager and group them by tag.

        Returns:
            A list of lists of results, where each inner list contains all results
            for same tags.

        """
        unprocessed_results = defaultdict(list)
        for result in results:
            result_or_error = result.get()
            if result.ok:
                assert (
                    result.tag
                ), "Cannot call _get_async_results on untagged async requests."
                unprocessed_results[result.tag].append(result_or_error)
            else:
                raise result_or_error

        for tag in unprocessed_results.keys():
            self._inflight_request_tags.remove(tag)
        return list(unprocessed_results.values())

    def additional_update(
        self,
        *,
        reduce_fn: Callable[[ResultDict], ResultDict] = _reduce_mean_results,
        **kwargs,
    ) -> Union[Mapping[str, Any], List[Mapping[str, Any]]]:
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
            return self._learner.additional_update(**kwargs)
        else:
            results = self._worker_manager.foreach_actor(
                [lambda w: w.additional_update(**kwargs) for _ in self._workers]
            )
            results = self._get_results(results)
            if reduce_fn is None:
                return results
            # TODO(sven): Move reduce_fn to the training_step
            return reduce_fn(results)

    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
    ) -> None:
        """Add a module to the Learners maintained by this LearnerGroup.

        Args:
            module_id: The id of the module to add.
            module_spec:  #TODO (Kourosh) fill in here.
        """
        if self.is_local:
            self._learner.add_module(
                module_id=module_id,
                module_spec=module_spec,
            )
        else:
            results = self._worker_manager.foreach_actor(
                lambda w: w.add_module(
                    module_id=module_id,
                    module_spec=module_spec,
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

    def set_weights(self, weights: Mapping[str, Any]) -> None:
        """Set the weights of the MultiAgentRLModule maintained by each Learner.

        The weights don't have to include all the modules in the MARLModule.
            This way the weights of only some of the Agents can be set.

        Args:
            weights: The weights to set each RLModule in the MARLModule to.

        """
        if self.is_local:
            self._learner.set_module_state(weights)
        else:
            results_or_errors = self._worker_manager.foreach_actor(
                lambda w: w.set_module_state(weights)
            )
            # raise errors if any
            self._get_results(results_or_errors)

    def get_weights(self, module_ids: Optional[Set[str]] = None) -> Mapping[str, Any]:
        """Get the weights of the MultiAgentRLModule maintained by each Learner.

        Args:
            module_ids: The ids of the modules to get the weights of.

        Returns:
            A mapping of module ids to their weights.

        """
        if self.is_local:
            state = self._learner.get_module_state(module_ids)
        else:
            worker = self._worker_manager.healthy_actor_ids()[0]
            assert len(self._workers) == self._worker_manager.num_healthy_actors()
            state = self._worker_manager.foreach_actor(
                lambda w: w.get_module_state(module_ids), remote_actor_ids=[worker]
            )
            state = self._get_results(state)[0]

        return convert_to_numpy(state)

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

    def save_state(self, path: str) -> None:
        """Saves the state of the LearnerGroup.

        Args:
            path: The path to save the state to.
        """
        if self.is_local:
            self._learner.save_state(path)
        else:
            worker = self._worker_manager.healthy_actor_ids()[0]
            worker_ip_addr = self._worker_manager.foreach_actor(
                self._get_ip_address, remote_actor_ids=[worker]
            )
            worker_ip_addr = self._get_results(worker_ip_addr)[0]
            self_ip_addr = self._get_ip_address()

            if worker_ip_addr == self_ip_addr:
                self._worker_manager.foreach_actor(
                    lambda w: w.save_state(path), remote_actor_ids=[worker]
                )
            else:
                # save the checkpoint to a temporary location on the worker

                # create a temporary directory on the worker
                worker_temp_dir = self._worker_manager.foreach_actor(
                    self._create_temporary_dir, remote_actor_ids=[worker]
                )
                worker_temp_dir = self._get_results(worker_temp_dir)[0]

                # save the checkpoint to the temporary directory on the worker
                self._worker_manager.foreach_actor(
                    lambda w: w.save_state(worker_temp_dir), remote_actor_ids=[worker]
                )

                # sync the temporary directory on the worker to the local directory
                sync_dir_between_nodes(
                    worker_ip_addr, worker_temp_dir, self_ip_addr, path
                )

                # creating this function here instead of making it a member funciton
                # becasue it uses the worker_temp_dir variable, and this can't
                # be passed in as an argument to foreach_actor
                def remove_dir(w):
                    import shutil

                    shutil.rmtree(worker_temp_dir)

                # remove the temporary directory on the worker
                self._worker_manager.foreach_actor(
                    remove_dir, remote_actor_ids=[worker]
                )

    def load_state(self, path: str) -> None:
        """Loads the state of the LearnerGroup.

        Args:
            path: The path to load the state from.
        """
        path = str(self._resolve_checkpoint_path(path))

        if self.is_local:
            self._learner.load_state(path)
        else:
            assert len(self._workers) == self._worker_manager.num_healthy_actors()
            head_node_ip = ray.util.get_node_ip_address()
            workers = self._worker_manager.healthy_actor_ids()

            def _load_state(w):
                # doing imports here since they might not be imported on the worker
                import ray
                import tempfile

                worker_node_ip = ray.util.get_node_ip_address()
                # if the worker is on the same node as the head, load the checkpoint
                # directly from the path otherwise sync the checkpoint from the head
                # to the worker and load it from there
                if worker_node_ip == head_node_ip:
                    w.load_state(path)
                else:
                    with tempfile.TemporaryDirectory() as temp_dir:
                        sync_dir_between_nodes(
                            head_node_ip, path, worker_node_ip, temp_dir
                        )
                        w.load_state(temp_dir)

            self._worker_manager.foreach_actor(_load_state, remote_actor_ids=workers)

    def load_module_state(
        self,
        *,
        marl_module_ckpt_dir: Optional[str] = None,
        modules_to_load: Optional[Set[str]] = None,
        rl_module_ckpt_dirs: Optional[Mapping[ModuleID, str]] = None,
    ) -> None:

        """Load the checkpoints of the modules being trained by this LearnerGroup.

        `load_module_state` can be used 3 ways:
            1. Load a checkpoint for the MultiAgentRLModule being trained by this
                LearnerGroup. Limit the modules that are loaded from the checkpoint
                by specifying the `modules_to_load` argument.
            2. Load the checkpoint(s) for single agent RLModules that
                are in the MultiAgentRLModule being trained by this LearnerGroup.
            3. Load a checkpoint for the MultiAgentRLModule being trained by this
                LearnerGroup and load the checkpoint(s) for single agent RLModules
                that are in the MultiAgentRLModule. The checkpoints for the single
                agent RLModules take precedence over the module states in the
                MultiAgentRLModule checkpoint.

        NOTE: At lease one of marl_module_ckpt_dir or rl_module_ckpt_dirs is
            must be specified. modules_to_load can only be specified if
            marl_module_ckpt_dir is specified.

        Args:
            marl_module_ckpt_dir: The path to the checkpoint for the
                MultiAgentRLModule.
            modules_to_load: A set of module ids to load from the checkpoint.
            rl_module_ckpt_dirs: A mapping from module ids to the path to a
                checkpoint for a single agent RLModule.
        """
        if not (marl_module_ckpt_dir or rl_module_ckpt_dirs):
            raise ValueError(
                "At least one of multi_agent_module_state or "
                "single_agent_module_states must be specified."
            )
        if marl_module_ckpt_dir:
            if not isinstance(marl_module_ckpt_dir, str):
                raise ValueError("multi_agent_module_state must be a string path.")
            marl_module_ckpt_dir = self._resolve_checkpoint_path(marl_module_ckpt_dir)
        if rl_module_ckpt_dirs:
            if not isinstance(rl_module_ckpt_dirs, dict):
                raise ValueError("single_agent_module_states must be a dictionary.")
            for module_id, path in rl_module_ckpt_dirs.items():
                if not isinstance(path, str):
                    raise ValueError(
                        "rl_module_ckpt_dirs must be a dictionary "
                        "mapping module ids to string paths."
                    )
                rl_module_ckpt_dirs[module_id] = self._resolve_checkpoint_path(path)
        if modules_to_load:
            if not isinstance(modules_to_load, set):
                raise ValueError("modules_to_load must be a set.")
            for module_id in modules_to_load:
                if not isinstance(module_id, str):
                    raise ValueError("modules_to_load must be a list of strings.")

        if self.is_local:
            module_keys = set(self._learner.module.keys())
        else:
            workers = self._worker_manager.healthy_actor_ids()
            module_keys = set(
                self._get_results(
                    self._worker_manager.foreach_actor(
                        lambda w: w.module.keys(), remote_actor_ids=[workers[0]]
                    )
                )[0]
            )

        if marl_module_ckpt_dir and rl_module_ckpt_dirs:
            # If both a MARLModule checkpoint and RLModule checkpoints are specified,
            # the RLModule checkpoints take precedence over the MARLModule checkpoint,
            # so we should not load any modules in the MARLModule checkpoint that are
            # also in the RLModule checkpoints.
            if modules_to_load:
                if any(
                    module_id in modules_to_load
                    for module_id in rl_module_ckpt_dirs.keys()
                ):
                    raise ValueError(
                        f"module_id {module_id} was specified in both "
                        "modules_to_load and rl_module_ckpt_dirs. Please only "
                        "specify a module to be loaded only once, either in "
                        "modules_to_load or rl_module_ckpt_dirs, but not both."
                    )
            else:
                modules_to_load = module_keys - set(rl_module_ckpt_dirs.keys())

        # No need to do any file transfer operations if we are running training
        # on the experiment head node.
        if self._is_local:
            if marl_module_ckpt_dir:
                # load the MARLModule checkpoint if they were specified
                self._learner.module.load_state(
                    marl_module_ckpt_dir, modules_to_load=modules_to_load
                )
            if rl_module_ckpt_dirs:
                # load the RLModule if they were specified
                for module_id, path in rl_module_ckpt_dirs.items():
                    self._learner.module[module_id].load_state(
                        path / RLMODULE_STATE_DIR_NAME
                    )
        else:
            self._distributed_load_module_state(
                marl_module_ckpt_dir=marl_module_ckpt_dir,
                modules_to_load=modules_to_load,
                rl_module_ckpt_dirs=rl_module_ckpt_dirs,
            )

    def _distributed_load_module_state(
        self,
        *,
        marl_module_ckpt_dir: Optional[str] = None,
        modules_to_load: Optional[Set[str]] = None,
        rl_module_ckpt_dirs: Optional[Mapping[ModuleID, str]] = None,
    ):
        """Load the checkpoints of the modules being trained by this LearnerGroup.

           This method only needs to be called if the LearnerGroup is training
           distributed learners (e.g num_learner_workers > 0).

        Args:
            marl_module_ckpt_dir: The path to the checkpoint for the
                MultiAgentRLModule.
            modules_to_load: A set of module ids to load from the checkpoint.
            rl_module_ckpt_dirs: A mapping from module ids to the path to a
                checkpoint for a single agent RLModule.

        """

        assert len(self._workers) == self._worker_manager.num_healthy_actors()
        workers = self._worker_manager.healthy_actor_ids()
        head_node_ip = ray.util.get_node_ip_address()

        def _load_module_state(w):
            # doing imports here since they might not be imported on the worker
            import ray
            import tempfile
            import shutil

            worker_node_ip = ray.util.get_node_ip_address()
            # sync the checkpoints from the head to the worker if the worker is not
            # on the same node as the head
            tmp_marl_module_ckpt_dir = marl_module_ckpt_dir
            tmp_rl_module_ckpt_dirs = rl_module_ckpt_dirs
            if worker_node_ip != head_node_ip:
                if marl_module_ckpt_dir:
                    tmp_marl_module_ckpt_dir = tempfile.mkdtemp()
                    sync_dir_between_nodes(
                        source_ip=head_node_ip,
                        source_path=marl_module_ckpt_dir,
                        target_ip=worker_node_ip,
                        target_path=tmp_marl_module_ckpt_dir,
                    )
                if rl_module_ckpt_dirs:
                    tmp_rl_module_ckpt_dirs = {}
                    for module_id, path in rl_module_ckpt_dirs.items():
                        tmp_rl_module_ckpt_dirs[module_id] = tempfile.mkdtemp()
                        sync_dir_between_nodes(
                            source_ip=head_node_ip,
                            source_path=path,
                            target_ip=worker_node_ip,
                            target_path=tmp_rl_module_ckpt_dirs[module_id],
                        )
                        tmp_rl_module_ckpt_dirs[module_id] = pathlib.Path(
                            tmp_rl_module_ckpt_dirs[module_id]
                        )
            if marl_module_ckpt_dir:
                # load the MARLModule checkpoint if they were specified
                w.module.load_state(
                    tmp_marl_module_ckpt_dir, modules_to_load=modules_to_load
                )
            if rl_module_ckpt_dirs:
                # load the RLModule if they were specified
                for module_id, path in tmp_rl_module_ckpt_dirs.items():
                    w.module[module_id].load_state(path / RLMODULE_STATE_DIR_NAME)

            # remove the temporary directories on the worker if any were created
            if worker_node_ip != head_node_ip:
                if marl_module_ckpt_dir:
                    shutil.rmtree(tmp_marl_module_ckpt_dir)
                if rl_module_ckpt_dirs:
                    for module_id, path in tmp_rl_module_ckpt_dirs.items():
                        shutil.rmtree(path)

        self._worker_manager.foreach_actor(_load_module_state, remote_actor_ids=workers)

    @staticmethod
    def _resolve_checkpoint_path(path: str) -> pathlib.Path:
        """Checks that the provided checkpoint path is a dir and makes it absolute."""
        path = pathlib.Path(path)
        if not path.is_dir():
            raise ValueError(
                f"Path {path} is not a directory. "
                "Please specify a directory containing the checkpoint files."
            )
        if not path.exists():
            raise ValueError(f"Path {path} does not exist.")
        path = path.absolute()
        return path

    @staticmethod
    def _create_temporary_dir(_=None) -> str:
        """Creates a temporary directory.

        Args:
            _: Unused arg. Exists to make this function compatible with foreach_actor
            calls.

        Returns:
            The path to the temporary directory.
        """
        import tempfile

        return tempfile.mkdtemp()

    @staticmethod
    def _get_ip_address(_=None) -> str:
        """Returns this process's address.

        Args:
            _: Unused arg. Exists to make this function compatible with foreach_actor
            calls.

        Returns:
            The address of this process.

        """
        import ray

        return ray.util.get_node_ip_address()

    def shutdown(self):
        """Shuts down the LearnerGroup."""
        if not self._is_local:
            self._backend_executor.shutdown()
            self._is_shut_down = True

    def __del__(self):
        if not self._is_shut_down:
            self.shutdown()
