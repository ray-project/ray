from collections import defaultdict, Counter
from functools import partial
import pathlib
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Type,
    Union,
)
import uuid

import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.rl_module import (
    SingleAgentRLModuleSpec,
    RLMODULE_STATE_DIR_NAME,
)
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.actor_manager import FaultTolerantActorManager
from ray.rllib.utils.deprecation import (
    Deprecated,
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.minibatch_utils import (
    ShardBatchIterator,
    ShardEpisodesIterator,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import (
    EpisodeType,
    ModuleID,
    RLModuleSpec,
    T,
)
from ray.train._internal.backend_executor import BackendExecutor
from ray.tune.utils.file_transfer import sync_dir_between_nodes
from ray.util.annotations import PublicAPI


def _get_backend_config(learner_class: Type[Learner]) -> str:
    if learner_class.framework == "torch":
        from ray.train.torch import TorchConfig

        backend_config = TorchConfig()
    elif learner_class.framework == "tf2":
        from ray.train.tensorflow import TensorflowConfig

        backend_config = TensorflowConfig()
    else:
        raise ValueError(
            "`learner_class.framework` must be either 'torch' or 'tf2' (but is "
            f"{learner_class.framework}!"
        )

    return backend_config


@PublicAPI(stability="alpha")
class LearnerGroup:
    """Coordinator of n (possibly remote) Learner workers.

    Each Learner worker has a copy of the RLModule, the loss function(s), and
    one or more optimizers.
    """

    def __init__(
        self,
        *,
        config: AlgorithmConfig = None,  # TODO (sven): Make this arg mandatory.
        module_spec: Optional[RLModuleSpec] = None,
        max_queue_len: int = 20,
        # Deprecated args.
        learner_spec=None,
    ):
        """Initializes a LearnerGroup instance.

        Args:
            config: The AlgorithmConfig object to use to configure this LearnerGroup.
                Call the `resources(num_learner_workers=...)` method on your config to
                specify the number of learner workers to use.
                Call the same method with arguments `num_cpus_per_learner_worker` and/or
                `num_gpus_per_learner_worker` to configure the compute used by each
                Learner worker in this LearnerGroup.
                Call the `training(learner_class=...)` method on your config to specify,
                which exact Learner class to use.
                Call the `rl_module(rl_module_spec=...)` method on your config to set up
                the specifics for your RLModule to be used in each Learner.
            module_spec: If not already specified in `config`, a separate overriding
                RLModuleSpec may be provided via this argument.
            max_queue_len: The maximum number of batches to queue up if doing
                async_update. If the queue is full it will evict the oldest batch first.
        """
        if learner_spec is not None:
            deprecation_warning(
                old="LearnerGroup(learner_spec=...)",
                new="config = AlgorithmConfig().[resources|training|rl_module](...); "
                "LearnerGroup(config=config)",
                error=True,
            )
        if config is None:
            raise ValueError(
                "LearnerGroup constructor must be called with a `config` arg! "
                "Pass in a `ray.rllib.algorithms.algorithm_config::AlgorithmConfig` "
                "object with the proper settings configured."
            )

        # scaling_config = learner_spec.learner_group_scaling_config
        self.config = config

        learner_class = self.config.learner_class
        module_spec = module_spec or self.config.get_marl_module_spec()

        self._learner = None
        self._workers = None
        # If a user calls self.shutdown() on their own then this flag is set to true.
        # When del is called the backend executor isn't shutdown twice if this flag is
        # true. the backend executor would otherwise log a warning to the console from
        # ray train.
        self._is_shut_down = False

        # How many timesteps had to be dropped due to a full input queue?
        self._ts_dropped = 0

        # A single local Learner.
        if not self.is_remote:
            self._learner = learner_class(config=config, module_spec=module_spec)
            self._learner.build()
            self._worker_manager = None
        # N remote Learner workers.
        else:
            backend_config = _get_backend_config(learner_class)

            # TODO (sven): Cannot set both `num_cpus_per_learner_worker`>1 and
            #  `num_gpus_per_learner_worker`>0! Users must set one or the other due
            #  to issues with placement group fragmentation. See
            #  https://github.com/ray-project/ray/issues/35409 for more details.
            num_cpus_per_worker = (
                self.config.num_cpus_per_learner_worker
                if not self.config.num_gpus_per_learner_worker
                else 0
            )
            num_gpus_per_worker = self.config.num_gpus_per_learner_worker
            resources_per_worker = {
                "CPU": num_cpus_per_worker,
                "GPU": num_gpus_per_worker,
            }

            backend_executor = BackendExecutor(
                backend_config=backend_config,
                num_workers=self.config.num_learner_workers,
                resources_per_worker=resources_per_worker,
                max_retries=0,
            )
            backend_executor.start(
                train_cls=learner_class,
                train_cls_kwargs={
                    "config": config,
                    "module_spec": module_spec,
                },
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
            # Counters for the tags for asynchronous update requests that are
            # in-flight. Used for keeping trakc of and grouping together the results of
            # requests that were sent to the workers at the same time.
            self._update_request_tags = Counter()
            self._additional_update_request_tags = Counter()

        # A special MetricsLogger object (not exposed to the user) for reducing
        # the n results dicts returned by our n Learner workers in case we are on
        # the old or hybrid API stack.
        self._metrics_logger_old_and_hybrid_stack: Optional[MetricsLogger] = None
        if not self.config.enable_env_runner_and_connector_v2:
            self._metrics_logger_old_and_hybrid_stack = MetricsLogger()

    # TODO (sven): Replace this with call to `self.metrics.peek()`?
    def get_stats(self) -> Dict[str, Any]:
        """Returns the current stats for the input queue for this learner group."""
        return {
            "learner_group_ts_dropped": self._ts_dropped,
            "actor_manager_num_outstanding_async_reqs": (
                0
                if self.is_local
                else self._worker_manager.num_outstanding_async_reqs()
            ),
        }

    @property
    def is_remote(self) -> bool:
        return self.config.num_learner_workers > 0

    @property
    def is_local(self) -> bool:
        return not self.is_remote

    def update_from_batch(
        self,
        batch: MultiAgentBatch,
        *,
        async_update: bool = False,
        # TODO (sven): Deprecate the following args. They should be extracted from
        #  self.config of those specific algorithms that actually require these
        #  settings.
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        # Already deprecated args.
        reduce_fn=DEPRECATED_VALUE,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], List[List[Dict[str, Any]]]]:
        """Performs gradient based update(s) on the Learner(s), based on given batch.

        Args:
            batch: A data batch to use for the update. If there are more
                than one Learner workers, the batch is split amongst these and one
                shard is sent to each Learner.
            async_update: Whether the update request(s) to the Learner workers should be
                sent asynchronously. If True, will return NOT the results from the
                update on the given data, but all results from prior asynchronous update
                requests that have not been returned thus far.
            minibatch_size: The minibatch size to use for the update.
            num_iters: The number of complete passes over all the sub-batches in the
                input multi-agent batch.

        Returns:
            If `async_update` is False, a dictionary with the reduced results of the
            updates from the Learner(s) or a list of dictionaries of results from the
            updates from the Learner(s).
            If `async_update` is True, a list of list of dictionaries of results, where
            the outer list corresponds to separate previous calls to this method, and
            the inner list corresponds to the results from each Learner(s). Or if the
            results are reduced, a list of dictionaries of the reduced results from each
            call to async_update that is ready.
        """
        if reduce_fn != DEPRECATED_VALUE:
            deprecation_warning(
                old="LearnerGroup.update_from_batch(reduce_fn=..)",
                new="Learner.metrics.[log_value|log_dict|log_time](key=..., value=..., "
                "reduce=[mean|min|max|sum], window=..., ema_coeff=...)",
                help="Use the new ray.rllib.utils.metrics.metrics_logger::MetricsLogger"
                " API in your custom Learner methods for logging and time-reducing any "
                "custom metrics. The central `MetricsLogger` instance is available "
                "under `self.metrics` within your custom Learner.",
                error=True,
            )
        return self._update(
            batch=batch,
            episodes=None,
            async_update=async_update,
            minibatch_size=minibatch_size,
            num_iters=num_iters,
        )

    def update_from_episodes(
        self,
        episodes: List[EpisodeType],
        *,
        async_update: bool = False,
        # TODO (sven): Deprecate the following args. They should be extracted from
        #  self.config of those specific algorithms that actually require these
        #  settings.
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        # Already deprecated args.
        reduce_fn=DEPRECATED_VALUE,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], List[List[Dict[str, Any]]]]:
        """Performs gradient based update(s) on the Learner(s), based on given episodes.

        Args:
            episodes: A list of Episodes to process and perform the update
                for. If there are more than one Learner workers, the list of episodes
                is split amongst these and one list shard is sent to each Learner.
            async_update: Whether the update request(s) to the Learner workers should be
                sent asynchronously. If True, will return NOT the results from the
                update on the given data, but all results from prior asynchronous update
                requests that have not been returned thus far.
            minibatch_size: The minibatch size to use for the update.
            num_iters: The number of complete passes over all the sub-batches in the
                input multi-agent batch.

        Returns:
            If async_update is False, a dictionary with the reduced results of the
            updates from the Learner(s) or a list of dictionaries of results from the
            updates from the Learner(s).
            If async_update is True, a list of list of dictionaries of results, where
            the outer list corresponds to separate previous calls to this method, and
            the inner list corresponds to the results from each Learner(s). Or if the
            results are reduced, a list of dictionaries of the reduced results from each
            call to async_update that is ready.
        """
        if reduce_fn != DEPRECATED_VALUE:
            deprecation_warning(
                old="LearnerGroup.update_from_episodes(reduce_fn=..)",
                new="Learner.metrics.[log_value|log_dict|log_time](key=..., value=..., "
                "reduce=[mean|min|max|sum], window=..., ema_coeff=...)",
                help="Use the new ray.rllib.utils.metrics.metrics_logger::MetricsLogger"
                " API in your custom Learner methods for logging and time-reducing any "
                "custom metrics. The central `MetricsLogger` instance is available "
                "under `self.metrics` within your custom Learner.",
                error=True,
            )

        return self._update(
            batch=None,
            episodes=episodes,
            async_update=async_update,
            minibatch_size=minibatch_size,
            num_iters=num_iters,
        )

    def _update(
        self,
        *,
        batch: Optional[MultiAgentBatch] = None,
        episodes: Optional[List[EpisodeType]] = None,
        async_update: bool = False,
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], List[List[Dict[str, Any]]]]:

        # Define function to be called on all Learner actors (or the local learner).
        def _learner_update(
            learner: Learner,
            batch_shard=None,
            episodes_shard=None,
            min_total_mini_batches=0,
        ):
            if batch_shard is not None:
                return learner.update_from_batch(
                    batch=batch_shard,
                    minibatch_size=minibatch_size,
                    num_iters=num_iters,
                )
            else:
                return learner.update_from_episodes(
                    episodes=episodes_shard,
                    minibatch_size=minibatch_size,
                    num_iters=num_iters,
                    min_total_mini_batches=min_total_mini_batches,
                )

        # Local Learner worker: Don't shard batch/episodes, just run data as-is through
        # this Learner.
        if self.is_local:
            if async_update:
                raise ValueError(
                    "Cannot call `update_from_batch(update_async=True)` when running in"
                    " local mode! Try setting `config.num_learner_workers > 0`."
                )

            results = [
                _learner_update(
                    learner=self._learner,
                    batch_shard=batch,
                    episodes_shard=episodes,
                )
            ]
        # One or more remote Learners: Shard batch/episodes into equal pieces (roughly
        # equal if multi-agent AND episodes) and send each Learner worker one of these
        # shards.
        else:
            # MultiAgentBatch: Shard into equal pieces.
            # TODO (sven): The sharder used here destroys - for multi-agent only -
            #  the relationship of the different agents' timesteps to each other.
            #  Thus, in case the algorithm requires agent-synchronized data (aka.
            #  "lockstep"), the `ShardBatchIterator` should not be used.
            if episodes is None:
                partials = [
                    partial(_learner_update, batch_shard=batch_shard)
                    for batch_shard in ShardBatchIterator(batch, len(self._workers))
                ]
            # Single- or MultiAgentEpisodes: Shard into equal pieces (only roughly equal
            # in case of multi-agent).
            else:
                eps_shards = list(ShardEpisodesIterator(episodes, len(self._workers)))
                # In the multi-agent case AND `minibatch_size` AND num_workers > 1, we
                # compute a max iteration counter such that the different Learners will
                # not go through a different number of iterations.
                min_total_mini_batches = 0
                if (
                    isinstance(episodes[0], MultiAgentEpisode)
                    and minibatch_size
                    and len(self._workers) > 1
                ):
                    # Find episode w/ the largest single-agent episode in it, then
                    # compute this single-agent episode's total number of mini batches
                    # (if we iterated over it num_sgd_iter times with the mini batch
                    # size).
                    longest_ts = 0
                    per_mod_ts = defaultdict(int)
                    for i, shard in enumerate(eps_shards):
                        for ma_episode in shard:
                            for sa_episode in ma_episode.agent_episodes.values():
                                key = (i, sa_episode.module_id)
                                per_mod_ts[key] += len(sa_episode)
                                if per_mod_ts[key] > longest_ts:
                                    longest_ts = per_mod_ts[key]
                    min_total_mini_batches = self._compute_num_total_mini_batches(
                        batch_size=longest_ts,
                        mini_batch_size=minibatch_size,
                        num_iters=num_iters,
                    )
                partials = [
                    partial(
                        _learner_update,
                        episodes_shard=eps_shard,
                        min_total_mini_batches=min_total_mini_batches,
                    )
                    for eps_shard in eps_shards
                ]

            if async_update:
                # Retrieve all ready results (kicked off by prior calls to this method).
                results = None
                if self._update_request_tags:
                    results = self._worker_manager.fetch_ready_async_reqs(
                        tags=list(self._update_request_tags)
                    )

                update_tag = str(uuid.uuid4())

                num_sent_requests = self._worker_manager.foreach_actor_async(
                    partials, tag=update_tag
                )

                if num_sent_requests:
                    self._update_request_tags[update_tag] = num_sent_requests

                # Some requests were dropped, record lost ts/data.
                if num_sent_requests != len(self._workers):
                    # assert num_sent_requests == 0, num_sent_requests
                    factor = 1 - (num_sent_requests / len(self._workers))
                    if episodes is None:
                        self._ts_dropped += factor * len(batch)
                    else:
                        self._ts_dropped += factor * sum(len(e) for e in episodes)
                # NOTE: There is a strong assumption here that the requests launched to
                # learner workers will return at the same time, since they have a
                # barrier inside for gradient aggregation. Therefore, results should be
                # a list of lists where each inner list should be the length of the
                # number of learner workers, if results from an non-blocking update are
                # ready.
                results = self._get_async_results(results)

            else:
                results = self._get_results(
                    self._worker_manager.foreach_actor(partials)
                )

        # If we are on the old or hybrid API stacks (no EnvRunners), we need to emulate
        # the old behavior of returning an already reduced dict (as if we had a
        # reduce_fn).
        if not self.config.enable_env_runner_and_connector_v2:
            # If we are doing an ansync update, we operate on a list (different async
            # requests that now have results ready) of lists (n Learner workers) here.
            if async_update:
                results = tree.flatten_up_to(
                    [[None] * len(r) for r in results], results
                )
            self._metrics_logger_old_and_hybrid_stack.log_n_dicts(results)
            results = self._metrics_logger_old_and_hybrid_stack.reduce(
                # We are returning to a client (Algorithm) that does NOT make any
                # use of MetricsLogger (or Stats) -> Convert all values to non-Stats
                # primitives.
                return_stats_obj=False
            )

        return results

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
        if results is None:
            return []

        unprocessed_results = defaultdict(list)
        for result in results:
            result_or_error = result.get()
            if result.ok:
                tag = result.tag
                if not tag:
                    raise RuntimeError(
                        "Cannot call `LearnerGroup._get_async_results()` on untagged "
                        "async requests!"
                    )
                unprocessed_results[tag].append(result_or_error)

                if tag in self._update_request_tags:
                    self._update_request_tags[tag] -= 1
                    if self._update_request_tags[tag] == 0:
                        del self._update_request_tags[tag]
                else:
                    assert tag in self._additional_update_request_tags
                    self._additional_update_request_tags[tag] -= 1
                    if self._additional_update_request_tags[tag] == 0:
                        del self._additional_update_request_tags[tag]

            else:
                raise result_or_error

        return list(unprocessed_results.values())

    def additional_update(
        self,
        *,
        reduce_fn=DEPRECATED_VALUE,
        **kwargs,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Apply additional non-gradient based updates to the Learners.

        For example, this could be used to do a polyak averaging update
        of a target network in off policy algorithms like SAC or DQN.

        By default, this is a pass through that calls all Learner workers'
        `additional_update(**kwargs)` method.

        Returns:
            A list of dictionaries of results returned by the
            `Learner.additional_update()` calls.
        """
        if reduce_fn != DEPRECATED_VALUE:
            deprecation_warning(
                old="LearnerGroup.additional_update(reduce_fn=..)",
                new="Learner.metrics.[log_value|log_dict|log_time](key=..., value=..., "
                "reduce=[mean|min|max|sum], window=..., ema_coeff=...)",
                help="Use the new ray.rllib.utils.metrics.metrics_logger::MetricsLogger"
                " API in your custom Learner methods for logging your custom values "
                "and time-reducing (or parallel-reducing) them.",
                error=True,
            )

        if self.is_local:
            results = [self._learner.additional_update(**kwargs)]
        else:
            results = self._worker_manager.foreach_actor(
                [lambda w: w.additional_update(**kwargs) for _ in self._workers]
            )
            results = self._get_results(results)

        # If we are on hybrid API stack (no EnvRunners), we need to emulate
        # the existing behavior of returning an already reduced dict (as if we had a
        # reduce_fn).
        if not self.config.enable_env_runner_and_connector_v2:
            self._metrics_logger_old_and_hybrid_stack.log_n_dicts(results)
            results = self._metrics_logger_old_and_hybrid_stack.reduce(
                return_stats_obj=False
            )

        return results

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

        # Remove all stats from the module from our metrics logger (hybrid API stack
        # only), so we don't report results from this module again.
        if (
            not self.config.enable_env_runner_and_connector_v2
            and module_id in self._metrics_logger_old_and_hybrid_stack.stats
        ):
            del self._metrics_logger_old_and_hybrid_stack.stats[module_id]

    def get_weights(
        self, module_ids: Optional[Set[str]] = None, inference_only: bool = False
    ) -> Dict[str, Any]:
        """Get the weights of the MultiAgentRLModule maintained by each Learner.

        Args:
            module_ids: The ids of the modules to get the weights of.

        Returns:
            A mapping of module ids to their weights.

        """
        if self.is_local:
            state = self._learner.get_module_state(module_ids, inference_only)
        else:
            worker = self._worker_manager.healthy_actor_ids()[0]
            assert len(self._workers) == self._worker_manager.num_healthy_actors()
            state = self._worker_manager.foreach_actor(
                lambda w: w.get_module_state(module_ids, inference_only),
                remote_actor_ids=[worker],
            )
            state = self._get_results(state)[0]

        return convert_to_numpy(state)

    def set_weights(self, weights: Dict[str, Any]) -> None:
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

    def get_state(self) -> Dict[str, Any]:
        """Get the states of this LearnerGroup.

        Contains the Learners' state (which should be the same across Learners) and
        some other information.

        Returns:
            The state dict mapping str keys to state information.
        """
        if self.is_local:
            learner_state = self._learner.get_state()
        else:
            worker = self._worker_manager.healthy_actor_ids()[0]
            assert len(self._workers) == self._worker_manager.num_healthy_actors()
            results = self._worker_manager.foreach_actor(
                lambda w: w.get_state(), remote_actor_ids=[worker]
            )
            learner_state = self._get_results(results)[0]

        return {"learner_state": learner_state}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of this LearnerGroup.

        Note that all Learners share the same state.

        Args:
            state: The state dict mapping str keys to state information.
        """
        learner_state = state.get("learner_state")
        if learner_state is not None:
            if self.is_local:
                self._learner.set_state(learner_state)
            else:
                self._worker_manager.foreach_actor(lambda w: w.set_state(learner_state))

    def foreach_learner(
        self, func: Callable[[Learner, Optional[Any]], T], **kwargs
    ) -> List[T]:
        """Calls the given function on each Learner L with the args: (L, \*\*kwargs).

        Args:
            func: The function to call on each Learner L with (L, \*\*kwargs).

        Returns:
            A list of size len(Learners) with the return values of all calls to `func`.
        """
        if self.is_local:
            return [func(self._learner, **kwargs)]
        return self._worker_manager.foreach_actor(partial(func, **kwargs))

    # TODO (sven): Why did we chose to re-invent the wheel here and provide load/save
    #  from/to disk functionality? This should all be replaced with a simple
    #  get/set_state logic, which returns/takes a dict and then loading and saving
    #  should be managed by the owner class (Algorithm/Trainable).
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
        rl_module_ckpt_dirs: Optional[Dict[ModuleID, str]] = None,
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
                for module_id in rl_module_ckpt_dirs.keys():
                    if module_id in modules_to_load:
                        raise ValueError(
                            f"module_id {module_id} was specified in both "
                            "`modules_to_load` AND `rl_module_ckpt_dirs`! "
                            "Specify a module to be loaded either in `modules_to_load` "
                            "or `rl_module_ckpt_dirs`, but not in both."
                        )
            else:
                modules_to_load = module_keys - set(rl_module_ckpt_dirs.keys())

        # No need to do any file transfer operations if we are running training
        # on the experiment head node.
        if self.is_local:
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
        rl_module_ckpt_dirs: Optional[Dict[ModuleID, str]] = None,
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
        if self.is_remote and hasattr(self, "_backend_executor"):
            self._backend_executor.shutdown()
        self._is_shut_down = True

    def __del__(self):
        if not self._is_shut_down:
            self.shutdown()

    @staticmethod
    def _compute_num_total_mini_batches(batch_size, mini_batch_size, num_iters):
        num_total_mini_batches = 0
        rest_size = 0
        for i in range(num_iters):
            eaten_batch = -rest_size
            while eaten_batch < batch_size:
                eaten_batch += mini_batch_size
                num_total_mini_batches += 1
            rest_size = mini_batch_size - (eaten_batch - batch_size)
            if rest_size:
                num_total_mini_batches -= 1
        if rest_size:
            num_total_mini_batches += 1
        return num_total_mini_batches

    @Deprecated(new="LearnerGroup.update_from_batch(async=False)", error=False)
    def update(self, *args, **kwargs):
        # Just in case, we would like to revert this API retirement, we can do so
        # easily.
        return self._update(*args, **kwargs, async_update=False)

    @Deprecated(new="LearnerGroup.update_from_batch(async=True)", error=False)
    def async_update(self, *args, **kwargs):
        # Just in case, we would like to revert this API retirement, we can do so
        # easily.
        return self._update(*args, **kwargs, async_update=True)
