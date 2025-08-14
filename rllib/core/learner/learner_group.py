import copy
from functools import partial
import itertools
import pathlib
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Set,
    Type,
    TYPE_CHECKING,
    Union,
)

import ray
from ray.rllib.core import (
    COMPONENT_LEARNER,
    COMPONENT_RL_MODULE,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module import validate_module_id
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.actor_manager import (
    FaultTolerantActorManager,
    RemoteCallResults,
    ResultOrError,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.typing import (
    EpisodeType,
    ModuleID,
    RLModuleSpecType,
    ShouldModuleBeUpdatedFn,
    StateDict,
    T,
)
from ray.train._internal.backend_executor import BackendExecutor
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.util.placement_group import PlacementGroup


def _get_backend_config(learner_class: Type[Learner]) -> str:
    if learner_class.framework == "torch":
        from ray.train.torch.config import TorchConfig, _TorchBackend

        # Override `_TorchBackend` share_cuda_visible_devices=True setting.
        # We need this to be False to make sure Learner actors only see their
        # own GPU. There is no need in RLlib's LearnerGroups for 2 different Learner
        # actors to communicate with each other through their GPUs.
        class _RLlibTorchBackend(_TorchBackend):
            share_cuda_visible_devices = False

        class RLlibTorchConfig(TorchConfig):
            @property
            def backend_cls(self):
                return _RLlibTorchBackend

        backend_config = RLlibTorchConfig()

    else:
        raise ValueError(
            "`learner_class.framework` must be 'torch' (but is "
            f"{learner_class.framework}!"
        )

    return backend_config


class RLlibBackendExecutor(BackendExecutor):
    # Override `BackendExecutor` placement group creation logic. We need to pass our own
    # to make sure the one of the Algorithm (Trainable) is used for all the
    # Algorithm's actors.
    def _create_placement_group(self):
        pass

    # TODO (sven): Change this once there is a better (public) API for this in the
    #  superclass.
    def set_placement_group(self, placement_group):
        if placement_group is not None:
            self._placement_group = placement_group


@PublicAPI(stability="alpha")
class LearnerGroup(Checkpointable):
    """Coordinator of n (possibly remote) Learner workers.

    Each Learner worker has a copy of the RLModule, the loss function(s), and
    one or more optimizers.
    """

    def __init__(
        self,
        *,
        config: "AlgorithmConfig",
        # TODO (sven): Rename into `rl_module_spec`.
        module_spec: Optional[RLModuleSpecType] = None,
        placement_group: Optional["PlacementGroup"] = None,
    ):
        """Initializes a LearnerGroup instance.

        Args:
            config: The AlgorithmConfig object to use to configure this LearnerGroup.
                Call the `learners(num_learners=...)` method on your config to
                specify the number of learner workers to use.
                Call the same method with arguments `num_cpus_per_learner` and/or
                `num_gpus_per_learner` to configure the compute used by each
                Learner worker in this LearnerGroup.
                Call the `training(learner_class=...)` method on your config to specify,
                which exact Learner class to use.
                Call the `rl_module(rl_module_spec=...)` method on your config to set up
                the specifics for your RLModule to be used in each Learner.
            module_spec: If not already specified in `config`, a separate overriding
                RLModuleSpec may be provided via this argument.
            placement_group: An optional `PlacementGroup` instance to set the
                `RLlibBackendExecutor`'s `self._placement_group` attribute to.
                If run within an Algorithm (tune.Trainable), the placement group of tune
                trial actor is passed through here.
        """
        self.config = config.copy(copy_frozen=False)
        self._module_spec = module_spec

        learner_class = self.config.learner_class
        module_spec = module_spec or self.config.get_multi_rl_module_spec()

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

            num_cpus_per_learner = (
                self.config.num_cpus_per_learner
                if self.config.num_cpus_per_learner != "auto"
                else 1
                if self.config.num_gpus_per_learner == 0
                else 0
            )
            num_gpus_per_learner = max(0, self.config.num_gpus_per_learner)
            resources_per_learner = {
                "CPU": num_cpus_per_learner,
                "GPU": num_gpus_per_learner,
            }

            backend_executor = RLlibBackendExecutor(
                backend_config=backend_config,
                num_workers=self.config.num_learners,
                resources_per_worker=resources_per_learner,
                max_retries=0,
            )
            # Set the placement group - if any - of the BackendExecutor.
            backend_executor.set_placement_group(placement_group)
            backend_executor.start(
                train_cls=learner_class,
                train_cls_kwargs={
                    "config": config,
                    "module_spec": module_spec,
                },
            )
            self._backend_executor = backend_executor

            self._workers = [w.actor for w in backend_executor.worker_group.workers]

            ray.get(
                [
                    worker._set_learner_index_and_placement_group.remote(
                        learner_index=idx,
                        placement_group=placement_group,
                    )
                    for idx, worker in enumerate(self._workers)
                ]
            )

            # Run the neural network building code on remote workers.
            ray.get([w.build.remote() for w in self._workers])

            self._worker_manager = FaultTolerantActorManager(
                self._workers,
                max_remote_requests_in_flight_per_actor=(
                    self.config.max_requests_in_flight_per_learner
                ),
            )

    # TODO (sven): Replace this with call to `self.metrics.peek()`?
    #  Currently LearnerGroup does not have a metrics object.
    def get_stats(self) -> Dict[str, Any]:
        """Returns the current stats for the input queue for this learner group."""
        return {
            "learner_group_ts_dropped_lifetime": self._ts_dropped,
            "actor_manager_num_outstanding_async_reqs": (
                0
                if self.is_local
                else self._worker_manager.num_outstanding_async_reqs()
            ),
        }

    @property
    def is_remote(self) -> bool:
        return self.config.num_learners > 0

    @property
    def is_local(self) -> bool:
        return not self.is_remote

    def update(
        self,
        *,
        batch: Optional[MultiAgentBatch] = None,
        batches: Optional[List[MultiAgentBatch]] = None,
        batch_refs: Optional[List[ray.ObjectRef]] = None,
        episodes: Optional[List[EpisodeType]] = None,
        episodes_refs: Optional[List[ray.ObjectRef]] = None,
        data_iterators: Optional[List[ray.data.DataIterator]] = None,
        training_data: Optional[TrainingData] = None,
        timesteps: Optional[Dict[str, Any]] = None,
        async_update: bool = False,
        return_state: bool = False,
        # User kwargs passed onto the Learners.
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Performs gradient based updates on Learners, based on given training data.

        Args:
            batch: A data batch to use for the update. If there are more
                than one Learner workers, the batch is split amongst these and one
                shard is sent to each Learner.
            batch_refs:
            episodes: A list of Episodes to process and perform the update
                for. If there are more than one Learner workers, the list of episodes
                is split amongst these and one list shard is sent to each Learner.
            episodes_refs:
            timesteps:
            async_update: Whether the update request(s) to the Learner workers should be
                sent asynchronously. If True, will return NOT the results from the
                update on the given data, but all results from prior asynchronous update
                requests that have not been returned thus far.
            return_state: Whether to include one of the Learner worker's state from
                after the update step in the returned results dict (under the
                `_rl_module_state_after_update` key). Note that after an update, all
                Learner workers' states should be identical, so we use the first
                Learner's state here. Useful for avoiding an extra `get_weights()` call,
                e.g. for synchronizing EnvRunner weights.
            num_epochs: The number of complete passes over the entire train batch. Each
                pass might be further split into n minibatches (if `minibatch_size`
                provided).
            minibatch_size: The size of minibatches to use to further split the train
                `batch` into sub-batches. The `batch` is then iterated over n times
                where n is `len(batch) // minibatch_size`.
            shuffle_batch_per_epoch: Whether to shuffle the train batch once per epoch.
                If the train batch has a time rank (axis=1), shuffling will only take
                place along the batch axis to not disturb any intact (episode)
                trajectories. Also, shuffling is always skipped if `minibatch_size` is
                None, meaning the entire train batch is processed each epoch, making it
                unnecessary to shuffle.
            **kwargs:

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
        # Create and validate TrainingData object, if not already provided.
        if training_data is None:
            training_data = TrainingData(
                batch=batch,
                batches=batches,
                batch_refs=batch_refs,
                episodes=episodes,
                episodes_refs=episodes_refs,
                data_iterators=data_iterators,
            )
        training_data.validate()

        # Local Learner instance.
        if self.is_local:
            if async_update:
                raise ValueError(
                    "Can't call `update(async_update=True)` when running with "
                    "`num_learners=0`! Set `config.num_learners > 0` to allow async "
                    "updates."
                )
            # Solve all ray refs locally already here.
            training_data.solve_refs()
            if return_state:
                kwargs["return_state"] = return_state
            # Return the single Learner's update results.
            return [
                self._learner.update(
                    training_data=training_data,
                    timesteps=timesteps,
                    **kwargs,
                )
            ]

        # Remote Learner actors' kwargs.
        remote_call_kwargs = [
            dict(
                training_data=td_shard,
                timesteps=timesteps,
                # If `return_state=True`, only return it from the first Learner
                # actor.
                return_state=(return_state and i == 0),
                **kw,
                **kwargs,
            )
            for i, (td_shard, kw) in enumerate(
                training_data.shard(
                    num_shards=len(self),
                    len_lookback_buffer=self.config.episode_lookback_horizon,
                    **kwargs,
                )
            )
        ]

        # Async updates.
        if async_update:
            # Retrieve all ready results (kicked off by prior calls to this method).
            results = self._worker_manager.fetch_ready_async_reqs(timeout_seconds=0.0)
            # Send out new request(s), if there is still capacity on the actors
            # (each actor is allowed only some number of max in-flight requests
            # at the same time).
            num_sent_requests = self._worker_manager.foreach_actor_async(
                "update",
                kwargs=remote_call_kwargs,
            )

            # Some requests were dropped, record lost ts/data.
            if num_sent_requests != len(self):
                factor = 1 - (num_sent_requests / len(self))
                # TODO (sven): Move this logic into a TrainingData API as well
                #  (`TrainingData.env_steps()`).
                if training_data.batch_refs is not None:
                    dropped = (
                        len(training_data.batch_refs)
                        * self.config.train_batch_size_per_learner
                    )
                elif training_data.batch is not None:
                    dropped = len(training_data.batch)
                # List of Ray ObjectRefs (each object ref is a list of episodes of
                # total len=`rollout_fragment_length * num_envs_per_env_runner`)
                elif training_data.episodes_refs is not None:
                    dropped = (
                        len(training_data.episodes_refs)
                        * self.config.get_rollout_fragment_length()
                        * self.config.num_envs_per_env_runner
                    )
                else:
                    assert training_data.episodes is not None
                    dropped = sum(len(e) for e in training_data.episodes)

                self._ts_dropped += factor * dropped
        # Sync updates.
        else:
            results = self._worker_manager.foreach_actor(
                "update",
                kwargs=remote_call_kwargs,
            )

        results = self._get_results(results)
        return results

    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: RLModuleSpec,
        config_overrides: Optional[Dict] = None,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
    ) -> MultiRLModuleSpec:
        """Adds a module to the underlying MultiRLModule.

        Changes this Learner's config in order to make this architectural change
        permanent wrt. to checkpointing.

        Args:
            module_id: The ModuleID of the module to be added.
            module_spec: The ModuleSpec of the module to be added.
            config_overrides: The `AlgorithmConfig` overrides that should apply to
                the new Module, if any.
            new_should_module_be_updated: An optional sequence of ModuleIDs or a
                callable taking ModuleID and SampleBatchType and returning whether the
                ModuleID should be updated (trained).
                If None, will keep the existing setup in place. RLModules,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.

        Returns:
            The new MultiRLModuleSpec (after the change has been performed).
        """
        validate_module_id(module_id, error=True)

        # Force-set inference-only = False.
        module_spec = copy.deepcopy(module_spec)
        module_spec.inference_only = False

        results = self.foreach_learner(
            func=lambda _learner: _learner.add_module(
                module_id=module_id,
                module_spec=module_spec,
                config_overrides=config_overrides,
                new_should_module_be_updated=new_should_module_be_updated,
            ),
        )
        marl_spec = self._get_results(results)[0]

        # Change our config (AlgorithmConfig) to contain the new Module.
        # TODO (sven): This is a hack to manipulate the AlgorithmConfig directly,
        #  but we'll deprecate config.policies soon anyway.
        self.config.policies[module_id] = PolicySpec()
        if config_overrides is not None:
            self.config.multi_agent(
                algorithm_config_overrides_per_module={module_id: config_overrides}
            )
        self.config.rl_module(rl_module_spec=marl_spec)
        if new_should_module_be_updated is not None:
            self.config.multi_agent(policies_to_train=new_should_module_be_updated)

        return marl_spec

    def remove_module(
        self,
        module_id: ModuleID,
        *,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
    ) -> MultiRLModuleSpec:
        """Removes a module from the Learner.

        Args:
            module_id: The ModuleID of the module to be removed.
            new_should_module_be_updated: An optional sequence of ModuleIDs or a
                callable taking ModuleID and SampleBatchType and returning whether the
                ModuleID should be updated (trained).
                If None, will keep the existing setup in place. RLModules,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.

        Returns:
            The new MultiRLModuleSpec (after the change has been performed).
        """
        results = self.foreach_learner(
            func=lambda _learner: _learner.remove_module(
                module_id=module_id,
                new_should_module_be_updated=new_should_module_be_updated,
            ),
        )
        marl_spec = self._get_results(results)[0]

        # Change self.config to reflect the new architecture.
        # TODO (sven): This is a hack to manipulate the AlgorithmConfig directly,
        #  but we'll deprecate config.policies soon anyway.
        del self.config.policies[module_id]
        self.config.algorithm_config_overrides_per_module.pop(module_id, None)
        if new_should_module_be_updated is not None:
            self.config.multi_agent(policies_to_train=new_should_module_be_updated)
        self.config.rl_module(rl_module_spec=marl_spec)

        return marl_spec

    @override(Checkpointable)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        state = {}

        if self._check_component(COMPONENT_LEARNER, components, not_components):
            if self.is_local:
                state[COMPONENT_LEARNER] = self._learner.get_state(
                    components=self._get_subcomponents(COMPONENT_LEARNER, components),
                    not_components=self._get_subcomponents(
                        COMPONENT_LEARNER, not_components
                    ),
                    **kwargs,
                )
            else:
                worker = self._worker_manager.healthy_actor_ids()[0]
                assert len(self) == self._worker_manager.num_healthy_actors()
                _comps = self._get_subcomponents(COMPONENT_LEARNER, components)
                _not_comps = self._get_subcomponents(COMPONENT_LEARNER, not_components)
                results = self._worker_manager.foreach_actor(
                    lambda w: w.get_state(_comps, not_components=_not_comps, **kwargs),
                    remote_actor_ids=[worker],
                )
                state[COMPONENT_LEARNER] = self._get_results(results)[0]

        return state

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        if COMPONENT_LEARNER in state:
            if self.is_local:
                self._learner.set_state(state[COMPONENT_LEARNER])
            else:
                state_ref = ray.put(state[COMPONENT_LEARNER])
                self.foreach_learner(
                    lambda _learner, _ref=state_ref: _learner.set_state(ray.get(_ref))
                )

    def get_weights(
        self, module_ids: Optional[Collection[ModuleID]] = None
    ) -> StateDict:
        """Convenience method instead of self.get_state(components=...).

        Args:
            module_ids: An optional collection of ModuleIDs for which to return weights.
                If None (default), return weights of all RLModules.

        Returns:
            The results of
            `self.get_state(components='learner/rl_module')['learner']['rl_module']`.
        """
        # Return the entire RLModule state (all possible single-agent RLModules).
        if module_ids is None:
            components = COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE
        # Return a subset of the single-agent RLModules.
        else:
            components = [
                "".join(tup)
                for tup in itertools.product(
                    [COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/"],
                    list(module_ids),
                )
            ]
        state = self.get_state(components)[COMPONENT_LEARNER][COMPONENT_RL_MODULE]
        return state

    def set_weights(self, weights) -> None:
        """Convenience method instead of self.set_state({'learner': {'rl_module': ..}}).

        Args:
            weights: The weights dict of the MultiRLModule of a Learner inside this
                LearnerGroup.
        """
        self.set_state({COMPONENT_LEARNER: {COMPONENT_RL_MODULE: weights}})

    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self):
        return (
            (),  # *args
            {
                "config": self.config,
                "module_spec": self._module_spec,
            },  # **kwargs
        )

    @override(Checkpointable)
    def get_checkpointable_components(self):
        # Return the entire ActorManager, if remote. Otherwise, return the
        # local worker. Also, don't give the component (Learner) a name ("")
        # as it's the only component in this LearnerGroup to be saved.
        return [
            (
                COMPONENT_LEARNER,
                self._learner if self.is_local else self._worker_manager,
            )
        ]

    def foreach_learner(
        self,
        func: Callable[[Learner, Optional[Any]], T],
        *,
        healthy_only: bool = True,
        remote_actor_ids: List[int] = None,
        timeout_seconds: Optional[float] = None,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
        **kwargs,
    ) -> RemoteCallResults:
        r"""Calls the given function on each Learner L with the args: (L, \*\*kwargs).

        Args:
            func: The function to call on each Learner L with args: (L, \*\*kwargs).
            healthy_only: If True, applies `func` only to Learner actors currently
                tagged "healthy", otherwise to all actors. If `healthy_only=False` and
                `mark_healthy=True`, will send `func` to all actors and mark those
                actors "healthy" that respond to the request within `timeout_seconds`
                and are currently tagged as "unhealthy".
            remote_actor_ids: Apply func on a selected set of remote actors. Use None
                (default) for all actors.
            timeout_seconds: Time to wait (in seconds) for results. Set this to 0.0 for
                fire-and-forget. Set this to None (default) to wait infinitely (i.e. for
                synchronous execution).
            return_obj_refs: whether to return ObjectRef instead of actual results.
                Note, for fault tolerance reasons, these returned ObjectRefs should
                never be resolved with ray.get() outside of the context of this manager.
            mark_healthy: Whether to mark all those actors healthy again that are
                currently marked unhealthy AND that returned results from the remote
                call (within the given `timeout_seconds`).
                Note that actors are NOT set unhealthy, if they simply time out
                (only if they return a RayActorError).
                Also not that this setting is ignored if `healthy_only=True` (b/c this
                setting only affects actors that are currently tagged as unhealthy).

        Returns:
            A list of size len(Learners) with the return values of all calls to `func`.
        """
        if self.is_local:
            results = RemoteCallResults()
            results.add_result(
                None,
                ResultOrError(result=func(self._learner, **kwargs)),
                None,
            )
            return results

        return self._worker_manager.foreach_actor(
            func=partial(func, **kwargs),
            healthy_only=healthy_only,
            remote_actor_ids=remote_actor_ids,
            timeout_seconds=timeout_seconds,
            return_obj_refs=return_obj_refs,
            mark_healthy=mark_healthy,
        )

    def __len__(self):
        return 0 if self.is_local else len(self._workers)

    def shutdown(self):
        """Shuts down the LearnerGroup."""
        if self.is_remote and hasattr(self, "_backend_executor"):
            self._backend_executor.shutdown(graceful_termination=True)
        self._is_shut_down = True

    def __del__(self):
        if not self._is_shut_down:
            self.shutdown()

    def _get_results(self, results):
        processed_results = []
        for result in results:
            result_or_error = result.get()
            if result.ok:
                processed_results.append(result_or_error)
            else:
                raise result_or_error
        return processed_results

    @Deprecated(new="LearnerGroup.update(batch=.., **kwargs)", error=False)
    def update_from_batch(self, batch, **kwargs):
        return self.update(batch=batch, **kwargs)

    @Deprecated(new="LearnerGroup.update(episodes=.., **kwargs)", error=False)
    def update_from_episodes(self, episodes, **kwargs):
        return self.update(episodes=episodes, **kwargs)

    @Deprecated(new="LearnerGroup.update_from_batch(async=True)", error=True)
    def async_update(self, *args, **kwargs):
        pass

    @Deprecated(new="LearnerGroup.load_from_path(path=..., component=...)", error=False)
    def load_module_state(
        self,
        *,
        multi_rl_module_ckpt_dir: Optional[str] = None,
        modules_to_load: Optional[Set[str]] = None,
        rl_module_ckpt_dirs: Optional[Dict[ModuleID, str]] = None,
    ) -> None:
        """Load the checkpoints of the modules being trained by this LearnerGroup.

        `load_module_state` can be used 3 ways:
            1. Load a checkpoint for the MultiRLModule being trained by this
                LearnerGroup. Limit the modules that are loaded from the checkpoint
                by specifying the `modules_to_load` argument.
            2. Load the checkpoint(s) for single agent RLModules that
                are in the MultiRLModule being trained by this LearnerGroup.
            3. Load a checkpoint for the MultiRLModule being trained by this
                LearnerGroup and load the checkpoint(s) for single agent RLModules
                that are in the MultiRLModule. The checkpoints for the single
                agent RLModules take precedence over the module states in the
                MultiRLModule checkpoint.

        NOTE: At lease one of multi_rl_module_ckpt_dir or rl_module_ckpt_dirs is
            must be specified. modules_to_load can only be specified if
            multi_rl_module_ckpt_dir is specified.

        Args:
            multi_rl_module_ckpt_dir: The path to the checkpoint for the
                MultiRLModule.
            modules_to_load: A set of module ids to load from the checkpoint.
            rl_module_ckpt_dirs: A mapping from module ids to the path to a
                checkpoint for a single agent RLModule.
        """
        if not (multi_rl_module_ckpt_dir or rl_module_ckpt_dirs):
            raise ValueError(
                "At least one of `multi_rl_module_ckpt_dir` or "
                "`rl_module_ckpt_dirs` must be provided!"
            )
        if multi_rl_module_ckpt_dir:
            multi_rl_module_ckpt_dir = pathlib.Path(multi_rl_module_ckpt_dir)
        if rl_module_ckpt_dirs:
            for module_id, path in rl_module_ckpt_dirs.items():
                rl_module_ckpt_dirs[module_id] = pathlib.Path(path)

        # MultiRLModule checkpoint is provided.
        if multi_rl_module_ckpt_dir:
            # Restore the entire MultiRLModule state.
            if modules_to_load is None:
                self.restore_from_path(
                    multi_rl_module_ckpt_dir,
                    component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE,
                )
            # Restore individual module IDs.
            else:
                for module_id in modules_to_load:
                    self.restore_from_path(
                        multi_rl_module_ckpt_dir / module_id,
                        component=(
                            COMPONENT_LEARNER
                            + "/"
                            + COMPONENT_RL_MODULE
                            + "/"
                            + module_id
                        ),
                    )
        if rl_module_ckpt_dirs:
            for module_id, path in rl_module_ckpt_dirs.items():
                self.restore_from_path(
                    path,
                    component=(
                        COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/" + module_id
                    ),
                )
