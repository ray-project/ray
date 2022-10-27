import gym
import logging
import importlib.util
import os
from types import FunctionType
from typing import (
    Callable,
    Container,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayError
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.env_context import EnvContext
from ray.rllib.offline import (
    NoopOutput,
    JsonReader,
    MixedInput,
    JsonWriter,
    ShuffledInput,
    D4RLReader,
    DatasetReader,
    DatasetWriter,
    get_dataset_and_shards,
)
from ray.rllib.policy.policy import Policy, PolicySpec, PolicyState
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.policy import validate_policy_id
from ray.rllib.utils.typing import (
    AgentID,
    AlgorithmConfigDict,
    EnvCreator,
    EnvType,
    EpisodeID,
    PartialAlgorithmConfigDict,
    PolicyID,
    SampleBatchType,
    TensorType,
)
from ray.tune.registry import registry_contains_input, registry_get_input

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)

# Generic type var for foreach_* methods.
T = TypeVar("T")


@DeveloperAPI
class WorkerSet:
    """Set of RolloutWorkers with n @ray.remote workers and zero or one local worker.

    Where: n >= 0.
    """

    def __init__(
        self,
        *,
        env_creator: Optional[EnvCreator] = None,
        validate_env: Optional[Callable[[EnvType], None]] = None,
        policy_class: Optional[Type[Policy]] = None,
        trainer_config: Optional[AlgorithmConfigDict] = None,
        num_workers: int = 0,
        local_worker: bool = True,
        logdir: Optional[str] = None,
        _setup: bool = True,
    ):
        """Initializes a WorkerSet instance.

        Args:
            env_creator: Function that returns env given env config.
            validate_env: Optional callable to validate the generated
                environment (only on worker=0).
            policy_class: An optional Policy class. If None, PolicySpecs can be
                generated automatically by using the Algorithm's default class
                of via a given multi-agent policy config dict.
            trainer_config: Optional dict that extends the common config of
                the Algorithm class.
            num_workers: Number of remote rollout workers to create.
            local_worker: Whether to create a local (non @ray.remote) worker
                in the returned set as well (default: True). If `num_workers`
                is 0, always create a local worker.
            logdir: Optional logging directory for workers.
            _setup: Whether to setup workers. This is only for testing.
        """

        if not trainer_config:
            from ray.rllib.algorithms.algorithm import COMMON_CONFIG

            trainer_config = COMMON_CONFIG

        self._env_creator = env_creator
        self._policy_class = policy_class
        self._remote_config = trainer_config
        self._remote_args = {
            "num_cpus": self._remote_config["num_cpus_per_worker"],
            "num_gpus": self._remote_config["num_gpus_per_worker"],
            "resources": self._remote_config["custom_resources_per_worker"],
        }
        self._cls = RolloutWorker.as_remote(**self._remote_args).remote
        self._logdir = logdir
        if _setup:
            # Force a local worker if num_workers == 0 (no remote workers).
            # Otherwise, this WorkerSet would be empty.
            self._local_worker = None
            if num_workers == 0:
                local_worker = True
            self._local_config = merge_dicts(
                trainer_config,
                {"tf_session_args": trainer_config["local_tf_session_args"]},
            )

            if trainer_config["input"] == "dataset":
                # Create the set of dataset readers to be shared by all the
                # rollout workers.
                self._ds, self._ds_shards = get_dataset_and_shards(
                    trainer_config, num_workers
                )
            else:
                self._ds = None
                self._ds_shards = None

            # Create a number of @ray.remote workers.
            self._remote_workers = []
            self.add_workers(
                num_workers,
                validate=trainer_config.get("validate_workers_after_construction"),
            )

            # Create a local worker, if needed.
            # If num_workers > 0 and we don't have an env on the local worker,
            # get the observation- and action spaces for each policy from
            # the first remote worker (which does have an env).
            if (
                local_worker
                and self._remote_workers
                and not trainer_config.get("create_env_on_driver")
                and (
                    not trainer_config.get("observation_space")
                    or not trainer_config.get("action_space")
                )
            ):
                remote_spaces = ray.get(
                    self.remote_workers()[0].foreach_policy.remote(
                        lambda p, pid: (pid, p.observation_space, p.action_space)
                    )
                )
                spaces = {
                    e[0]: (getattr(e[1], "original_space", e[1]), e[2])
                    for e in remote_spaces
                }
                # Try to add the actual env's obs/action spaces.
                try:
                    env_spaces = ray.get(
                        self.remote_workers()[0].foreach_env.remote(
                            lambda env: (env.observation_space, env.action_space)
                        )
                    )[0]
                    spaces["__env__"] = env_spaces
                except Exception:
                    pass

                logger.info(
                    "Inferred observation/action spaces from remote "
                    f"worker (local worker has no env): {spaces}"
                )
            else:
                spaces = None

            if local_worker:
                self._local_worker = self._make_worker(
                    cls=RolloutWorker,
                    env_creator=env_creator,
                    validate_env=validate_env,
                    policy_cls=self._policy_class,
                    # Initially, policy_specs will be inferred from config dict.
                    policy_specs=None,
                    worker_index=0,
                    num_workers=num_workers,
                    config=self._local_config,
                    spaces=spaces,
                )

    def local_worker(self) -> RolloutWorker:
        """Returns the local rollout worker."""
        return self._local_worker

    def remote_workers(self) -> List[ActorHandle]:
        """Returns a list of remote rollout workers."""
        return self._remote_workers

    def sync_weights(
        self,
        policies: Optional[List[PolicyID]] = None,
        from_worker: Optional[RolloutWorker] = None,
        global_vars: Optional[Dict[str, TensorType]] = None,
    ) -> None:
        """Syncs model weights from the local worker to all remote workers.

        Args:
            policies: Optional list of PolicyIDs to sync weights for.
                If None (default), sync weights to/from all policies.
            from_worker: Optional RolloutWorker instance to sync from.
                If None (default), sync from this WorkerSet's local worker.
            global_vars: An optional global vars dict to set this
                worker to. If None, do not update the global_vars.
        """
        if self.local_worker() is None and from_worker is None:
            raise TypeError(
                "No `local_worker` in WorkerSet, must provide `from_worker` "
                "arg in `sync_weights()`!"
            )

        # Only sync if we have remote workers or `from_worker` is provided.
        weights = None
        if self.remote_workers() or from_worker is not None:
            weights = (from_worker or self.local_worker()).get_weights(policies)
            # Put weights only once into object store and use same object
            # ref to synch to all workers.
            weights_ref = ray.put(weights)
            # Sync to all remote workers in this WorkerSet.
            for to_worker in self.remote_workers():
                to_worker.set_weights.remote(weights_ref, global_vars=global_vars)

        # If `from_worker` is provided, also sync to this WorkerSet's
        # local worker.
        if from_worker is not None and self.local_worker() is not None:
            self.local_worker().set_weights(weights, global_vars=global_vars)
        # If `global_vars` is provided and local worker exists  -> Update its
        # global_vars.
        elif self.local_worker() is not None and global_vars is not None:
            self.local_worker().set_global_vars(global_vars)

    def add_policy(
        self,
        policy_id: PolicyID,
        policy_cls: Optional[Type[Policy]] = None,
        policy: Optional[Policy] = None,
        *,
        observation_space: Optional[gym.spaces.Space] = None,
        action_space: Optional[gym.spaces.Space] = None,
        config: Optional[PartialAlgorithmConfigDict] = None,
        policy_state: Optional[PolicyState] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Container[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
        workers: Optional[List[Union[RolloutWorker, ActorHandle]]] = None,
    ) -> None:
        """Adds a policy to this WorkerSet's workers or a specific list of workers.

        Args:
            policy_id: ID of the policy to add.
            policy_cls: The Policy class to use for constructing the new Policy.
                Note: Only one of `policy_cls` or `policy` must be provided.
            policy: The Policy instance to add to this WorkerSet. If not None, the
                given Policy object will be directly inserted into the
                local worker and clones of that Policy will be created on all remote
                workers.
                Note: Only one of `policy_cls` or `policy` must be provided.
            observation_space: The observation space of the policy to add.
                If None, try to infer this space from the environment.
            action_space: The action space of the policy to add.
                If None, try to infer this space from the environment.
            config: The config overrides for the policy to add.
            policy_state: Optional state dict to apply to the new
                policy instance, right after its construction.
            policy_mapping_fn: An optional (updated) policy mapping function
                to use from here on. Note that already ongoing episodes will
                not change their mapping but will use the old mapping till
                the end of the episode.
            policies_to_train: An optional list of policy IDs to be trained
                or a callable taking PolicyID and SampleBatchType and
                returning a bool (trainable or not?).
                If None, will keep the existing setup in place. Policies,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.
            workers: A list of RolloutWorker/ActorHandles (remote
                RolloutWorkers) to add this policy to. If defined, will only
                add the given policy to these workers.

        Raises:
            KeyError: If the given `policy_id` already exists in this WorkerSet.
        """
        if (
            workers is None
            and self.local_worker()
            and policy_id in self.local_worker().policy_map
        ):
            raise KeyError(
                f"Policy ID '{policy_id}' already exists in policy map! "
                "Make sure you use a Policy ID that has not been taken yet."
                " Policy IDs that are already in your policy map: "
                f"{list(self.local_worker().policy_map.keys())}"
            )

        # No `workers` arg provided: Compile list of workers automatically from
        # all RolloutWorkers in this WorkerSet.
        if workers is None:
            workers = (
                [self.local_worker()] if self.local_worker() else []
            ) + self.remote_workers()

        self.add_policy_to_workers(
            workers=workers,
            policy_id=policy_id,
            policy_cls=policy_cls,
            policy=policy,
            observation_space=observation_space,
            action_space=action_space,
            config=config,
            policy_state=policy_state,
            policy_mapping_fn=policy_mapping_fn,
            policies_to_train=policies_to_train,
        )

    @staticmethod
    def add_policy_to_workers(
        workers: List[Union[RolloutWorker, ActorHandle]],
        policy_id: PolicyID,
        policy_cls: Optional[Type[Policy]] = None,
        policy: Optional[Policy] = None,
        *,
        observation_space: Optional[gym.spaces.Space] = None,
        action_space: Optional[gym.spaces.Space] = None,
        config: Optional[PartialAlgorithmConfigDict] = None,
        policy_state: Optional[PolicyState] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Container[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
    ) -> None:
        """Adds a new policy to a specific list of RolloutWorkers (or remote actors).

        Args:
            workers: A list of RolloutWorker/ActorHandles (remote
                RolloutWorkers) to add this policy to.
            policy_id: ID of the policy to add.
            policy_cls: The Policy class to use for constructing the new Policy.
                Note: Only one of `policy_cls` or `policy` must be provided.
            policy: The Policy instance to add to this WorkerSet. If not None, the
                given Policy object will be directly inserted into the
                local worker and clones of that Policy will be created on all remote
                workers.
                Note: Only one of `policy_cls` or `policy` must be provided.
            observation_space: The observation space of the policy to add.
                If None, try to infer this space from the environment.
            action_space: The action space of the policy to add.
                If None, try to infer this space from the environment.
            config: The config overrides for the policy to add.
            policy_state: Optional state dict to apply to the new
                policy instance, right after its construction.
            policy_mapping_fn: An optional (updated) policy mapping function
                to use from here on. Note that already ongoing episodes will
                not change their mapping but will use the old mapping till
                the end of the episode.
            policies_to_train: An optional list of policy IDs to be trained
                or a callable taking PolicyID and SampleBatchType and
                returning a bool (trainable or not?).
                If None, will keep the existing setup in place. Policies,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.

        Raises:
            ValueError: If both `policy_cls` AND `policy` are provided.
            ValueError: If Policy ID is not a valid one.
        """
        if (policy_cls is None) == (policy is None):
            raise ValueError(
                "Only one of `policy_cls` or `policy` must be provided to "
                "staticmethod: `WorkerSet.add_policy_to_workers()`!"
            )
        validate_policy_id(policy_id, error=False)

        # Policy instance not provided: Use the information given here.
        if policy_cls is not None:
            new_policy_instance_kwargs = dict(
                policy_id=policy_id,
                policy_cls=policy_cls,
                observation_space=observation_space,
                action_space=action_space,
                config=config,
                policy_state=policy_state,
                policy_mapping_fn=policy_mapping_fn,
                policies_to_train=list(policies_to_train)
                if policies_to_train
                else None,
            )
        # Policy instance provided: Create clones of this very policy on the different
        # workers (copy all its properties here for the calls to add_policy on the
        # remote workers).
        else:
            new_policy_instance_kwargs = dict(
                policy_id=policy_id,
                policy_cls=type(policy),
                observation_space=policy.observation_space,
                action_space=policy.action_space,
                config=policy.config,
                policy_state=policy.get_state(),
                policy_mapping_fn=policy_mapping_fn,
                policies_to_train=list(policies_to_train)
                if policies_to_train
                else None,
            )

        def _create_new_policy_fn(worker: RolloutWorker):
            # `foreach_worker` function: Adds the policy the the worker (and
            # maybe changes its policy_mapping_fn - if provided here).
            worker.add_policy(**new_policy_instance_kwargs)

        ray_gets = []
        for worker in workers:
            # Existing policy AND local worker: Add Policy as-is.
            if policy is not None and not isinstance(worker, ActorHandle):
                worker.add_policy(
                    policy_id=policy_id,
                    policy=policy,
                    policy_mapping_fn=policy_mapping_fn,
                    policies_to_train=policies_to_train,
                )
            # A remote worker (ray actor).
            elif isinstance(worker, ActorHandle):
                ray_gets.append(worker.apply.remote(_create_new_policy_fn))
            # (Local) RolloutWorker instance.
            else:
                worker.add_policy(**new_policy_instance_kwargs)

        ray.get(ray_gets)

    def add_workers(self, num_workers: int, validate: bool = False) -> None:
        """Creates and adds a number of remote workers to this worker set.

        Can be called several times on the same WorkerSet to add more
        RolloutWorkers to the set.

        Args:
            num_workers: The number of remote Workers to add to this
                WorkerSet.
            validate: Whether to validate remote workers after their construction
                process.

        Raises:
            RayError: If any of the constructed remote workers is not up and running
            properly.
        """
        old_num_workers = len(self._remote_workers)
        self._remote_workers.extend(
            [
                self._make_worker(
                    cls=self._cls,
                    env_creator=self._env_creator,
                    validate_env=None,
                    policy_cls=self._policy_class,
                    # Setup remote workers with policy_specs inferred from config dict.
                    # Simply provide None here.
                    policy_specs=None,
                    worker_index=old_num_workers + i + 1,
                    num_workers=old_num_workers + num_workers,
                    config=self._remote_config,
                )
                for i in range(num_workers)
            ]
        )

        # Validate here, whether all remote workers have been constructed properly
        # and are "up and running". If not, the following will throw a RayError
        # which needs to be handled by this WorkerSet's owner (usually
        # a RLlib Algorithm instance).
        if validate:
            self.foreach_worker(lambda w: w.assert_healthy())

    def reset(self, new_remote_workers: List[ActorHandle]) -> None:
        """Hard overrides the remote workers in this set with the given one.

        Args:
            new_remote_workers: A list of new RolloutWorkers
                (as `ActorHandles`) to use as remote workers.
        """
        self._remote_workers = new_remote_workers

    def remove_failed_workers(self):
        faulty_indices = self._worker_health_check()
        removed_workers = []
        # Terminate faulty workers.
        for worker_index in faulty_indices:
            worker = self.remote_workers()[worker_index - 1]
            logger.info(f"Trying to terminate faulty worker {worker_index}.")
            try:
                worker.__ray_terminate__.remote()
                removed_workers.append(worker)
            except Exception:
                logger.exception("Error terminating faulty worker.")

        # Remove all faulty workers from self._remote_workers.
        for worker_index in reversed(faulty_indices):
            del self._remote_workers[worker_index - 1]
        # TODO: Should we also change each healthy worker's num_workers counter and
        #  worker_index property?

        if len(self.remote_workers()) == 0:
            raise RuntimeError(
                f"No healthy workers remaining (worker indices {faulty_indices} have "
                f"died)! Can't continue training."
            )
        return removed_workers

    def recreate_failed_workers(
        self, local_worker_for_synching: RolloutWorker
    ) -> Tuple[List[ActorHandle], List[ActorHandle]]:
        """Recreates any failed workers (after health check).

        Args:
            local_worker_for_synching: RolloutWorker to use to synchronize the weights
                after recreation.

        Returns:
            A tuple consisting of two items: The list of removed workers and the list of
            newly added ones.
        """
        faulty_indices = self._worker_health_check()
        removed_workers = []
        new_workers = []
        for worker_index in faulty_indices:
            worker = self.remote_workers()[worker_index - 1]
            removed_workers.append(worker)
            logger.info(f"Trying to recreate faulty worker {worker_index}")
            try:
                worker.__ray_terminate__.remote()
            except Exception:
                logger.exception("Error terminating faulty worker.")
            # Try to recreate the failed worker (start a new one).
            new_worker = self._make_worker(
                cls=self._cls,
                env_creator=self._env_creator,
                validate_env=None,
                policy_cls=self._policy_class,
                # For recreated remote workers, we need to sync the entire
                # policy specs dict from local_worker_for_synching.
                # We can not let self._make_worker() infer policy specs
                # from self._remote_config dict because custom policies
                # may be added to both rollout and evaluation workers
                # while the training job progresses.
                policy_specs=local_worker_for_synching.policy_dict,
                worker_index=worker_index,
                num_workers=len(self._remote_workers),
                recreated_worker=True,
                config=self._remote_config,
            )

            # Sync new worker from provided one (or local one).
            # Restore weights and global variables.
            new_worker.set_weights.remote(
                weights=local_worker_for_synching.get_weights(),
                global_vars=local_worker_for_synching.get_global_vars(),
            )

            # Add new worker to list of remote workers.
            self._remote_workers[worker_index - 1] = new_worker
            new_workers.append(new_worker)

        return removed_workers, new_workers

    def stop(self) -> None:
        """Calls `stop` on all rollout workers (including the local one)."""
        try:
            if self.local_worker():
                self.local_worker().stop()
            tids = [w.stop.remote() for w in self.remote_workers()]
            ray.get(tids)
        except Exception:
            logger.exception("Failed to stop workers!")
        finally:
            for w in self.remote_workers():
                w.__ray_terminate__.remote()

    @DeveloperAPI
    def is_policy_to_train(
        self, policy_id: PolicyID, batch: Optional[SampleBatchType] = None
    ) -> bool:
        """Whether given PolicyID (optionally inside some batch) is trainable."""
        local_worker = self.local_worker()
        if local_worker:
            return local_worker.is_policy_to_train(policy_id, batch)
        else:
            raise NotImplementedError

    @DeveloperAPI
    def foreach_worker(self, func: Callable[[RolloutWorker], T]) -> List[T]:
        """Calls the given function with each worker instance as arg.

        Args:
            func: The function to call for each worker (as only arg).

        Returns:
             The list of return values of all calls to `func([worker])`.
        """
        local_result = []
        if self.local_worker() is not None:
            local_result = [func(self.local_worker())]
        remote_results = ray.get([w.apply.remote(func) for w in self.remote_workers()])
        return local_result + remote_results

    @DeveloperAPI
    def foreach_worker_with_index(
        self, func: Callable[[RolloutWorker, int], T]
    ) -> List[T]:
        """Calls `func` with each worker instance and worker idx as args.

        The index will be passed as the second arg to the given function.

        Args:
            func: The function to call for each worker and its index
                (as args). The local worker has index 0, all remote workers
                have indices > 0.

        Returns:
             The list of return values of all calls to `func([worker, idx])`.
                The first entry in this list are the results of the local
                worker, followed by all remote workers' results.
        """
        local_result = []
        # Local worker: Index=0.
        if self.local_worker() is not None:
            local_result = [func(self.local_worker(), 0)]
        # Remote workers: Index > 0.
        remote_results = ray.get(
            [w.apply.remote(func, i + 1) for i, w in enumerate(self.remote_workers())]
        )
        return local_result + remote_results

    @DeveloperAPI
    def foreach_policy(self, func: Callable[[Policy, PolicyID], T]) -> List[T]:
        """Calls `func` with each worker's (policy, PolicyID) tuple.

        Note that in the multi-agent case, each worker may have more than one
        policy.

        Args:
            func: A function - taking a Policy and its ID - that is
                called on all workers' Policies.

        Returns:
            The list of return values of func over all workers' policies. The
                length of this list is:
                (num_workers + 1 (local-worker)) *
                [num policies in the multi-agent config dict].
                The local workers' results are first, followed by all remote
                workers' results
        """
        results = []
        if self.local_worker() is not None:
            results = self.local_worker().foreach_policy(func)
        ray_gets = []
        for worker in self.remote_workers():
            ray_gets.append(worker.apply.remote(lambda w: w.foreach_policy(func)))
        remote_results = ray.get(ray_gets)
        for r in remote_results:
            results.extend(r)
        return results

    @DeveloperAPI
    def foreach_policy_to_train(self, func: Callable[[Policy, PolicyID], T]) -> List[T]:
        """Apply `func` to all workers' Policies iff in `policies_to_train`.

        Args:
            func: A function - taking a Policy and its ID - that is
                called on all workers' Policies, for which
                `worker.is_policy_to_train()` returns True.

        Returns:
            List[any]: The list of n return values of all
                `func([trainable policy], [ID])`-calls.
        """
        results = []
        if self.local_worker() is not None:
            results = self.local_worker().foreach_policy_to_train(func)
        ray_gets = []
        for worker in self.remote_workers():
            ray_gets.append(
                worker.apply.remote(lambda w: w.foreach_policy_to_train(func))
            )
        remote_results = ray.get(ray_gets)
        for r in remote_results:
            results.extend(r)
        return results

    @DeveloperAPI
    def foreach_env(self, func: Callable[[EnvType], List[T]]) -> List[List[T]]:
        """Calls `func` with all workers' sub-environments as args.

        An "underlying sub environment" is a single clone of an env within
        a vectorized environment.
        `func` takes a single underlying sub environment as arg, e.g. a
        gym.Env object.

        Args:
            func: A function - taking an EnvType (normally a gym.Env object)
                as arg and returning a list of lists of return values, one
                value per underlying sub-environment per each worker.

        Returns:
            The list (workers) of lists (sub environments) of results.
        """
        local_results = []
        if self.local_worker() is not None:
            local_results = [self.local_worker().foreach_env(func)]
        ray_gets = []
        for worker in self.remote_workers():
            ray_gets.append(worker.foreach_env.remote(func))
        return local_results + ray.get(ray_gets)

    @DeveloperAPI
    def foreach_env_with_context(
        self, func: Callable[[BaseEnv, EnvContext], List[T]]
    ) -> List[List[T]]:
        """Calls `func` with all workers' sub-environments and env_ctx as args.

        An "underlying sub environment" is a single clone of an env within
        a vectorized environment.
        `func` takes a single underlying sub environment and the env_context
        as args.

        Args:
            func: A function - taking a BaseEnv object and an EnvContext as
                arg - and returning a list of lists of return values over envs
                of the worker.

        Returns:
            The list (1 item per workers) of lists (1 item per sub-environment)
                of results.
        """
        local_results = []
        if self.local_worker() is not None:
            local_results = [self.local_worker().foreach_env_with_context(func)]
        ray_gets = []
        for worker in self.remote_workers():
            ray_gets.append(worker.foreach_env_with_context.remote(func))
        return local_results + ray.get(ray_gets)

    @staticmethod
    def _from_existing(
        local_worker: RolloutWorker, remote_workers: List[ActorHandle] = None
    ):
        workers = WorkerSet(
            env_creator=None, policy_class=None, trainer_config={}, _setup=False
        )
        workers._local_worker = local_worker
        workers._remote_workers = remote_workers or []
        return workers

    def _make_worker(
        self,
        *,
        cls: Callable,
        env_creator: EnvCreator,
        validate_env: Optional[Callable[[EnvType], None]],
        policy_cls: Type[Policy],
        policy_specs: Optional[Dict[str, PolicySpec]] = None,
        worker_index: int,
        num_workers: int,
        recreated_worker: bool = False,
        config: AlgorithmConfigDict,
        spaces: Optional[
            Dict[PolicyID, Tuple[gym.spaces.Space, gym.spaces.Space]]
        ] = None,
    ) -> Union[RolloutWorker, ActorHandle]:
        def session_creator():
            logger.debug("Creating TF session {}".format(config["tf_session_args"]))
            return tf1.Session(config=tf1.ConfigProto(**config["tf_session_args"]))

        def valid_module(class_path):
            if (
                isinstance(class_path, str)
                and not os.path.isfile(class_path)
                and "." in class_path
            ):
                module_path, class_name = class_path.rsplit(".", 1)
                try:
                    spec = importlib.util.find_spec(module_path)
                    if spec is not None:
                        return True
                except (ModuleNotFoundError, ValueError):
                    print(
                        f"module {module_path} not found while trying to get "
                        f"input {class_path}"
                    )
            return False

        # A callable returning an InputReader object to use.
        if isinstance(config["input"], FunctionType):
            input_creator = config["input"]
        # Use RLlib's Sampler classes (SyncSampler or AsynchSampler, depending
        # on `config.sample_async` setting).
        elif config["input"] == "sampler":
            input_creator = lambda ioctx: ioctx.default_sampler_input()
        # Ray Dataset input -> Use `config.input_config` to construct DatasetReader.
        elif config["input"] == "dataset":
            # Input dataset shards should have already been prepared.
            # We just need to take the proper shard here.
            input_creator = lambda ioctx: DatasetReader(
                self._ds_shards[worker_index], ioctx
            )
        # Dict: Mix of different input methods with different ratios.
        elif isinstance(config["input"], dict):
            input_creator = lambda ioctx: ShuffledInput(
                MixedInput(config["input"], ioctx), config["shuffle_buffer_size"]
            )
        # A pre-registered input descriptor (str).
        elif isinstance(config["input"], str) and registry_contains_input(
            config["input"]
        ):
            input_creator = registry_get_input(config["input"])
        # D4RL input.
        elif "d4rl" in config["input"]:
            env_name = config["input"].split(".")[-1]
            input_creator = lambda ioctx: D4RLReader(env_name, ioctx)
        # Valid python module (class path) -> Create using `from_config`.
        elif valid_module(config["input"]):
            input_creator = lambda ioctx: ShuffledInput(
                from_config(config["input"], ioctx=ioctx)
            )
        # JSON file or list of JSON files -> Use JsonReader (shuffled).
        else:
            input_creator = lambda ioctx: ShuffledInput(
                JsonReader(config["input"], ioctx), config["shuffle_buffer_size"]
            )

        if isinstance(config["output"], FunctionType):
            output_creator = config["output"]
        elif config["output"] is None:
            output_creator = lambda ioctx: NoopOutput()
        elif config["output"] == "dataset":
            output_creator = lambda ioctx: DatasetWriter(
                ioctx, compress_columns=config["output_compress_columns"]
            )
        elif config["output"] == "logdir":
            output_creator = lambda ioctx: JsonWriter(
                ioctx.log_dir,
                ioctx,
                max_file_size=config["output_max_file_size"],
                compress_columns=config["output_compress_columns"],
            )
        else:
            output_creator = lambda ioctx: JsonWriter(
                config["output"],
                ioctx,
                max_file_size=config["output_max_file_size"],
                compress_columns=config["output_compress_columns"],
            )

        if not policy_specs:
            # Infer policy specs from multiagent.policies dict.
            if config["multiagent"]["policies"]:
                # Make a copy so we don't modify the original multiagent config dict
                # by accident.
                policy_specs = config["multiagent"]["policies"].copy()
                # Assert everything is correct in "multiagent" config dict (if given).
                for policy_spec in policy_specs.values():
                    assert isinstance(policy_spec, PolicySpec)
                    # Class is None -> Use `policy_cls`.
                    if policy_spec.policy_class is None:
                        policy_spec.policy_class = policy_cls
            # Use the only policy class as policy specs.
            else:
                policy_specs = policy_cls

        if worker_index == 0:
            extra_python_environs = config.get("extra_python_environs_for_driver", None)
        else:
            extra_python_environs = config.get("extra_python_environs_for_worker", None)

        worker = cls(
            env_creator=env_creator,
            validate_env=validate_env,
            policy_spec=policy_specs,
            policy_mapping_fn=config["multiagent"]["policy_mapping_fn"],
            policies_to_train=config["multiagent"]["policies_to_train"],
            tf_session_creator=(session_creator if config["tf_session_args"] else None),
            rollout_fragment_length=config["rollout_fragment_length"],
            count_steps_by=config["multiagent"]["count_steps_by"],
            batch_mode=config["batch_mode"],
            episode_horizon=config["horizon"],
            preprocessor_pref=config["preprocessor_pref"],
            sample_async=config["sample_async"],
            compress_observations=config["compress_observations"],
            num_envs=config["num_envs_per_worker"],
            observation_fn=config["multiagent"]["observation_fn"],
            clip_rewards=config["clip_rewards"],
            normalize_actions=config["normalize_actions"],
            clip_actions=config["clip_actions"],
            env_config=config["env_config"],
            policy_config=config,
            worker_index=worker_index,
            num_workers=num_workers,
            recreated_worker=recreated_worker,
            log_dir=self._logdir,
            log_level=config["log_level"],
            callbacks=config["callbacks"],
            input_creator=input_creator,
            output_creator=output_creator,
            remote_worker_envs=config["remote_worker_envs"],
            remote_env_batch_wait_ms=config["remote_env_batch_wait_ms"],
            soft_horizon=config["soft_horizon"],
            no_done_at_end=config["no_done_at_end"],
            seed=(config["seed"] + worker_index)
            if config["seed"] is not None
            else None,
            fake_sampler=config["fake_sampler"],
            extra_python_environs=extra_python_environs,
            spaces=spaces,
            disable_env_checking=config["disable_env_checking"],
        )

        return worker

    def _worker_health_check(self) -> List[int]:
        """Performs a health-check on each remote worker.

        Returns:
            List of indices (into `self._remote_workers` list) of faulty workers.
            Note that index=1 is the 0th item in `self._remote_workers`.
        """
        logger.info("Health checking all workers ...")
        checks = []
        for worker in self.remote_workers():
            # TODO: Maybe find a better way to probe for healthiness. Performing an
            #  entire `sample()` step may be costly. Then again, we only do this
            #  upon any worker failure during the `step_attempt()`, not regularly.
            _, obj_ref = worker.sample_with_count.remote()
            checks.append(obj_ref)

        faulty_worker_indices = []
        for i, obj_ref in enumerate(checks):
            try:
                ray.get(obj_ref)
                logger.info("Worker {} looks healthy.".format(i + 1))
            except RayError:
                logger.exception("Worker {} is faulty.".format(i + 1))
                faulty_worker_indices.append(i + 1)

        return faulty_worker_indices

    @classmethod
    def _valid_module(cls, class_path):
        del cls
        if (
            isinstance(class_path, str)
            and not os.path.isfile(class_path)
            and "." in class_path
        ):
            module_path, class_name = class_path.rsplit(".", 1)
            try:
                spec = importlib.util.find_spec(module_path)
                if spec is not None:
                    return True
            except (ModuleNotFoundError, ValueError):
                print(
                    f"module {module_path} not found while trying to get "
                    f"input {class_path}"
                )
        return False

    @Deprecated(new="WorkerSet.foreach_policy_to_train", error=True)
    def foreach_trainable_policy(self, func):
        return self.foreach_policy_to_train(func)

    @Deprecated(new="WorkerSet.is_policy_to_train([pid], [batch]?)", error=True)
    def trainable_policies(self):
        local_worker = self.local_worker()
        if local_worker is not None:
            return [
                pid
                for pid in local_worker.policy_map.keys()
                if local_worker.is_policy_to_train(pid, None)
            ]
        else:
            raise NotImplementedError
