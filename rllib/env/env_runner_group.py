import importlib.util
import logging
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import gymnasium as gym

import ray
from ray._common.deprecation import (
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.actor import ActorHandle
from ray.exceptions import RayActorError
from ray.rllib.core import (
    COMPONENT_ENV_TO_MODULE_CONNECTOR,
    COMPONENT_LEARNER,
    COMPONENT_MODULE_TO_ENV_CONNECTOR,
    COMPONENT_RL_MODULE,
)
from ray.rllib.core.learner import LearnerGroup
from ray.rllib.core.rl_module import validate_module_id
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.offline import get_dataset_and_shards
from ray.rllib.policy.policy import Policy, PolicyState
from ray.rllib.utils.actor_manager import FaultTolerantActorManager
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME, WEIGHTS_SEQ_NO
from ray.rllib.utils.typing import (
    AgentID,
    EnvCreator,
    EnvType,
    EpisodeID,
    PartialAlgorithmConfigDict,
    PolicyID,
    SampleBatchType,
    TensorType,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)

# Generic type var for foreach_* methods.
T = TypeVar("T")


@DeveloperAPI
class EnvRunnerGroup:
    """Set of EnvRunners with n @ray.remote workers and zero or one local worker.

    Where: n >= 0.
    """

    def __init__(
        self,
        *,
        env_creator: Optional[EnvCreator] = None,
        validate_env: Optional[Callable[[EnvType], None]] = None,
        default_policy_class: Optional[Type[Policy]] = None,
        config: Optional["AlgorithmConfig"] = None,
        local_env_runner: bool = True,
        logdir: Optional[str] = None,
        _setup: bool = True,
        tune_trial_id: Optional[str] = None,
        pg_offset: int = 0,
        # Deprecated args.
        num_env_runners: Optional[int] = None,
        num_workers=DEPRECATED_VALUE,
        local_worker=DEPRECATED_VALUE,
    ):
        """Initializes a EnvRunnerGroup instance.

        Args:
            env_creator: Function that returns env given env config.
            validate_env: Optional callable to validate the generated
                environment (only on worker=0). This callable should raise
                an exception if the environment is invalid.
            default_policy_class: An optional default Policy class to use inside
                the (multi-agent) `policies` dict. In case the PolicySpecs in there
                have no class defined, use this `default_policy_class`.
                If None, PolicySpecs will be using the Algorithm's default Policy
                class.
            config: Optional AlgorithmConfig (or config dict).
            local_env_runner: Whether to create a local (non @ray.remote) EnvRunner
                in the returned set as well (default: True). If `num_env_runners`
                is 0, always create a local EnvRunner.
            logdir: Optional logging directory for workers.
            _setup: Whether to actually set up workers. This is only for testing.
            tune_trial_id: The Ray Tune trial ID, if this EnvRunnerGroup is part of
                an Algorithm run as a Tune trial. None, otherwise.
        """
        if num_workers != DEPRECATED_VALUE or local_worker != DEPRECATED_VALUE:
            deprecation_warning(
                old="WorkerSet(num_workers=..., local_worker=...)",
                new="EnvRunnerGroup(num_env_runners=..., local_env_runner=...)",
                error=True,
            )

        from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

        # Make sure `config` is an AlgorithmConfig object.
        if not config:
            config = AlgorithmConfig()
        elif isinstance(config, dict):
            config = AlgorithmConfig.from_dict(config)

        self._env_creator = env_creator
        self._policy_class = default_policy_class
        self._remote_config = config
        self._remote_config_obj_ref = ray.put(self._remote_config)
        self._remote_args = {
            "num_cpus": self._remote_config.num_cpus_per_env_runner,
            "num_gpus": self._remote_config.num_gpus_per_env_runner,
            "resources": self._remote_config.custom_resources_per_env_runner,
            "max_restarts": (
                config.max_num_env_runner_restarts
                if config.restart_failed_env_runners
                else 0
            ),
        }
        self._tune_trial_id = tune_trial_id
        self._pg_offset = pg_offset

        # Set the EnvRunner subclass to be used as "workers". Default: RolloutWorker.
        self.env_runner_cls = config.env_runner_cls
        if self.env_runner_cls is None:
            if config.enable_env_runner_and_connector_v2:
                # If experiences should be recorded, use the `
                # OfflineSingleAgentEnvRunner`.
                if config.output:
                    # No multi-agent support.
                    if config.is_multi_agent:
                        raise ValueError("Multi-agent recording is not supported, yet.")
                    # Otherwise, load the single-agent env runner for
                    # recording.
                    else:
                        from ray.rllib.offline.offline_env_runner import (
                            OfflineSingleAgentEnvRunner,
                        )

                        self.env_runner_cls = OfflineSingleAgentEnvRunner
                else:
                    if config.is_multi_agent:
                        from ray.rllib.env.multi_agent_env_runner import (
                            MultiAgentEnvRunner,
                        )

                        self.env_runner_cls = MultiAgentEnvRunner
                    else:
                        from ray.rllib.env.single_agent_env_runner import (
                            SingleAgentEnvRunner,
                        )

                        self.env_runner_cls = SingleAgentEnvRunner
            else:
                self.env_runner_cls = RolloutWorker

        self._logdir = logdir
        self._ignore_ray_errors_on_env_runners = (
            config.ignore_env_runner_failures or config.restart_failed_env_runners
        )

        # Create remote worker manager.
        # ID=0 is used by the local worker.
        # Starting remote workers from ID=1 to avoid conflicts.
        self._worker_manager = FaultTolerantActorManager(
            max_remote_requests_in_flight_per_actor=(
                config.max_requests_in_flight_per_env_runner
            ),
            init_id=1,
        )

        if _setup:
            try:
                self._setup(
                    validate_env=validate_env,
                    config=config,
                    num_env_runners=(
                        num_env_runners
                        if num_env_runners is not None
                        else config.num_env_runners
                    ),
                    local_env_runner=local_env_runner,
                )
            # EnvRunnerGroup creation possibly fails, if some (remote) workers cannot
            # be initialized properly (due to some errors in the EnvRunners's
            # constructor).
            except RayActorError as e:
                # In case of an actor (remote worker) init failure, the remote worker
                # may still exist and will be accessible, however, e.g. calling
                # its `sample.remote()` would result in strange "property not found"
                # errors.
                if e.actor_init_failed:
                    # Raise the original error here that the EnvRunners raised
                    # during its construction process. This is to enforce transparency
                    # for the user (better to understand the real reason behind the
                    # failure).
                    # - e.args[0]: The RayTaskError (inside the caught RayActorError).
                    # - e.args[0].args[2]: The original Exception (e.g. a ValueError due
                    # to a config mismatch) thrown inside the actor.
                    raise e.args[0].args[2]
                # In any other case, raise the RayActorError as-is.
                else:
                    raise e

    def _setup(
        self,
        *,
        validate_env: Optional[Callable[[EnvType], None]] = None,
        config: Optional["AlgorithmConfig"] = None,
        num_env_runners: int = 0,
        local_env_runner: bool = True,
    ):
        """Sets up an EnvRunnerGroup instance.
        Args:
            validate_env: Optional callable to validate the generated
                environment (only on worker=0).
            config: Optional dict that extends the common config of
                the Algorithm class.
            num_env_runners: Number of remote EnvRunner workers to create.
            local_env_runner: Whether to create a local (non @ray.remote) EnvRunner
                in the returned set as well (default: True). If `num_env_runners`
                is 0, always create a local EnvRunner.
        """
        # Force a local worker if num_env_runners == 0 (no remote workers).
        # Otherwise, this EnvRunnerGroup would be empty.
        self._local_env_runner = None
        if num_env_runners == 0:
            local_env_runner = True
        # Create a local (learner) version of the config for the local worker.
        # The only difference is the tf_session_args, which - for the local worker -
        # will be `config.tf_session_args` updated/overridden with
        # `config.local_tf_session_args`.
        local_tf_session_args = config.tf_session_args.copy()
        local_tf_session_args.update(config.local_tf_session_args)
        self._local_config = config.copy(copy_frozen=False).framework(
            tf_session_args=local_tf_session_args
        )

        if config.input_ == "dataset":
            # Create the set of dataset readers to be shared by all the
            # rollout workers.
            self._ds, self._ds_shards = get_dataset_and_shards(config, num_env_runners)
        else:
            self._ds = None
            self._ds_shards = None

        # Create a number of @ray.remote workers.
        self.add_workers(
            num_env_runners,
            validate=config.validate_env_runners_after_construction,
        )

        # If num_env_runners > 0 and we don't have an env on the local worker,
        # get the observation- and action spaces for each policy from
        # the first remote worker (which does have an env).
        if (
            local_env_runner
            and self._worker_manager.num_actors() > 0
            and not config.create_env_on_local_worker
            and (not config.observation_space or not config.action_space)
        ):
            spaces = self.get_spaces()
        else:
            spaces = None

        # Create a local worker, if needed.
        if local_env_runner:
            self._local_env_runner = self._make_worker(
                env_creator=self._env_creator,
                validate_env=validate_env,
                worker_index=0,
                num_workers=num_env_runners,
                config=self._local_config,
                spaces=spaces,
            )

    def get_spaces(self):
        """Infer observation and action spaces from one (local or remote) EnvRunner.

        Returns:
            A dict mapping from ModuleID to a 2-tuple containing obs- and action-space.
        """
        # Get ID of the first remote worker.
        remote_worker_ids = (
            [self._worker_manager.actor_ids()[0]]
            if self._worker_manager.actor_ids()
            else []
        )

        spaces = self.foreach_env_runner(
            lambda env_runner: env_runner.get_spaces(),
            remote_worker_ids=remote_worker_ids,
            local_env_runner=not remote_worker_ids,
        )[0]

        logger.info(
            "Inferred observation/action spaces from remote "
            f"worker (local worker has no env): {spaces}"
        )

        return spaces

    @property
    def local_env_runner(self) -> EnvRunner:
        """Returns the local EnvRunner."""
        return self._local_env_runner

    def healthy_env_runner_ids(self) -> List[int]:
        """Returns the list of remote worker IDs."""
        return self._worker_manager.healthy_actor_ids()

    def healthy_worker_ids(self) -> List[int]:
        """Returns the list of remote worker IDs."""
        return self.healthy_env_runner_ids()

    def num_remote_env_runners(self) -> int:
        """Returns the number of remote EnvRunners."""
        return self._worker_manager.num_actors()

    def num_remote_workers(self) -> int:
        """Returns the number of remote EnvRunners."""
        return self.num_remote_env_runners()

    def num_healthy_remote_env_runners(self) -> int:
        """Returns the number of healthy remote workers."""
        return self._worker_manager.num_healthy_actors()

    def num_healthy_remote_workers(self) -> int:
        """Returns the number of healthy remote workers."""
        return self.num_healthy_remote_env_runners()

    def num_healthy_env_runners(self) -> int:
        """Returns the number of all healthy workers, including the local worker."""
        return int(bool(self._local_env_runner)) + self.num_healthy_remote_workers()

    def num_healthy_workers(self) -> int:
        """Returns the number of all healthy workers, including the local worker."""
        return self.num_healthy_env_runners()

    def num_in_flight_async_reqs(self, tag: Optional[str] = None) -> int:
        """Returns the number of in-flight async requests."""
        return self._worker_manager.num_outstanding_async_reqs(tag=tag)

    def num_remote_worker_restarts(self) -> int:
        """Total number of times managed remote workers have been restarted."""
        return self._worker_manager.total_num_restarts()

    def sync_env_runner_states(
        self,
        *,
        config: "AlgorithmConfig",
        from_worker: Optional[EnvRunner] = None,
        env_steps_sampled: Optional[int] = None,
        connector_states: Optional[List[Dict[str, Any]]] = None,
        rl_module_state: Optional[Dict[str, Any]] = None,
        env_runner_indices_to_update: Optional[List[int]] = None,
        env_to_module=None,
        module_to_env=None,
    ) -> None:
        """Synchronizes the connectors of this EnvRunnerGroup's EnvRunners.

        The exact procedure works as follows:
        - If `from_worker` is None, set `from_worker=self.local_env_runner`.
        - If `config.use_worker_filter_stats` is True, gather all remote EnvRunners'
        ConnectorV2 states. Otherwise, only use the ConnectorV2 states of `from_worker`.
        - Merge all gathered states into one resulting state.
        - Broadcast the resulting state back to all remote EnvRunners AND the local
        EnvRunner.

        Args:
            config: The AlgorithmConfig object to use to determine, in which
                direction(s) we need to synch and what the timeouts are.
            from_worker: The EnvRunner from which to synch. If None, will use the local
                worker of this EnvRunnerGroup.
            env_steps_sampled: The total number of env steps taken thus far by all
                workers combined. Used to broadcast this number to all remote workers
                if `update_worker_filter_stats` is True in `config`.
            env_runner_indices_to_update: The indices of those EnvRunners to update
                with the merged state. Use None (default) to update all remote
                EnvRunners.
        """
        if env_steps_sampled is not None:
            env_steps_sampled = int(env_steps_sampled)
        from_worker = from_worker or self.local_env_runner

        merge = (
            not config.enable_env_runner_and_connector_v2
            and config.use_worker_filter_stats
        ) or (
            config.enable_env_runner_and_connector_v2
            and (
                config.merge_env_runner_states is True
                or (
                    config.merge_env_runner_states == "training_only"
                    and not config.in_evaluation
                )
            )
        )
        broadcast = (
            not config.enable_env_runner_and_connector_v2
            and config.update_worker_filter_stats
        ) or (
            config.enable_env_runner_and_connector_v2
            and config.broadcast_env_runner_states
        )

        # Early out if the number of (healthy) remote workers is 0. In this case, the
        # local worker is the only operating worker and thus of course always holds
        # the reference connector state.
        if self.num_healthy_remote_workers() == 0 and self.local_env_runner:
            self.local_env_runner.set_state(
                {
                    **(
                        {NUM_ENV_STEPS_SAMPLED_LIFETIME: env_steps_sampled}
                        if env_steps_sampled is not None
                        else {}
                    ),
                    **(rl_module_state or {}),
                }
            )
            return

        # Also early out, if we don't merge AND don't broadcast.
        if not merge and not broadcast:
            return

        # Use states from all remote EnvRunners.
        if merge:
            if connector_states == []:
                env_runner_states = {}
            else:
                if connector_states is None:
                    connector_states = self.foreach_env_runner(
                        lambda w: w.get_state(
                            components=[
                                COMPONENT_ENV_TO_MODULE_CONNECTOR,
                                COMPONENT_MODULE_TO_ENV_CONNECTOR,
                            ]
                        ),
                        local_env_runner=False,
                        timeout_seconds=(
                            config.sync_filters_on_rollout_workers_timeout_s
                        ),
                    )
                env_to_module_states = [
                    s[COMPONENT_ENV_TO_MODULE_CONNECTOR]
                    for s in connector_states
                    if COMPONENT_ENV_TO_MODULE_CONNECTOR in s
                ]
                module_to_env_states = [
                    s[COMPONENT_MODULE_TO_ENV_CONNECTOR]
                    for s in connector_states
                    if COMPONENT_MODULE_TO_ENV_CONNECTOR in s
                ]

                if (
                    self.local_env_runner is not None
                    and hasattr(self.local_env_runner, "_env_to_module")
                    and hasattr(self.local_env_runner, "_module_to_env")
                ):
                    assert env_to_module is None
                    env_to_module = self.local_env_runner._env_to_module
                    assert module_to_env is None
                    module_to_env = self.local_env_runner._module_to_env

                env_runner_states = {}
                if env_to_module_states:
                    env_runner_states.update(
                        {
                            COMPONENT_ENV_TO_MODULE_CONNECTOR: (
                                env_to_module.merge_states(env_to_module_states)
                            ),
                        }
                    )
                if module_to_env_states:
                    env_runner_states.update(
                        {
                            COMPONENT_MODULE_TO_ENV_CONNECTOR: (
                                module_to_env.merge_states(module_to_env_states)
                            ),
                        }
                    )
        # Ignore states from remote EnvRunners (use the current `from_worker` states
        # only).
        else:
            if from_worker is None:
                env_runner_states = {
                    COMPONENT_ENV_TO_MODULE_CONNECTOR: env_to_module.get_state(),
                    COMPONENT_MODULE_TO_ENV_CONNECTOR: module_to_env.get_state(),
                }
            else:
                env_runner_states = from_worker.get_state(
                    components=[
                        COMPONENT_ENV_TO_MODULE_CONNECTOR,
                        COMPONENT_MODULE_TO_ENV_CONNECTOR,
                    ]
                )

        # Update the global number of environment steps, if necessary.
        if env_steps_sampled is not None:
            env_runner_states[NUM_ENV_STEPS_SAMPLED_LIFETIME] = env_steps_sampled

        # If we do NOT want remote EnvRunners to get their Connector states updated,
        # only update the local worker here (with all state components, except the model
        # weights) and then remove the connector components.
        if not broadcast:
            if self.local_env_runner is not None:
                self.local_env_runner.set_state(env_runner_states)
            else:
                env_to_module.set_state(
                    env_runner_states.get(COMPONENT_ENV_TO_MODULE_CONNECTOR), {}
                )
                module_to_env.set_state(
                    env_runner_states.get(COMPONENT_MODULE_TO_ENV_CONNECTOR), {}
                )
            env_runner_states.pop(COMPONENT_ENV_TO_MODULE_CONNECTOR, None)
            env_runner_states.pop(COMPONENT_MODULE_TO_ENV_CONNECTOR, None)

        # If there are components in the state left -> Update remote workers with these
        # state components (and maybe the local worker, if it hasn't been updated yet).
        if env_runner_states:
            # Update the local EnvRunner, but NOT with the weights. If used at all for
            # evaluation (through the user calling `self.evaluate`), RLlib would update
            # the weights up front either way.
            if self.local_env_runner is not None and broadcast:
                self.local_env_runner.set_state(env_runner_states)

            # Send the model weights only to remote EnvRunners.
            # In case the local EnvRunner is ever needed for evaluation,
            # RLlib updates its weight right before such an eval step.
            if rl_module_state:
                env_runner_states.update(rl_module_state)

            # Broadcast updated states back to all workers.
            # We explicitly don't want to fire and forget here, because this can lead to a lot of in-flight requests.
            # When these pile up, object store memory can spike.
            self.foreach_env_runner_async_fetch_ready(
                func="set_state",
                tag="set_state",
                kwargs=dict(state=env_runner_states),
                remote_worker_ids=env_runner_indices_to_update,
                timeout_seconds=0.0,
            )

    def foreach_env_runner_async_fetch_ready(
        self,
        func: Union[
            Callable[[EnvRunner], T], List[Callable[[EnvRunner], T]], str, List[str]
        ],
        kwargs: Optional[Dict[str, Any]] = None,
        tag: Optional[str] = None,
        timeout_seconds: Optional[float] = 0.0,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
        healthy_only: bool = True,
        remote_worker_ids: List[int] = None,
        return_actor_ids: bool = False,
    ) -> List[Union[Tuple[int, T], T]]:
        """Calls the given function asynchronously and returns previous results if any.

        This is a convenience function that calls the underlying actor manager's
        `foreach_actor_async_fetch_ready()` method.

        """
        return self._worker_manager.foreach_actor_async_fetch_ready(
            func=func,
            tag=tag,
            kwargs=kwargs,
            timeout_seconds=timeout_seconds,
            return_obj_refs=return_obj_refs,
            mark_healthy=mark_healthy,
            healthy_only=healthy_only,
            remote_actor_ids=remote_worker_ids,
            ignore_ray_errors=self._ignore_ray_errors_on_env_runners,
            return_actor_ids=return_actor_ids,
        )

    def sync_weights(
        self,
        policies: Optional[List[PolicyID]] = None,
        from_worker_or_learner_group: Optional[Union[EnvRunner, "LearnerGroup"]] = None,
        to_worker_indices: Optional[List[int]] = None,
        global_vars: Optional[Dict[str, TensorType]] = None,
        timeout_seconds: Optional[float] = 0.0,
        inference_only: Optional[bool] = False,
    ) -> None:
        """Syncs model weights from the given weight source to all remote workers.

        Weight source can be either a (local) rollout worker or a learner_group. It
        should just implement a `get_weights` method.

        Args:
            policies: Optional list of PolicyIDs to sync weights for.
                If None (default), sync weights to/from all policies.
            from_worker_or_learner_group: Optional (local) EnvRunner instance or
                LearnerGroup instance to sync from. If None (default),
                sync from this EnvRunnerGroup's local worker.
            to_worker_indices: Optional list of worker indices to sync the
                weights to. If None (default), sync to all remote workers.
            global_vars: An optional global vars dict to set this
                worker to. If None, do not update the global_vars.
            timeout_seconds: Timeout in seconds to wait for the sync weights
                calls to complete. Default is 0.0 (fire-and-forget, do not wait
                for any sync calls to finish). Setting this to 0.0 might significantly
                improve algorithm performance, depending on the algo's `training_step`
                logic.
            inference_only: Sync weights with workers that keep inference-only
                modules. This is needed for algorithms in the new stack that
                use inference-only modules. In this case only a part of the
                parameters are synced to the workers. Default is False.
        """
        if self.local_env_runner is None and from_worker_or_learner_group is None:
            raise TypeError(
                "No `local_env_runner` in EnvRunnerGroup! Must provide "
                "`from_worker_or_learner_group` arg in `sync_weights()`!"
            )

        # Only sync if we have remote workers or `from_worker_or_trainer` is provided.
        rl_module_state = None
        if self.num_remote_workers() or from_worker_or_learner_group is not None:
            weights_src = (
                from_worker_or_learner_group
                if from_worker_or_learner_group is not None
                else self.local_env_runner
            )

            if weights_src is None:
                raise ValueError(
                    "`from_worker_or_trainer` is None. In this case, EnvRunnerGroup "
                    "should have local_env_runner. But local_env_runner is also None."
                )

            modules = (
                [COMPONENT_RL_MODULE + "/" + p for p in policies]
                if policies is not None
                else [COMPONENT_RL_MODULE]
            )
            # LearnerGroup has a Learner, which has an RLModule.
            if isinstance(weights_src, LearnerGroup):
                rl_module_state = weights_src.get_state(
                    components=[COMPONENT_LEARNER + "/" + m for m in modules],
                    inference_only=inference_only,
                )[COMPONENT_LEARNER]
            # EnvRunner (new API stack).
            elif self._remote_config.enable_env_runner_and_connector_v2:
                # EnvRunner (remote) has an RLModule.
                # TODO (sven): Replace this with a new ActorManager API:
                #  try_remote_request_till_success("get_state") -> tuple(int,
                #  remoteresult)
                #  `weights_src` could be the ActorManager, then. Then RLlib would know
                #   that it has to ping the manager to try all healthy actors until the
                #   first returns something.
                if isinstance(weights_src, ray.actor.ActorHandle):
                    rl_module_state = ray.get(
                        weights_src.get_state.remote(
                            components=modules,
                            inference_only=inference_only,
                        )
                    )
                # EnvRunner (local) has an RLModule.
                else:
                    rl_module_state = weights_src.get_state(
                        components=modules,
                        inference_only=inference_only,
                    )
            # RolloutWorker (old API stack).
            else:
                rl_module_state = weights_src.get_weights(
                    policies=policies,
                    inference_only=inference_only,
                )

            if self._remote_config.enable_env_runner_and_connector_v2:

                # Make sure `rl_module_state` only contains the weights and the
                # weight seq no, nothing else.
                rl_module_state = {
                    k: v
                    for k, v in rl_module_state.items()
                    if k in [COMPONENT_RL_MODULE, WEIGHTS_SEQ_NO]
                }

                # Move weights to the object store to avoid having to make n pickled
                # copies of the weights dict for each worker.
                rl_module_state_ref = ray.put(rl_module_state)

                # Sync to specified remote workers in this EnvRunnerGroup.
                # We explicitly don't want to fire and forget here, because this can lead to a lot of in-flight requests.
                # When these pile up, object store memory can spike.
                self.foreach_env_runner_async_fetch_ready(
                    func="set_state",
                    tag="set_state",
                    kwargs=dict(state=rl_module_state_ref),
                    remote_worker_ids=to_worker_indices,
                    timeout_seconds=timeout_seconds,
                )

            else:
                rl_module_state_ref = ray.put(rl_module_state)

                def _set_weights(env_runner):
                    env_runner.set_weights(ray.get(rl_module_state_ref), global_vars)

                # Sync to specified remote workers in this EnvRunnerGroup.
                self.foreach_env_runner(
                    func=_set_weights,
                    local_env_runner=False,  # Do not sync back to local worker.
                    remote_worker_ids=to_worker_indices,
                    timeout_seconds=timeout_seconds,
                )

        # If `from_worker_or_learner_group` is provided, also sync to this
        # EnvRunnerGroup's local worker.
        if self.local_env_runner is not None:
            if from_worker_or_learner_group is not None:
                if self._remote_config.enable_env_runner_and_connector_v2:
                    self.local_env_runner.set_state(rl_module_state)
                else:
                    self.local_env_runner.set_weights(rl_module_state)
            # If `global_vars` is provided and local worker exists  -> Update its
            # global_vars.
            if global_vars is not None:
                self.local_env_runner.set_global_vars(global_vars)

    def add_workers(self, num_workers: int, validate: bool = False) -> None:
        """Creates and adds a number of remote workers to this worker set.

        Can be called several times on the same EnvRunnerGroup to add more
        EnvRunners to the set.

        Args:
            num_workers: The number of remote Workers to add to this
                EnvRunnerGroup.
            validate: Whether to validate remote workers after their construction
                process.

        Raises:
            RayError: If any of the constructed remote workers is not up and running
                properly.
        """
        old_num_workers = self._worker_manager.num_actors()
        new_workers = [
            self._make_worker(
                env_creator=self._env_creator,
                validate_env=None,
                worker_index=old_num_workers + i + 1,
                num_workers=old_num_workers + num_workers,
                # self._remote_config can be large
                # and it's best practice to pass it by reference
                # instead of value (https://docs.ray.io/en/latest/ray-core/patterns/pass-large-arg-by-value.html)
                config=self._remote_config_obj_ref,
            )
            for i in range(num_workers)
        ]
        self._worker_manager.add_actors(new_workers)

        # Validate here, whether all remote workers have been constructed properly
        # and are "up and running". Establish initial states.
        if validate:
            for result in self._worker_manager.foreach_actor(
                lambda w: w.assert_healthy()
            ):
                # Simiply raise the error, which will get handled by the try-except
                # clause around the _setup().
                if not result.ok:
                    e = result.get()
                    if self._ignore_ray_errors_on_env_runners:
                        logger.error(f"Validation of EnvRunner failed! Error={str(e)}")
                    else:
                        raise e

    def reset(self, new_remote_workers: List[ActorHandle]) -> None:
        """Hard overrides the remote EnvRunners in this set with the provided ones.

        Args:
            new_remote_workers: A list of new EnvRunners (as `ActorHandles`) to use as
                new remote workers.
        """
        self._worker_manager.clear()
        self._worker_manager.add_actors(new_remote_workers)

    def stop(self) -> None:
        """Calls `stop` on all EnvRunners (including the local one)."""
        try:
            # Make sure we stop all EnvRunners, include the ones that were just
            # restarted / recovered or that are tagged unhealthy (at least, we should
            # try).
            self.foreach_env_runner(
                lambda w: w.stop(), healthy_only=False, local_env_runner=True
            )
        except Exception:
            logger.exception("Failed to stop workers!")
        finally:
            self._worker_manager.clear()

    def foreach_env_runner(
        self,
        func: Union[
            Callable[[EnvRunner], T], List[Callable[[EnvRunner], T]], str, List[str]
        ],
        *,
        kwargs=None,
        local_env_runner: bool = True,
        healthy_only: bool = True,
        remote_worker_ids: List[int] = None,
        timeout_seconds: Optional[float] = None,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> List[T]:
        """Calls the given function with each EnvRunner as its argument.

        Args:
            func: The function to call for each EnvRunners. The only call argument is
                the respective EnvRunner instance.
            local_env_runner: Whether to apply `func` to local EnvRunner, too.
                Default is True.
            healthy_only: Apply `func` on known-to-be healthy EnvRunners only.
            remote_worker_ids: Apply `func` on a selected set of remote EnvRunners.
                Use None (default) for all remote EnvRunners.
            timeout_seconds: Time to wait (in seconds) for results. Set this to 0.0 for
                fire-and-forget. Set this to None (default) to wait infinitely (i.e. for
                synchronous execution).
            return_obj_refs: Whether to return ObjectRef instead of actual results.
                Note, for fault tolerance reasons, these returned ObjectRefs should
                never be resolved with ray.get() outside of this EnvRunnerGroup.
            mark_healthy: Whether to mark all those EnvRunners healthy again that are
                currently marked unhealthy AND that returned results from the remote
                call (within the given `timeout_seconds`).
                Note that EnvRunners are NOT set unhealthy, if they simply time out
                (only if they return a RayActorError).
                Also note that this setting is ignored if `healthy_only=True` (b/c
                `mark_healthy` only affects EnvRunners that are currently tagged as
                unhealthy).

        Returns:
             The list of return values of all calls to `func([worker])`.
        """
        assert (
            not return_obj_refs or not local_env_runner
        ), "Can not return ObjectRef from local worker."

        local_result = []
        if local_env_runner and self.local_env_runner is not None:
            assert kwargs is None
            if isinstance(func, str):
                local_result = [getattr(self.local_env_runner, func)]
            else:
                local_result = [func(self.local_env_runner)]

        if not self._worker_manager.actor_ids():
            return local_result

        remote_results = self._worker_manager.foreach_actor(
            func,
            kwargs=kwargs,
            healthy_only=healthy_only,
            remote_actor_ids=remote_worker_ids,
            timeout_seconds=timeout_seconds,
            return_obj_refs=return_obj_refs,
            mark_healthy=mark_healthy,
        )

        FaultTolerantActorManager.handle_remote_call_result_errors(
            remote_results, ignore_ray_errors=self._ignore_ray_errors_on_env_runners
        )

        # With application errors handled, return good results.
        remote_results = [r.get() for r in remote_results.ignore_errors()]

        return local_result + remote_results

    def foreach_env_runner_async(
        self,
        func: Union[
            Callable[[EnvRunner], T], List[Callable[[EnvRunner], T]], str, List[str]
        ],
        tag: Optional[str] = None,
        *,
        kwargs=None,
        healthy_only: bool = True,
        remote_worker_ids: List[int] = None,
    ) -> int:
        """Calls the given function asynchronously with each EnvRunner as the argument.

        Does not return results directly. Instead, `fetch_ready_async_reqs()` can be
        used to pull results in an async manner whenever they are available.

        Args:
            func: The function to call for each EnvRunners. The only call argument is
                the respective EnvRunner instance.
            tag: A tag to identify the results from this async call when fetching with
                `fetch_ready_async_reqs()`.
            kwargs: An optional kwargs dict to be passed to the remote function calls.
            healthy_only: Apply `func` on known-to-be healthy EnvRunners only.
            remote_worker_ids: Apply `func` on a selected set of remote EnvRunners.

        Returns:
             The number of async requests that have actually been made. This is the
             length of `remote_worker_ids` (or self.num_remote_workers()` if
             `remote_worker_ids` is None) minus the number of requests that were NOT
             made b/c a remote EnvRunner already had its
             `max_remote_requests_in_flight_per_actor` counter reached for this tag.
        """

        return self._worker_manager.foreach_actor_async(
            func,
            tag=tag,
            kwargs=kwargs,
            healthy_only=healthy_only,
            remote_actor_ids=remote_worker_ids,
        )

    def fetch_ready_async_reqs(
        self,
        *,
        tags: Optional[Union[str, List[str], Tuple[str]]] = None,
        timeout_seconds: Optional[float] = 0.0,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> List[Tuple[int, T]]:
        """Get results from outstanding asynchronous requests that are ready.

        Args:
            tags: Tags to identify the results from a specific async call.
                If None (default), returns results from all ready async requests.
                If a single string, returns results from all ready async requests with that tag.
            timeout_seconds: Time to wait for results. Default is 0, meaning
                those requests that are already ready.
            return_obj_refs: Whether to return ObjectRef instead of actual results.
            mark_healthy: Whether to mark all those workers healthy again that are
                currently marked unhealthy AND that returned results from the remote
                call (within the given `timeout_seconds`).
                Note that workers are NOT set unhealthy, if they simply time out
                (only if they return a RayActorError).
                Also note that this setting is ignored if `healthy_only=True` (b/c
                `mark_healthy` only affects workers that are currently tagged as
                unhealthy).

        Returns:
            A list of results successfully returned from outstanding remote calls,
            paired with the indices of the callee workers.
        """
        # Get remote results
        remote_results = self._worker_manager.fetch_ready_async_reqs(
            tags=tags,
            timeout_seconds=timeout_seconds,
            return_obj_refs=return_obj_refs,
            mark_healthy=mark_healthy,
        )

        FaultTolerantActorManager.handle_remote_call_result_errors(
            remote_results,
            ignore_ray_errors=self._ignore_ray_errors_on_env_runners,
        )

        return [(r.actor_id, r.get()) for r in remote_results.ignore_errors()]

    @OldAPIStack
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
        return list(
            self.foreach_env_runner(
                lambda w: w.foreach_env(func),
                local_env_runner=True,
            )
        )

    @OldAPIStack
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
        return list(
            self.foreach_env_runner(
                lambda w: w.foreach_env_with_context(func),
                local_env_runner=True,
            )
        )

    def probe_unhealthy_env_runners(self) -> List[int]:
        """Checks for unhealthy workers and tries restoring their states.

        Returns:
            List of IDs of the workers that were restored.
        """
        return self._worker_manager.probe_unhealthy_actors(
            timeout_seconds=self._remote_config.env_runner_health_probe_timeout_s,
            mark_healthy=True,
        )

    @OldAPIStack
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
        for r in self.foreach_env_runner(
            lambda w: w.foreach_policy(func), local_env_runner=True
        ):
            results.extend(r)
        return results

    @OldAPIStack
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
        for r in self.foreach_env_runner(
            lambda w: w.foreach_policy_to_train(func), local_env_runner=True
        ):
            results.extend(r)
        return results

    @OldAPIStack
    def is_policy_to_train(
        self, policy_id: PolicyID, batch: Optional[SampleBatchType] = None
    ) -> bool:
        """Whether given PolicyID (optionally inside some batch) is trainable."""
        if self.local_env_runner:
            if self.local_env_runner.is_policy_to_train is None:
                return True
            return self.local_env_runner.is_policy_to_train(policy_id, batch)
        else:
            raise NotImplementedError

    @OldAPIStack
    def add_policy(
        self,
        policy_id: PolicyID,
        policy_cls: Optional[Type[Policy]] = None,
        policy: Optional[Policy] = None,
        *,
        observation_space: Optional[gym.spaces.Space] = None,
        action_space: Optional[gym.spaces.Space] = None,
        config: Optional[Union["AlgorithmConfig", PartialAlgorithmConfigDict]] = None,
        policy_state: Optional[PolicyState] = None,
        policy_mapping_fn: Optional[Callable[[AgentID, EpisodeID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[
                Collection[PolicyID],
                Callable[[PolicyID, Optional[SampleBatchType]], bool],
            ]
        ] = None,
        module_spec: Optional[RLModuleSpec] = None,
        # Deprecated.
        workers: Optional[List[Union[EnvRunner, ActorHandle]]] = DEPRECATED_VALUE,
    ) -> None:
        """Adds a policy to this EnvRunnerGroup's workers or a specific list of workers.

        Args:
            policy_id: ID of the policy to add.
            policy_cls: The Policy class to use for constructing the new Policy.
                Note: Only one of `policy_cls` or `policy` must be provided.
            policy: The Policy instance to add to this EnvRunnerGroup. If not None, the
                given Policy object will be directly inserted into the
                local worker and clones of that Policy will be created on all remote
                workers.
                Note: Only one of `policy_cls` or `policy` must be provided.
            observation_space: The observation space of the policy to add.
                If None, try to infer this space from the environment.
            action_space: The action space of the policy to add.
                If None, try to infer this space from the environment.
            config: The config object or overrides for the policy to add.
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
            module_spec: In the new RLModule API we need to pass in the module_spec for
                the new module that is supposed to be added. Knowing the policy spec is
                not sufficient.
            workers: A list of EnvRunner/ActorHandles (remote
                EnvRunners) to add this policy to. If defined, will only
                add the given policy to these workers.

        Raises:
            KeyError: If the given `policy_id` already exists in this EnvRunnerGroup.
        """
        if self.local_env_runner and policy_id in self.local_env_runner.policy_map:
            raise KeyError(
                f"Policy ID '{policy_id}' already exists in policy map! "
                "Make sure you use a Policy ID that has not been taken yet."
                " Policy IDs that are already in your policy map: "
                f"{list(self.local_env_runner.policy_map.keys())}"
            )

        if workers is not DEPRECATED_VALUE:
            deprecation_warning(
                old="EnvRunnerGroup.add_policy(.., workers=..)",
                help=(
                    "The `workers` argument to `EnvRunnerGroup.add_policy()` is "
                    "deprecated! Please do not use it anymore."
                ),
                error=True,
            )

        if (policy_cls is None) == (policy is None):
            raise ValueError(
                "Only one of `policy_cls` or `policy` must be provided to "
                "staticmethod: `EnvRunnerGroup.add_policy()`!"
            )
        validate_module_id(policy_id, error=False)

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
                module_spec=module_spec,
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
                module_spec=module_spec,
            )

        def _create_new_policy_fn(worker):
            # `foreach_env_runner` function: Adds the policy to the worker (and
            # maybe changes its policy_mapping_fn - if provided here).
            worker.add_policy(**new_policy_instance_kwargs)

        if self.local_env_runner is not None:
            # Add policy directly by (already instantiated) object.
            if policy is not None:
                self.local_env_runner.add_policy(
                    policy_id=policy_id,
                    policy=policy,
                    policy_mapping_fn=policy_mapping_fn,
                    policies_to_train=policies_to_train,
                    module_spec=module_spec,
                )
            # Add policy by constructor kwargs.
            else:
                self.local_env_runner.add_policy(**new_policy_instance_kwargs)

        # Add the policy to all remote workers.
        self.foreach_env_runner(_create_new_policy_fn, local_env_runner=False)

    def _make_worker(
        self,
        *,
        env_creator: EnvCreator,
        validate_env: Optional[Callable[[EnvType], None]],
        worker_index: int,
        num_workers: int,
        recreated_worker: bool = False,
        config: "AlgorithmConfig",
        spaces: Optional[
            Dict[PolicyID, Tuple[gym.spaces.Space, gym.spaces.Space]]
        ] = None,
    ) -> Union[EnvRunner, ActorHandle]:
        kwargs = dict(
            env_creator=env_creator,
            validate_env=validate_env,
            default_policy_class=self._policy_class,
            config=config,
            worker_index=worker_index,
            num_workers=num_workers,
            recreated_worker=recreated_worker,
            log_dir=self._logdir,
            spaces=spaces,
            dataset_shards=self._ds_shards,
            tune_trial_id=self._tune_trial_id,
        )

        if worker_index == 0:
            return self.env_runner_cls(**kwargs)

        pg_bundle_idx = (
            -1
            if ray.util.get_current_placement_group() is None
            else self._pg_offset + worker_index
        )
        return (
            ray.remote(**self._remote_args)(self.env_runner_cls)
            .options(placement_group_bundle_index=pg_bundle_idx)
            .remote(**kwargs)
        )

    @staticmethod
    def _valid_module(class_path):
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
            except (ModuleNotFoundError, ValueError) as e:
                logger.warning(
                    f"module {module_path} not found using input {class_path} with error: {e}"
                )
        return False
