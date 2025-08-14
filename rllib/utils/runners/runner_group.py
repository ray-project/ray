import abc
import logging
import ray

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    TypeVar,
    Union,
)

from ray.actor import ActorHandle
from ray.exceptions import RayActorError
from ray.rllib.core import (
    COMPONENT_ENV_TO_MODULE_CONNECTOR,
    COMPONENT_LEARNER,
    COMPONENT_MODULE_TO_ENV_CONNECTOR,
    COMPONENT_RL_MODULE,
)
from ray.rllib.core.learner.learner_group import LearnerGroup
from ray.rllib.utils.actor_manager import FaultTolerantActorManager
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME, WEIGHTS_SEQ_NO
from ray.rllib.utils.runners.runner import Runner
from ray.rllib.utils.typing import PolicyID
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

logger = logging.getLogger(__name__)

# Generic type var for `foreach_*` methods.
T = TypeVar("T")


@DeveloperAPI
class RunnerGroup(metaclass=abc.ABCMeta):
    def __init__(
        self,
        config: "AlgorithmConfig",
        # TODO (simon): Check, if this is needed. Derived classes could define
        # this if needed.
        # default_policy_class: Optional[Type[Policy]]
        local_runner: Optional[bool] = False,
        logdir: Optional[str] = None,
        # TODO (simon): Check, if still needed.
        tune_trial_id: Optional[str] = None,
        pg_offset: int = 0,
        _setup: bool = True,
        **kwargs: Dict[str, Any],
    ) -> None:

        # TODO (simon): Remove when old stack is deprecated.
        self.config: AlgorithmConfig = (
            AlgorithmConfig.from_dict(config)
            if isinstance(config, dict)
            else (config or AlgorithmConfig())
        )

        self._remote_config = config
        self._remote_config_obj_ref = ray.put(self._remote_config)

        self._tune_trial_id = tune_trial_id
        self._pg_offset = pg_offset

        self._logdir = logdir

        self._worker_manager = FaultTolerantActorManager(
            max_remote_requests_in_flight_per_actor=self._max_requests_in_flight_per_runner,
            init_id=1,
        )

        if _setup:
            try:
                self._setup(
                    config=config,
                    num_runners=self.num_runners,
                    local_runner=local_runner,
                    **kwargs,
                )
            # `RunnerGroup` creation possibly fails, if some (remote) workers cannot
            # be initialized properly (due to some errors in the `Runners`'s
            # constructor).
            except RayActorError as e:
                # In case of an actor (remote worker) init failure, the remote worker
                # may still exist and will be accessible, however, e.g. calling
                # its `run.remote()` would result in strange "property not found"
                # errors.
                if e.actor_init_failed:
                    # Raise the original error here that the `Runners` raised
                    # during its construction process. This is to enforce transparency
                    # for the user (better to understand the real reason behind the
                    # failure).
                    # - e.args[0]: The `RayTaskError` (inside the caught `RayActorError`).
                    # - e.args[0].args[2]: The original `Exception` (e.g. a `ValueError` due
                    # to a config mismatch) thrown inside the actor.
                    raise e.args[0].args[2]
                # In any other case, raise the `RayActorError` as-is.
                else:
                    raise e

    def _setup(
        self,
        *,
        config: Optional["AlgorithmConfig"] = None,
        num_runners: int = 0,
        local_runner: Optional[bool] = False,
        validate: Optional[bool] = None,
        **kwargs: Dict[str, Any],
    ) -> None:

        # TODO (simon): Deprecate this as soon as we are deprecating the old stack.
        self._local_runner = None
        if num_runners == 0:
            local_runner = True

        self.__local_config = config

        # Create a number of @ray.remote workers.
        self.add_runners(
            num_runners,
            validate=validate
            if validate is not None
            else self._validate_runners_after_construction,
            **kwargs,
        )

        if local_runner:
            self._local_runner = self._make_runner(
                runner_index=0,
                num_runners=num_runners,
                config=self._local_config,
                **kwargs,
            )

    def add_runners(self, num_runners: int, validate: bool = False, **kwargs) -> None:
        """Creates and adds a number of remote runners to this runner set."""

        old_num_runners = self._worker_manager.num_actors()
        new_runners = [
            self._make_runner(
                runner_index=old_num_runners + i + 1,
                num_runners=old_num_runners + num_runners,
                # `self._remote_config` can be large and it's best practice to
                # pass it by reference instead of value
                # (https://docs.ray.io/en/latest/ray-core/patterns/pass-large-arg-by-value.html) # noqa
                config=self._remote_config_obj_ref,
                **kwargs,
            )
            for i in range(num_runners)
        ]
        # Add the new workers to the worker manager.
        self._worker_manager.add_actors(new_runners)

        # Validate here, whether all remote workers have been constructed properly
        # and are "up and running". Establish initial states.
        if validate:
            self.validate()

    def validate(self) -> Exception:
        for result in self._worker_manager.foreach_actor(lambda w: w.assert_healthy()):
            # Simiply raise the error, which will get handled by the try-except
            # clause around the _setup().
            if not result.ok:
                e = result.get()
                if self._ignore_ray_errors_on_runners:
                    logger.error(
                        f"Validation of {self.runner_cls.__name__} failed! Error={str(e)}"
                    )
                else:
                    raise e

    def _make_runner(
        self,
        *,
        runner_index: int,
        num_runners: int,
        recreated_runner: bool = False,
        config: "AlgorithmConfig",
        **kwargs,
    ) -> ActorHandle:
        # TODO (simon): Change this in the `EnvRunner` API
        # to `runner_*`.
        kwargs = dict(
            config=config,
            worker_index=runner_index,
            num_workers=num_runners,
            recreated_worker=recreated_runner,
            log_dir=self._logdir,
            tune_trial_id=self._tune_trial_id,
            **kwargs,
        )

        # If a local runner is requested just return a runner instance.
        if runner_index == 0:
            return self.runner_cls(**kwargs)

        # Otherwise define a bundle index and schedule the remote worker.
        pg_bundle_idx = (
            -1
            if ray.util.get_current_placement_group() is None
            else self._pg_offset + runner_index
        )
        return (
            ray.remote(**self._remote_args)(self.runner_cls)
            .options(placement_group_bundle_index=pg_bundle_idx)
            .remote(**kwargs)
        )

    def sync_runner_states(
        self,
        *,
        config: "AlgorithmConfig",
        from_runner: Optional[Runner] = None,
        env_steps_sampled: Optional[int] = None,
        connector_states: Optional[List[Dict[str, Any]]] = None,
        rl_module_state: Optional[Dict[str, Any]] = None,
        runner_indices_to_update: Optional[List[int]] = None,
        env_to_module=None,
        module_to_env=None,
        **kwargs,
    ):
        """Synchronizes the connectors of this `RunnerGroup`'s `Runner`s."""

        # If no `Runner` is passed in synchronize through the local `Runner`.
        from_runner = from_runner or self.local_runner

        merge = config.merge_runner_states or (
            config.merge_runner_states == "training_only" and config.in_evaluation
        )

        broadcast = config.broadcast_runner_states

        # Early out if the number of (healthy) remote workers is 0. In this case, the
        # local worker is the only operating worker and thus of course always holds
        # the reference connector state.
        if self.num_healthy_remote_runners == 0 and self.local_runner:
            self.local_runner.set_state(
                {
                    **(
                        {NUM_ENV_STEPS_SAMPLED_LIFETIME: env_steps_sampled}
                        if env_steps_sampled is not None
                        else {}
                    ),
                    **(rl_module_state or {}),
                }
            )

        # Also early out, if we don't merge AND don't broadcast.
        if not merge and not broadcast:
            return

        # Use states from all remote `Runner`s.
        if merge:
            if connector_states == []:
                runner_states = {}
            else:
                if connector_states is None:
                    connector_states = self.foreach_runner(
                        lambda w: w.get_state(
                            components=[
                                COMPONENT_ENV_TO_MODULE_CONNECTOR,
                                COMPONENT_MODULE_TO_ENV_CONNECTOR,
                            ]
                        ),
                        local_runner=False,
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
                    self.local_runner is not None
                    and hasattr(self.local_runner, "_env_to_module")
                    and hasattr(self.local_runner, "_module_to_env")
                ):
                    assert env_to_module is None
                    env_to_module = self.local_runner._env_to_module
                    assert module_to_env is None
                    module_to_env = self.local_runner._module_to_env

                runner_states = {}
                if env_to_module_states:
                    runner_states.update(
                        {
                            COMPONENT_ENV_TO_MODULE_CONNECTOR: (
                                env_to_module.merge_states(env_to_module_states)
                            ),
                        }
                    )
                if module_to_env_states:
                    runner_states.update(
                        {
                            COMPONENT_MODULE_TO_ENV_CONNECTOR: (
                                module_to_env.merge_states(module_to_env_states)
                            ),
                        }
                    )
        # Ignore states from remote `Runner`s (use the current `from_worker` states
        # only).
        else:
            if from_runner is None:
                runner_states = {
                    COMPONENT_ENV_TO_MODULE_CONNECTOR: env_to_module.get_state(),
                    COMPONENT_MODULE_TO_ENV_CONNECTOR: module_to_env.get_state(),
                }
            else:
                runner_states = from_runner.get_state(
                    components=[
                        COMPONENT_ENV_TO_MODULE_CONNECTOR,
                        COMPONENT_MODULE_TO_ENV_CONNECTOR,
                    ]
                )

        # Update the global number of environment steps, if necessary.
        # Make sure to divide by the number of env runners (such that each `Runner`
        # knows (roughly) its own(!) lifetime count and can infer the global lifetime
        # count from it).
        if env_steps_sampled is not None:
            runner_states[NUM_ENV_STEPS_SAMPLED_LIFETIME] = env_steps_sampled // (
                config.num_runners or 1
            )

        # If we do NOT want remote `Runner`s to get their Connector states updated,
        # only update the local worker here (with all state components, except the model
        # weights) and then remove the connector components.
        if not broadcast:
            if self.local_runner is not None:
                self.local_runner.set_state(runner_states)
            else:
                env_to_module.set_state(
                    runner_states.get(COMPONENT_ENV_TO_MODULE_CONNECTOR), {}
                )
                module_to_env.set_state(
                    runner_states.get(COMPONENT_MODULE_TO_ENV_CONNECTOR), {}
                )
            runner_states.pop(COMPONENT_ENV_TO_MODULE_CONNECTOR, None)
            runner_states.pop(COMPONENT_MODULE_TO_ENV_CONNECTOR, None)

        # If there are components in the state left -> Update remote workers with these
        # state components (and maybe the local worker, if it hasn't been updated yet).
        if runner_states:
            # Update the local `Runner`, but NOT with the weights. If used at all for
            # evaluation (through the user calling `self.evaluate`), RLlib would update
            # the weights up front either way.
            if self.local_runner is not None and broadcast:
                self.local_runner.set_state(runner_states)

            # Send the model weights only to remote `Runner`s.
            # In case the local `Runner` is ever needed for evaluation,
            # RLlib updates its weight right before such an eval step.
            if rl_module_state:
                runner_states.update(rl_module_state)

            # Broadcast updated states back to all workers.
            self.foreach_runner(
                "set_state",  # Call the `set_state()` remote method.
                kwargs=dict(state=runner_states),
                remote_worker_ids=runner_indices_to_update,
                local_runner=False,
                timeout_seconds=0.0,  # This is a state update -> Fire-and-forget.
            )

    def sync_weights(
        self,
        policies: Optional[List[PolicyID]] = None,
        from_worker_or_learner_group: Optional[Union[Runner, "LearnerGroup"]] = None,
        to_worker_indices: Optional[List[int]] = None,
        timeout_seconds: Optional[float] = 0.0,
        inference_only: Optional[bool] = False,
        **kwargs,
    ) -> None:
        """Syncs model weights from the given weight source to all remote workers.

        Weight source can be either a (local) rollout worker or a learner_group. It
        should just implement a `get_weights` method.

        Args:
            policies: Optional list of PolicyIDs to sync weights for.
                If None (default), sync weights to/from all policies.
            from_worker_or_learner_group: Optional (local) `Runner` instance or
                LearnerGroup instance to sync from. If None (default),
                sync from this `Runner`Group's local worker.
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
        if self.local_runner is None and from_worker_or_learner_group is None:
            raise TypeError(
                "No `local_runner` in `RunnerGroup`! Must provide "
                "`from_worker_or_learner_group` arg in `sync_weights()`!"
            )

        # Only sync if we have remote workers or `from_worker_or_trainer` is provided.
        rl_module_state = None
        if self.num_remote_runners or from_worker_or_learner_group is not None:
            weights_src = (
                from_worker_or_learner_group
                if from_worker_or_learner_group is not None
                else self.local_runner
            )

            if weights_src is None:
                raise ValueError(
                    "`from_worker_or_trainer` is None. In this case, `RunnerGroup`^ "
                    "should have `local_runner`. But `local_runner` is also `None`."
                )

            modules = (
                [COMPONENT_RL_MODULE + "/" + p for p in policies]
                if policies is not None
                else [COMPONENT_RL_MODULE]
            )
            # LearnerGroup has-a Learner, which has-a RLModule.
            if isinstance(weights_src, LearnerGroup):
                rl_module_state = weights_src.get_state(
                    components=[COMPONENT_LEARNER + "/" + m for m in modules],
                    inference_only=inference_only,
                )[COMPONENT_LEARNER]
            # `Runner` (new API stack).
            else:
                # Runner (remote) has a RLModule.
                # TODO (sven): Replace this with a new ActorManager API:
                #  try_remote_request_till_success("get_state") -> tuple(int,
                #  remoteresult)
                #  `weights_src` could be the ActorManager, then. Then RLlib would know
                #   that it has to ping the manager to try all healthy actors until the
                #   first returns something.
                if isinstance(weights_src, ActorHandle):
                    rl_module_state = ray.get(
                        weights_src.get_state.remote(
                            components=modules,
                            inference_only=inference_only,
                        )
                    )
                # `Runner` (local) has an RLModule.
                else:
                    rl_module_state = weights_src.get_state(
                        components=modules,
                        inference_only=inference_only,
                    )

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

            # Sync to specified remote workers in this `Runner`Group.
            self.foreach_runner(
                func="set_state",
                kwargs=dict(state=rl_module_state_ref),
                local_runner=False,  # Do not sync back to local worker.
                remote_worker_ids=to_worker_indices,
                timeout_seconds=timeout_seconds,
            )

        # If `from_worker_or_learner_group` is provided, also sync to this
        # `RunnerGroup`'s local worker.
        if self.local_runner is not None:
            if from_worker_or_learner_group is not None:
                self.local_runner.set_state(rl_module_state)

    def reset(self, new_remote_runners: List[ActorHandle]) -> None:
        """Hard overrides the remote `Runner`s in this set with the provided ones.

        Args:
            new_remote_workers: A list of new `Runner`s (as `ActorHandles`) to use as
                new remote workers.
        """
        self._worker_manager.clear()
        self._worker_manager.add_actors(new_remote_runners)

    def stop(self) -> None:
        """Calls `stop` on all `Runner`s (including the local one)."""
        try:
            # Make sure we stop all `Runner`s, include the ones that were just
            # restarted / recovered or that are tagged unhealthy (at least, we should
            # try).
            self.foreach_runner(
                lambda w: w.stop(), healthy_only=False, local_runner=True
            )
        except Exception:
            logger.exception("Failed to stop workers!")
        finally:
            self._worker_manager.clear()

    def foreach_runner(
        self,
        func: Union[Callable[[Runner], T], List[Callable[[Runner], T]], str, List[str]],
        *,
        kwargs=None,
        local_runner: bool = True,
        healthy_only: bool = True,
        remote_worker_ids: List[int] = None,
        timeout_seconds: Optional[float] = None,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> List[T]:
        """Calls the given function with each `Runner` as its argument.

        Args:
            func: The function to call for each `Runner`s. The only call argument is
                the respective `Runner` instance.
            local_env_runner: Whether to apply `func` to local `Runner`, too.
                Default is True.
            healthy_only: Apply `func` on known-to-be healthy `Runner`s only.
            remote_worker_ids: Apply `func` on a selected set of remote `Runner`s.
                Use None (default) for all remote `Runner`s.
            timeout_seconds: Time to wait (in seconds) for results. Set this to 0.0 for
                fire-and-forget. Set this to None (default) to wait infinitely (i.e. for
                synchronous execution).
            return_obj_refs: Whether to return `ObjectRef` instead of actual results.
                Note, for fault tolerance reasons, these returned ObjectRefs should
                never be resolved with ray.get() outside of this `RunnerGroup`.
            mark_healthy: Whether to mark all those `Runner`s healthy again that are
                currently marked unhealthy AND that returned results from the remote
                call (within the given `timeout_seconds`).
                Note that `Runner`s are NOT set unhealthy, if they simply time out
                (only if they return a `RayActorError`).
                Also note that this setting is ignored if `healthy_only=True` (b/c
                `mark_healthy` only affects `Runner`s that are currently tagged as
                unhealthy).

        Returns:
             The list of return values of all calls to `func([worker])`.
        """
        assert (
            not return_obj_refs or not local_runner
        ), "Can not return `ObjectRef` from local worker."

        local_result = []
        if local_runner and self.local_runner is not None:
            if kwargs:
                local_kwargs = kwargs[0]
                kwargs = kwargs[1:]
            else:
                local_kwargs = {}
                kwargs = kwargs
            if isinstance(func, str):
                local_result = [getattr(self.local_runner, func)(**local_kwargs)]
            else:
                local_result = [func(self.local_runner, **local_kwargs)]

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
            remote_results, ignore_ray_errors=self._ignore_ray_errors_on_runners
        )

        # With application errors handled, return good results.
        remote_results = [r.get() for r in remote_results.ignore_errors()]

        return local_result + remote_results

    def foreach_runner_async(
        self,
        func: Union[Callable[[Runner], T], List[Callable[[Runner], T]], str, List[str]],
        *,
        healthy_only: bool = True,
        remote_worker_ids: List[int] = None,
    ) -> int:
        """Calls the given function asynchronously with each `Runner` as the argument.

        Does not return results directly. Instead, `fetch_ready_async_reqs()` can be
        used to pull results in an async manner whenever they are available.

        Args:
            func: The function to call for each `Runner`s. The only call argument is
                the respective `Runner` instance.
            healthy_only: Apply `func` on known-to-be healthy `Runner`s only.
            remote_worker_ids: Apply `func` on a selected set of remote `Runner`s.

        Returns:
             The number of async requests that have actually been made. This is the
             length of `remote_worker_ids` (or self.num_remote_workers()` if
             `remote_worker_ids` is None) minus the number of requests that were NOT
             made b/c a remote `Runner` already had its
             `max_remote_requests_in_flight_per_actor` counter reached.
        """
        return self._worker_manager.foreach_actor_async(
            func,
            healthy_only=healthy_only,
            remote_actor_ids=remote_worker_ids,
        )

    def fetch_ready_async_reqs(
        self,
        *,
        timeout_seconds: Optional[float] = 0.0,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> List[Tuple[int, T]]:
        """Get esults from outstanding asynchronous requests that are ready.

        Args:
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
        remote_results = self._worker_manager.fetch_ready_async_reqs(
            timeout_seconds=timeout_seconds,
            return_obj_refs=return_obj_refs,
            mark_healthy=mark_healthy,
        )

        FaultTolerantActorManager.handle_remote_call_result_errors(
            remote_results,
            ignore_ray_errors=self._ignore_ray_errors_on_runners,
        )

        return [(r.actor_id, r.get()) for r in remote_results.ignore_errors()]

    def probe_unhealthy_runners(self) -> List[int]:
        """Checks for unhealthy workers and tries restoring their states.

        Returns:
            List of IDs of the workers that were restored.
        """
        return self._worker_manager.probe_unhealthy_actors(
            timeout_seconds=self.runner_health_probe_timeout_s,
            mark_healthy=True,
        )

    @property
    @abc.abstractmethod
    def runner_health_probe_timeout_s(self):
        """Number of seconds to wait for health probe calls to `Runner`s."""

    @property
    @abc.abstractmethod
    def runner_cls(self) -> Callable:
        """Class for each runner."""

    @property
    def _local_config(self) -> "AlgorithmConfig":
        """Returns the config for a local `Runner`."""
        return self.__local_config

    @property
    def local_runner(self) -> Runner:
        """Returns the local `Runner`."""
        return self._local_runner

    @property
    def healthy_runner_ids(self) -> List[int]:
        """Returns the list of remote `Runner` IDs."""
        return self._worker_manager.healthy_actor_ids()

    @property
    @abc.abstractmethod
    def num_runners(self) -> int:
        """Number of runners to schedule and manage."""

    @property
    def num_remote_runners(self) -> int:
        """Number of remote `Runner`s."""
        return self._worker_manager.num_actors()

    @property
    def num_healthy_remote_runners(self) -> int:
        """Returns the number of healthy remote `Runner`s."""
        return self._worker_manager.num_healthy_actors()

    @property
    def num_healthy_runners(self) -> int:
        """Returns the number of healthy `Runner`s."""
        return int(bool(self._local_runner)) + self.num_healthy_remote_runners()

    @property
    def num_in_flight_async_reqs(self) -> int:
        """Returns the number of in-flight async requests."""
        return self._worker_manager.num_outstanding_async_reqs()

    @property
    def num_remote_runner_restarts(self) -> int:
        """Returns the number of times managed remote `Runner`s have been restarted."""
        return self._worker_manager.total_num_restarts()

    @property
    @abc.abstractmethod
    def _remote_args(self):
        """Remote arguments for each runner."""

    @property
    @abc.abstractmethod
    def _ignore_ray_errors_on_runners(self):
        """If errors in runners should be ignored."""

    @property
    @abc.abstractmethod
    def _max_requests_in_flight_per_runner(self):
        """Maximum requests in flight per runner."""

    @property
    @abc.abstractmethod
    def _validate_runners_after_construction(self):
        """If runners should validated after constructed."""
