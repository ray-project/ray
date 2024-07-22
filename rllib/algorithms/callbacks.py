import gc
import os
import platform
import tracemalloc
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Type, Union

import gymnasium as gym
import numpy as np

from ray.air.constants import TRAINING_ITERATION
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.env_context import EnvContext
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.episode_v2 import EpisodeV2
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import (
    OldAPIStack,
    override,
    OverrideToImplementCustomLogic,
    PublicAPI,
)
from ray.rllib.utils.exploration.random_encoder import (
    _MovingMeanStd,
    compute_states_entropy,
    update_beta,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.typing import AgentID, EnvType, EpisodeType, PolicyID
from ray.tune.callback import _CallbackMeta

# Import psutil after ray so the packaged version is used.
import psutil

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm import Algorithm
    from ray.rllib.env.env_runner import EnvRunner
    from ray.rllib.env.env_runner_group import EnvRunnerGroup


@PublicAPI
class DefaultCallbacks(metaclass=_CallbackMeta):
    """Abstract base class for RLlib callbacks (similar to Keras callbacks).

    These callbacks can be used for custom metrics and custom postprocessing.

    By default, all of these callbacks are no-ops. To configure custom training
    callbacks, subclass DefaultCallbacks and then set
    {"callbacks": YourCallbacksClass} in the algo config.
    """

    @OverrideToImplementCustomLogic
    def on_algorithm_init(
        self,
        *,
        algorithm: "Algorithm",
        metrics_logger: Optional[MetricsLogger] = None,
        **kwargs,
    ) -> None:
        """Callback run when a new Algorithm instance has finished setup.

        This method gets called at the end of Algorithm.setup() after all
        the initialization is done, and before actually training starts.

        Args:
            algorithm: Reference to the Algorithm instance.
            metrics_logger: The MetricsLogger object inside the `Algorithm`. Can be
                used to log custom metrics after algo initialization.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_workers_recreated(
        self,
        *,
        algorithm: "Algorithm",
        worker_set: "EnvRunnerGroup",
        worker_ids: List[int],
        is_evaluation: bool,
        **kwargs,
    ) -> None:
        """Callback run after one or more workers have been recreated.

        You can access (and change) the worker(s) in question via the following code
        snippet inside your custom override of this method:

        Note that any "worker" inside the algorithm's `self.env_runner_group` and
        `self.eval_env_runner_group` are instances of a subclass of EnvRunner.

        .. testcode::
            from ray.rllib.algorithms.callbacks import DefaultCallbacks

            class MyCallbacks(DefaultCallbacks):
                def on_workers_recreated(
                    self,
                    *,
                    algorithm,
                    worker_set,
                    worker_ids,
                    is_evaluation,
                    **kwargs,
                ):
                    # Define what you would like to do on the recreated
                    # workers:
                    def func(w):
                        # Here, we just set some arbitrary property to 1.
                        if is_evaluation:
                            w._custom_property_for_evaluation = 1
                        else:
                            w._custom_property_for_training = 1

                    # Use the `foreach_workers` method of the worker set and
                    # only loop through those worker IDs that have been restarted.
                    # Note that we set `local_worker=False` to NOT include it (local
                    # workers are never recreated; if they fail, the entire Algorithm
                    # fails).
                    worker_set.foreach_worker(
                        func,
                        remote_worker_ids=worker_ids,
                        local_worker=False,
                    )

        Args:
            algorithm: Reference to the Algorithm instance.
            worker_set: The EnvRunnerGroup object in which the workers in question
                reside. You can use a `worker_set.foreach_worker(remote_worker_ids=...,
                local_worker=False)` method call to execute custom
                code on the recreated (remote) workers. Note that the local worker is
                never recreated as a failure of this would also crash the Algorithm.
            worker_ids: The list of (remote) worker IDs that have been recreated.
            is_evaluation: Whether `worker_set` is the evaluation EnvRunnerGroup
                (located in `Algorithm.eval_env_runner_group`) or not.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_checkpoint_loaded(
        self,
        *,
        algorithm: "Algorithm",
        **kwargs,
    ) -> None:
        """Callback run when an Algorithm has loaded a new state from a checkpoint.

        This method gets called at the end of `Algorithm.load_checkpoint()`.

        Args:
            algorithm: Reference to the Algorithm instance.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OldAPIStack
    def on_create_policy(self, *, policy_id: PolicyID, policy: Policy) -> None:
        """Callback run whenever a new policy is added to an algorithm.

        Args:
            policy_id: ID of the newly created policy.
            policy: The policy just created.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_environment_created(
        self,
        *,
        env_runner: "EnvRunner",
        metrics_logger: Optional[MetricsLogger] = None,
        env: gym.Env,
        env_context: EnvContext,
        **kwargs,
    ) -> None:
        """Callback run when a new environment object has been created.

        Note: This only applies to the new API stack. The env used is usually a
        gym.Env (or more specifically a gym.vector.Env).

        Args:
            env_runner: Reference to the current EnvRunner instance.
            metrics_logger: The MetricsLogger object inside the `env_runner`. Can be
                used to log custom metrics after environment creation.
            env: The environment object that has been created on `env_runner`. This is
                usually a gym.Env (or a gym.vector.Env) object.
            env_context: The `EnvContext` object that has been passed to the
                `gym.make()` call as kwargs (and to the gym.Env as `config`). It should
                have all the config key/value pairs in it as well as the
                EnvContext-typical properties: `worker_index`, `num_workers`, and
                `remote`.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OldAPIStack
    def on_sub_environment_created(
        self,
        *,
        worker: "EnvRunner",
        sub_environment: EnvType,
        env_context: EnvContext,
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Callback run when a new sub-environment has been created.

        This method gets called after each sub-environment (usually a
        gym.Env) has been created, validated (RLlib built-in validation
        + possible custom validation function implemented by overriding
        `Algorithm.validate_env()`), wrapped (e.g. video-wrapper), and seeded.

        Args:
            worker: Reference to the current rollout worker.
            sub_environment: The sub-environment instance that has been
                created. This is usually a gym.Env object.
            env_context: The `EnvContext` object that has been passed to
                the env's constructor.
            env_index: The index of the sub-environment that has been created
                (within the vector of sub-environments of the BaseEnv).
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_episode_created(
        self,
        *,
        # TODO (sven): Deprecate Episode/EpisodeV2 with new API stack.
        episode: Union[EpisodeType, Episode, EpisodeV2],
        # TODO (sven): Deprecate this arg new API stack (in favor of `env_runner`).
        worker: Optional["EnvRunner"] = None,
        env_runner: Optional["EnvRunner"] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        # TODO (sven): Deprecate this arg new API stack (in favor of `env`).
        base_env: Optional[BaseEnv] = None,
        env: Optional[gym.Env] = None,
        # TODO (sven): Deprecate this arg new API stack (in favor of `rl_module`).
        policies: Optional[Dict[PolicyID, Policy]] = None,
        rl_module: Optional[RLModule] = None,
        env_index: int,
        **kwargs,
    ) -> None:
        """Callback run when a new episode is created (but has not started yet!).

        This method gets called after a new Episode(V2) (old stack) or
        MultiAgentEpisode instance has been created.
        This happens before the respective sub-environment's (usually a gym.Env)
        `reset()` is called by RLlib.

        Note, at the moment this callback does not get called in the new API stack
        and single-agent mode.

        1) Episode(V2)/MultiAgentEpisode created: This callback is called.
        2) Respective sub-environment (gym.Env) is `reset()`.
        3) Callback `on_episode_start` is called.
        4) Stepping through sub-environment/episode commences.

        Args:
            episode: The newly created episode. On the new API stack, this will be a
                MultiAgentEpisode object. On the old API stack, this will be a
                Episode or EpisodeV2 object.
                This is the episode that is about to be started with an upcoming
                `env.reset()`. Only after this reset call, the `on_episode_start`
                callback will be called.
            env_runner: Replaces `worker` arg. Reference to the current EnvRunner.
            metrics_logger: The MetricsLogger object inside the `env_runner`. Can be
                used to log custom metrics after Episode creation.
            env: Replaces `base_env` arg.  The gym.Env (new API stack) or RLlib
                BaseEnv (old API stack) running the episode. On the old stack, the
                underlying sub environment objects can be retrieved by calling
                `base_env.get_sub_environments()`.
            rl_module: Replaces `policies` arg. Either the RLModule (new API stack) or a
                dict mapping policy IDs to policy objects (old stack). In single agent
                mode there will only be a single policy/RLModule under the
                `rl_module["default_policy"]` key.
            env_index: The index of the sub-environment that is about to be reset
                (within the vector of sub-environments of the BaseEnv).
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_episode_start(
        self,
        *,
        episode: Union[EpisodeType, Episode, EpisodeV2],
        env_runner: Optional["EnvRunner"] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        env: Optional[gym.Env] = None,
        env_index: int,
        rl_module: Optional[RLModule] = None,
        # TODO (sven): Deprecate these args.
        worker: Optional["EnvRunner"] = None,
        base_env: Optional[BaseEnv] = None,
        policies: Optional[Dict[PolicyID, Policy]] = None,
        **kwargs,
    ) -> None:
        """Callback run right after an Episode has been started.

        This method gets called after a SingleAgentEpisode or MultiAgentEpisode instance
        has been reset with a call to `env.reset()` by the EnvRunner.

        1) Single-/MultiAgentEpisode created: `on_episode_created()` is called.
        2) Respective sub-environment (gym.Env) is `reset()`.
        3) Single-/MultiAgentEpisode starts: This callback is called.
        4) Stepping through sub-environment/episode commences.

        Args:
            episode: The just started (after `env.reset()`) SingleAgentEpisode or
                MultiAgentEpisode object.
            env_runner: Reference to the EnvRunner running the env and episode.
            metrics_logger: The MetricsLogger object inside the `env_runner`. Can be
                used to log custom metrics during env/episode stepping.
            env: The gym.Env or gym.vector.Env object running the started episode.
            env_index: The index of the sub-environment that is about to be reset
                (within the vector of sub-environments of the BaseEnv).
            rl_module: The RLModule used to compute actions for stepping the env.
                In a single-agent setup, this is a (single-agent) RLModule, in a multi-
                agent setup, this will be a MultiAgentRLModule.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_episode_step(
        self,
        *,
        episode: Union[EpisodeType, Episode, EpisodeV2],
        env_runner: Optional["EnvRunner"] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        env: Optional[gym.Env] = None,
        env_index: int,
        rl_module: Optional[RLModule] = None,
        # TODO (sven): Deprecate these args.
        worker: Optional["EnvRunner"] = None,
        base_env: Optional[BaseEnv] = None,
        policies: Optional[Dict[PolicyID, Policy]] = None,
        **kwargs,
    ) -> None:
        """Called on each episode step (after the action(s) has/have been logged).

        Note that on the new API stack, this callback is also called after the final
        step of an episode, meaning when terminated/truncated are returned as True
        from the `env.step()` call, but is still provided with the non-finalized
        episode object (meaning the data has NOT been converted to numpy arrays yet).

        The exact time of the call of this callback is after `env.step([action])` and
        also after the results of this step (observation, reward, terminated, truncated,
        infos) have been logged to the given `episode` object.

        Args:
            episode: The just stepped SingleAgentEpisode or MultiAgentEpisode object
                (after `env.step()` and after returned obs, rewards, etc.. have been
                logged to the episode object).
            env_runner: Reference to the EnvRunner running the env and episode.
            metrics_logger: The MetricsLogger object inside the `env_runner`. Can be
                used to log custom metrics during env/episode stepping.
            env: The gym.Env or gym.vector.Env object running the started episode.
            env_index: The index of the sub-environment that has just been stepped.
            rl_module: The RLModule used to compute actions for stepping the env.
                In a single-agent setup, this is a (single-agent) RLModule, in a multi-
                agent setup, this will be a MultiAgentRLModule.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_episode_end(
        self,
        *,
        episode: Union[EpisodeType, Episode, EpisodeV2],
        env_runner: Optional["EnvRunner"] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        env: Optional[gym.Env] = None,
        env_index: int,
        rl_module: Optional[RLModule] = None,
        # TODO (sven): Deprecate these args.
        worker: Optional["EnvRunner"] = None,
        base_env: Optional[BaseEnv] = None,
        policies: Optional[Dict[PolicyID, Policy]] = None,
        **kwargs,
    ) -> None:
        """Called when an episode is done (after terminated/truncated have been logged).

        The exact time of the call of this callback is after `env.step([action])` and
        also after the results of this step (observation, reward, terminated, truncated,
        infos) have been logged to the given `episode` object, where either terminated
        or truncated were True:

        - The env is stepped: `final_obs, rewards, ... = env.step([action])`

        - The step results are logged `episode.add_env_step(final_obs, rewards)`

        - Callback `on_episode_step` is fired.

        - Another env-to-module connector call is made (even though we won't need any
          RLModule forward pass anymore). We make this additional call to ensure that in
          case users use the connector pipeline to process observations (and write them
          back into the episode), the episode object has all observations - even the
          terminal one - properly processed.

        - ---> This callback `on_episode_end()` is fired. <---

        - The episode is finalized (i.e. lists of obs/rewards/actions/etc.. are
          converted into numpy arrays).

        Args:
            episode: The terminated/truncated SingleAgent- or MultiAgentEpisode object
                (after `env.step()` that returned terminated=True OR truncated=True and
                after the returned obs, rewards, etc.. have been logged to the episode
                object). Note that this method is still called before(!) the episode
                object is finalized, meaning all its timestep data is still present in
                lists of individual timestep data.
            env_runner: Reference to the EnvRunner running the env and episode.
            metrics_logger: The MetricsLogger object inside the `env_runner`. Can be
                used to log custom metrics during env/episode stepping.
            env: The gym.Env or gym.vector.Env object running the started episode.
            env_index: The index of the sub-environment that has just been terminated
                or truncated.
            rl_module: The RLModule used to compute actions for stepping the env.
                In a single-agent setup, this is a (single-agent) RLModule, in a multi-
                agent setup, this will be a MultiAgentRLModule.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_evaluate_start(
        self,
        *,
        algorithm: "Algorithm",
        metrics_logger: Optional[MetricsLogger] = None,
        **kwargs,
    ) -> None:
        """Callback before evaluation starts.

        This method gets called at the beginning of Algorithm.evaluate().

        Args:
            algorithm: Reference to the algorithm instance.
            metrics_logger: The MetricsLogger object inside the `Algorithm`. Can be
                used to log custom metrics before running the next round of evaluation.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_evaluate_end(
        self,
        *,
        algorithm: "Algorithm",
        metrics_logger: Optional[MetricsLogger] = None,
        evaluation_metrics: dict,
        **kwargs,
    ) -> None:
        """Runs when the evaluation is done.

        Runs at the end of Algorithm.evaluate().

        Args:
            algorithm: Reference to the algorithm instance.
            metrics_logger: The MetricsLogger object inside the `Algorithm`. Can be
                used to log custom metrics after the most recent evaluation round.
            evaluation_metrics: Results dict to be returned from algorithm.evaluate().
                You can mutate this object to add additional metrics.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OldAPIStack
    def on_postprocess_trajectory(
        self,
        *,
        worker: "EnvRunner",
        episode: Episode,
        agent_id: AgentID,
        policy_id: PolicyID,
        policies: Dict[PolicyID, Policy],
        postprocessed_batch: SampleBatch,
        original_batches: Dict[AgentID, Tuple[Policy, SampleBatch]],
        **kwargs,
    ) -> None:
        """Called immediately after a policy's postprocess_fn is called.

        You can use this callback to do additional postprocessing for a policy,
        including looking at the trajectory data of other agents in multi-agent
        settings.

        Args:
            worker: Reference to the current rollout worker.
            episode: Episode object.
            agent_id: Id of the current agent.
            policy_id: Id of the current policy for the agent.
            policies: Dict mapping policy IDs to policy objects. In single
                agent mode there will only be a single "default_policy".
            postprocessed_batch: The postprocessed sample batch
                for this agent. You can mutate this object to apply your own
                trajectory postprocessing.
            original_batches: Dict mapping agent IDs to their unpostprocessed
                trajectory data. You should not mutate this object.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_sample_end(
        self,
        *,
        env_runner: Optional["EnvRunner"] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        samples: Union[SampleBatch, List[EpisodeType]],
        # TODO (sven): Deprecate these args.
        worker: Optional["EnvRunner"] = None,
        **kwargs,
    ) -> None:
        """Called at the end of `EnvRunner.sample()`.

        Args:
            env_runner: Reference to the current EnvRunner object.
            metrics_logger: The MetricsLogger object inside the `env_runner`. Can be
                used to log custom metrics during env/episode stepping.
            samples: Batch to be returned. You can mutate this
                object to modify the samples generated.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_learn_on_batch(
        self, *, policy: Policy, train_batch: SampleBatch, result: dict, **kwargs
    ) -> None:
        """Called at the beginning of Policy.learn_on_batch().

        Note: This is called before 0-padding via
        `pad_batch_to_sequences_of_same_size`.

        Also note, SampleBatch.INFOS column will not be available on
        train_batch within this callback if framework is tf1, due to
        the fact that tf1 static graph would mistake it as part of the
        input dict if present.
        It is available though, for tf2 and torch frameworks.

        Args:
            policy: Reference to the current Policy object.
            train_batch: SampleBatch to be trained on. You can
                mutate this object to modify the samples generated.
            result: A results dict to add custom metrics to.
            kwargs: Forward compatibility placeholder.
        """
        pass

    @OverrideToImplementCustomLogic
    def on_train_result(
        self,
        *,
        algorithm: "Algorithm",
        metrics_logger: Optional[MetricsLogger] = None,
        result: dict,
        **kwargs,
    ) -> None:
        """Called at the end of Algorithm.train().

        Args:
            algorithm: Current Algorithm instance.
            metrics_logger: The MetricsLogger object inside the Algorithm. Can be
                used to log custom metrics after traing results are available.
            result: Dict of results returned from Algorithm.train() call.
                You can mutate this object to add additional metrics.
            kwargs: Forward compatibility placeholder.
        """
        pass


class MemoryTrackingCallbacks(DefaultCallbacks):
    """MemoryTrackingCallbacks can be used to trace and track memory usage
    in rollout workers.

    The Memory Tracking Callbacks uses tracemalloc and psutil to track
    python allocations during rollouts,
    in training or evaluation.

    The tracking data is logged to the custom_metrics of an episode and
    can therefore be viewed in tensorboard
    (or in WandB etc..)

    Add MemoryTrackingCallbacks callback to the tune config
    e.g. { ...'callbacks': MemoryTrackingCallbacks ...}

    Note:
        This class is meant for debugging and should not be used
        in production code as tracemalloc incurs
        a significant slowdown in execution speed.
    """

    def __init__(self):
        super().__init__()

        # Will track the top 10 lines where memory is allocated
        tracemalloc.start(10)

    @override(DefaultCallbacks)
    def on_episode_end(
        self,
        *,
        episode: Union[EpisodeType, Episode, EpisodeV2],
        env_runner: Optional["EnvRunner"] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        env: Optional[gym.Env] = None,
        env_index: int,
        rl_module: Optional[RLModule] = None,
        # TODO (sven): Deprecate these args.
        worker: Optional["EnvRunner"] = None,
        base_env: Optional[BaseEnv] = None,
        policies: Optional[Dict[PolicyID, Policy]] = None,
        **kwargs,
    ) -> None:
        gc.collect()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")

        for stat in top_stats[:10]:
            count = stat.count
            # Convert total size from Bytes to KiB.
            size = stat.size / 1024

            trace = str(stat.traceback)

            episode.custom_metrics[f"tracemalloc/{trace}/size"] = size
            episode.custom_metrics[f"tracemalloc/{trace}/count"] = count

        process = psutil.Process(os.getpid())
        worker_rss = process.memory_info().rss
        worker_vms = process.memory_info().vms
        if platform.system() == "Linux":
            # This is only available on Linux
            worker_data = process.memory_info().data
            episode.custom_metrics["tracemalloc/worker/data"] = worker_data
        episode.custom_metrics["tracemalloc/worker/rss"] = worker_rss
        episode.custom_metrics["tracemalloc/worker/vms"] = worker_vms


def make_multi_callbacks(
    callback_class_list: List[Type[DefaultCallbacks]],
) -> DefaultCallbacks:
    """Allows combining multiple sub-callbacks into one new callbacks class.

    The resulting DefaultCallbacks will call all the sub-callbacks' callbacks
    when called.

    .. testcode::
        :skipif: True

        config.callbacks(make_multi_callbacks([
            MyCustomStatsCallbacks,
            MyCustomVideoCallbacks,
            MyCustomTraceCallbacks,
            ....
        ]))

    Args:
        callback_class_list: The list of sub-classes of DefaultCallbacks to
            be baked into the to-be-returned class. All of these sub-classes'
            implemented methods will be called in the given order.

    Returns:
        A DefaultCallbacks subclass that combines all the given sub-classes.
    """

    class _MultiCallbacks(DefaultCallbacks):
        IS_CALLBACK_CONTAINER = True

        def __init__(self):
            super().__init__()
            self._callback_list = [
                callback_class() for callback_class in callback_class_list
            ]

        @override(DefaultCallbacks)
        def on_algorithm_init(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_algorithm_init(**kwargs)

        @override(DefaultCallbacks)
        def on_workers_recreated(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_workers_recreated(**kwargs)

        @override(DefaultCallbacks)
        def on_checkpoint_loaded(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_checkpoint_loaded(**kwargs)

        @override(DefaultCallbacks)
        def on_create_policy(self, *, policy_id: PolicyID, policy: Policy) -> None:
            for callback in self._callback_list:
                callback.on_create_policy(policy_id=policy_id, policy=policy)

        @override(DefaultCallbacks)
        def on_environment_created(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_environment_created(**kwargs)

        @OldAPIStack
        @override(DefaultCallbacks)
        def on_sub_environment_created(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_sub_environment_created(**kwargs)

        @override(DefaultCallbacks)
        def on_episode_created(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_episode_created(**kwargs)

        @override(DefaultCallbacks)
        def on_episode_start(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_episode_start(**kwargs)

        @override(DefaultCallbacks)
        def on_episode_step(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_episode_step(**kwargs)

        @override(DefaultCallbacks)
        def on_episode_end(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_episode_end(**kwargs)

        @override(DefaultCallbacks)
        def on_evaluate_start(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_evaluate_start(**kwargs)

        @override(DefaultCallbacks)
        def on_evaluate_end(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_evaluate_end(**kwargs)

        @OldAPIStack
        @override(DefaultCallbacks)
        def on_postprocess_trajectory(
            self,
            *,
            worker: "EnvRunner",
            episode: Episode,
            agent_id: AgentID,
            policy_id: PolicyID,
            policies: Dict[PolicyID, Policy],
            postprocessed_batch: SampleBatch,
            original_batches: Dict[AgentID, Tuple[Policy, SampleBatch]],
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_postprocess_trajectory(
                    worker=worker,
                    episode=episode,
                    agent_id=agent_id,
                    policy_id=policy_id,
                    policies=policies,
                    postprocessed_batch=postprocessed_batch,
                    original_batches=original_batches,
                    **kwargs,
                )

        @override(DefaultCallbacks)
        def on_sample_end(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_sample_end(**kwargs)

        @OldAPIStack
        @override(DefaultCallbacks)
        def on_learn_on_batch(
            self, *, policy: Policy, train_batch: SampleBatch, result: dict, **kwargs
        ) -> None:
            for callback in self._callback_list:
                callback.on_learn_on_batch(
                    policy=policy, train_batch=train_batch, result=result, **kwargs
                )

        @override(DefaultCallbacks)
        def on_train_result(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_train_result(**kwargs)

    return _MultiCallbacks


# This Callback is used by the RE3 exploration strategy.
# See rllib/examples/re3_exploration.py for details.
class RE3UpdateCallbacks(DefaultCallbacks):
    """Update input callbacks to mutate batch with states entropy rewards."""

    _step = 0

    def __init__(
        self,
        *args,
        embeds_dim: int = 128,
        k_nn: int = 50,
        beta: float = 0.1,
        rho: float = 0.0001,
        beta_schedule: str = "constant",
        **kwargs,
    ):
        self.embeds_dim = embeds_dim
        self.k_nn = k_nn
        self.beta = beta
        self.rho = rho
        self.beta_schedule = beta_schedule
        self._rms = _MovingMeanStd()
        super().__init__(*args, **kwargs)

    @override(DefaultCallbacks)
    def on_learn_on_batch(
        self,
        *,
        policy: Policy,
        train_batch: SampleBatch,
        result: dict,
        **kwargs,
    ):
        super().on_learn_on_batch(
            policy=policy, train_batch=train_batch, result=result, **kwargs
        )
        states_entropy = compute_states_entropy(
            train_batch[SampleBatch.OBS_EMBEDS], self.embeds_dim, self.k_nn
        )
        states_entropy = update_beta(
            self.beta_schedule, self.beta, self.rho, RE3UpdateCallbacks._step
        ) * np.reshape(
            self._rms(states_entropy),
            train_batch[SampleBatch.OBS_EMBEDS].shape[:-1],
        )
        train_batch[SampleBatch.REWARDS] = (
            train_batch[SampleBatch.REWARDS] + states_entropy
        )
        if Postprocessing.ADVANTAGES in train_batch:
            train_batch[Postprocessing.ADVANTAGES] = (
                train_batch[Postprocessing.ADVANTAGES] + states_entropy
            )
            train_batch[Postprocessing.VALUE_TARGETS] = (
                train_batch[Postprocessing.VALUE_TARGETS] + states_entropy
            )

    @override(DefaultCallbacks)
    def on_train_result(self, *, result: dict, algorithm=None, **kwargs) -> None:
        # TODO(gjoliver): Remove explicit _step tracking and pass
        #  Algorithm._iteration as a parameter to on_learn_on_batch() call.
        RE3UpdateCallbacks._step = result[TRAINING_ITERATION]
        super().on_train_result(algorithm=algorithm, result=result, **kwargs)
