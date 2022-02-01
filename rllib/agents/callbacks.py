import numpy as np
import os
import tracemalloc
from typing import Dict, Optional, TYPE_CHECKING

from ray.rllib.env import BaseEnv
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.exploration.random_encoder import (
    MovingMeanStd,
    compute_states_entropy,
    update_beta,
)
from ray.rllib.utils.typing import AgentID, PolicyID

# Import psutil after ray so the packaged version is used.
import psutil

if TYPE_CHECKING:
    from ray.rllib.agents.trainer import Trainer
    from ray.rllib.evaluation import RolloutWorker


@PublicAPI
class DefaultCallbacks:
    """Abstract base class for RLlib callbacks (similar to Keras callbacks).

    These callbacks can be used for custom metrics and custom postprocessing.

    By default, all of these callbacks are no-ops. To configure custom training
    callbacks, subclass DefaultCallbacks and then set
    {"callbacks": YourCallbacksClass} in the trainer config.
    """

    def __init__(self, legacy_callbacks_dict: Dict[str, callable] = None):
        if legacy_callbacks_dict:
            deprecation_warning(
                "callbacks dict interface",
                "a class extending rllib.agents.callbacks.DefaultCallbacks",
            )
        self.legacy_callbacks = legacy_callbacks_dict or {}

    def on_episode_start(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[PolicyID, Policy],
        episode: Episode,
        **kwargs,
    ) -> None:
        """Callback run on the rollout worker before each episode starts.

        Args:
            worker: Reference to the current rollout worker.
            base_env: BaseEnv running the episode. The underlying
                sub environment objects can be retrieved by calling
                `base_env.get_sub_environments()`.
            policies: Mapping of policy id to policy objects. In single
                agent mode there will only be a single "default" policy.
            episode: Episode object which contains the episode's
                state. You can use the `episode.user_data` dict to store
                temporary data, and `episode.custom_metrics` to store custom
                metrics for the episode.
            kwargs: Forward compatibility placeholder.
        """

        if self.legacy_callbacks.get("on_episode_start"):
            self.legacy_callbacks["on_episode_start"](
                {
                    "env": base_env,
                    "policy": policies,
                    "episode": episode,
                }
            )

    def on_episode_step(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Optional[Dict[PolicyID, Policy]] = None,
        episode: Episode,
        **kwargs,
    ) -> None:
        """Runs on each episode step.

        Args:
            worker: Reference to the current rollout worker.
            base_env: BaseEnv running the episode. The underlying
                sub environment objects can be retrieved by calling
                `base_env.get_sub_environments()`.
            policies: Mapping of policy id to policy objects.
                In single agent mode there will only be a single
                "default_policy".
            episode: Episode object which contains episode
                state. You can use the `episode.user_data` dict to store
                temporary data, and `episode.custom_metrics` to store custom
                metrics for the episode.
            kwargs: Forward compatibility placeholder.
        """

        if self.legacy_callbacks.get("on_episode_step"):
            self.legacy_callbacks["on_episode_step"](
                {"env": base_env, "episode": episode}
            )

    def on_episode_end(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[PolicyID, Policy],
        episode: Episode,
        **kwargs,
    ) -> None:
        """Runs when an episode is done.

        Args:
            worker: Reference to the current rollout worker.
            base_env: BaseEnv running the episode. The underlying
                sub environment objects can be retrieved by calling
                `base_env.get_sub_environments()`.
            policies: Mapping of policy id to policy
                objects. In single agent mode there will only be a single
                "default_policy".
            episode: Episode object which contains episode
                state. You can use the `episode.user_data` dict to store
                temporary data, and `episode.custom_metrics` to store custom
                metrics for the episode.
            kwargs: Forward compatibility placeholder.
        """

        if self.legacy_callbacks.get("on_episode_end"):
            self.legacy_callbacks["on_episode_end"](
                {
                    "env": base_env,
                    "policy": policies,
                    "episode": episode,
                }
            )

    def on_postprocess_trajectory(
        self,
        *,
        worker: "RolloutWorker",
        episode: Episode,
        agent_id: AgentID,
        policy_id: PolicyID,
        policies: Dict[PolicyID, Policy],
        postprocessed_batch: SampleBatch,
        original_batches: Dict[AgentID, SampleBatch],
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
            policies: Mapping of policy id to policy objects. In single
                agent mode there will only be a single "default_policy".
            postprocessed_batch: The postprocessed sample batch
                for this agent. You can mutate this object to apply your own
                trajectory postprocessing.
            original_batches: Mapping of agents to their unpostprocessed
                trajectory data. You should not mutate this object.
            kwargs: Forward compatibility placeholder.
        """

        if self.legacy_callbacks.get("on_postprocess_traj"):
            self.legacy_callbacks["on_postprocess_traj"](
                {
                    "episode": episode,
                    "agent_id": agent_id,
                    "pre_batch": original_batches[agent_id],
                    "post_batch": postprocessed_batch,
                    "all_pre_batches": original_batches,
                }
            )

    def on_sample_end(
        self, *, worker: "RolloutWorker", samples: SampleBatch, **kwargs
    ) -> None:
        """Called at the end of RolloutWorker.sample().

        Args:
            worker: Reference to the current rollout worker.
            samples: Batch to be returned. You can mutate this
                object to modify the samples generated.
            kwargs: Forward compatibility placeholder.
        """

        if self.legacy_callbacks.get("on_sample_end"):
            self.legacy_callbacks["on_sample_end"](
                {
                    "worker": worker,
                    "samples": samples,
                }
            )

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

    def on_train_result(self, *, trainer: "Trainer", result: dict, **kwargs) -> None:
        """Called at the end of Trainable.train().

        Args:
            trainer: Current trainer instance.
            result: Dict of results returned from trainer.train() call.
                You can mutate this object to add additional metrics.
            kwargs: Forward compatibility placeholder.
        """

        if self.legacy_callbacks.get("on_train_result"):
            self.legacy_callbacks["on_train_result"](
                {
                    "trainer": trainer,
                    "result": result,
                }
            )


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

    def on_episode_end(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[PolicyID, Policy],
        episode: Episode,
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")

        for stat in top_stats[:10]:
            count = stat.count
            size = stat.size

            trace = str(stat.traceback)

            episode.custom_metrics[f"tracemalloc/{trace}/size"] = size
            episode.custom_metrics[f"tracemalloc/{trace}/count"] = count

        process = psutil.Process(os.getpid())
        worker_rss = process.memory_info().rss
        worker_data = process.memory_info().data
        worker_vms = process.memory_info().vms
        episode.custom_metrics["tracemalloc/worker/rss"] = worker_rss
        episode.custom_metrics["tracemalloc/worker/data"] = worker_data
        episode.custom_metrics["tracemalloc/worker/vms"] = worker_vms


class MultiCallbacks(DefaultCallbacks):
    """MultiCallbacks allows multiple callbacks to be registered at
    the same time in the config of the environment.

    Example:

        .. code-block:: python

            'callbacks': MultiCallbacks([
                MyCustomStatsCallbacks,
                MyCustomVideoCallbacks,
                MyCustomTraceCallbacks,
                ....
            ])
    """

    def __init__(self, callback_class_list):
        super().__init__()
        self._callback_class_list = callback_class_list

        self._callback_list = []

    def __call__(self, *args, **kwargs):
        self._callback_list = [
            callback_class() for callback_class in self._callback_class_list
        ]

        return self

    def on_episode_start(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[PolicyID, Policy],
        episode: Episode,
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        for callback in self._callback_list:
            callback.on_episode_start(
                worker=worker,
                base_env=base_env,
                policies=policies,
                episode=episode,
                env_index=env_index,
                **kwargs,
            )

    def on_episode_step(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Optional[Dict[PolicyID, Policy]] = None,
        episode: Episode,
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        for callback in self._callback_list:
            callback.on_episode_step(
                worker=worker,
                base_env=base_env,
                policies=policies,
                episode=episode,
                env_index=env_index,
                **kwargs,
            )

    def on_episode_end(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[PolicyID, Policy],
        episode: Episode,
        env_index: Optional[int] = None,
        **kwargs,
    ) -> None:
        for callback in self._callback_list:
            callback.on_episode_end(
                worker=worker,
                base_env=base_env,
                policies=policies,
                episode=episode,
                env_index=env_index,
                **kwargs,
            )

    def on_postprocess_trajectory(
        self,
        *,
        worker: "RolloutWorker",
        episode: Episode,
        agent_id: AgentID,
        policy_id: PolicyID,
        policies: Dict[PolicyID, Policy],
        postprocessed_batch: SampleBatch,
        original_batches: Dict[AgentID, SampleBatch],
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

    def on_sample_end(
        self, *, worker: "RolloutWorker", samples: SampleBatch, **kwargs
    ) -> None:
        for callback in self._callback_list:
            callback.on_sample_end(worker=worker, samples=samples, **kwargs)

    def on_learn_on_batch(
        self, *, policy: Policy, train_batch: SampleBatch, result: dict, **kwargs
    ) -> None:
        for callback in self._callback_list:
            callback.on_learn_on_batch(
                policy=policy, train_batch=train_batch, result=result, **kwargs
            )

    def on_train_result(self, *, trainer, result: dict, **kwargs) -> None:
        for callback in self._callback_list:
            callback.on_train_result(trainer=trainer, result=result, **kwargs)


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
        self._rms = MovingMeanStd()
        super().__init__(*args, **kwargs)

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

    def on_train_result(self, *, trainer, result: dict, **kwargs) -> None:
        # TODO(gjoliver): Remove explicit _step tracking and pass
        # trainer._iteration as a parameter to on_learn_on_batch() call.
        RE3UpdateCallbacks._step = result["training_iteration"]
        super().on_train_result(trainer=trainer, result=result, **kwargs)
