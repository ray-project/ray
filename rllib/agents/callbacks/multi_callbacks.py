from typing import Optional, Dict, TYPE_CHECKING

from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.typing import PolicyID, AgentID

if TYPE_CHECKING:
    from ray.rllib.evaluation import RolloutWorker


@PublicAPI
class MultiCallbacks(DefaultCallbacks):
    """
    MultiCallback allows multiple callbacks to be registered at the same
    time in the config of the environment

    For example:

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

    def on_episode_start(self,
                         *,
                         worker: "RolloutWorker",
                         base_env: BaseEnv,
                         policies: Dict[PolicyID, Policy],
                         episode: MultiAgentEpisode,
                         env_index: Optional[int] = None,
                         **kwargs) -> None:
        for callback in self._callback_list:
            callback.on_episode_start(
                worker=worker,
                base_env=base_env,
                policies=policies,
                episode=episode,
                env_index=env_index,
                **kwargs)

    def on_episode_step(self,
                        *,
                        worker: "RolloutWorker",
                        base_env: BaseEnv,
                        episode: MultiAgentEpisode,
                        env_index: Optional[int] = None,
                        **kwargs) -> None:
        for callback in self._callback_list:
            callback.on_episode_step(
                worker=worker,
                base_env=base_env,
                episode=episode,
                env_index=env_index,
                **kwargs)

    def on_episode_end(self,
                       *,
                       worker: "RolloutWorker",
                       base_env: BaseEnv,
                       policies: Dict[PolicyID, Policy],
                       episode: MultiAgentEpisode,
                       env_index: Optional[int] = None,
                       **kwargs) -> None:
        for callback in self._callback_list:
            callback.on_episode_end(
                worker=worker,
                base_env=base_env,
                policies=policies,
                episode=episode,
                env_index=env_index,
                **kwargs)

    def on_postprocess_trajectory(
            self, *, worker: "RolloutWorker", episode: MultiAgentEpisode,
            agent_id: AgentID, policy_id: PolicyID,
            policies: Dict[PolicyID, Policy], postprocessed_batch: SampleBatch,
            original_batches: Dict[AgentID, SampleBatch], **kwargs) -> None:
        for callback in self._callback_list:
            callback.on_postprocess_trajectory(
                worker=worker,
                episode=episode,
                agent_id=agent_id,
                policy_id=policy_id,
                policies=policies,
                postprocessed_batch=postprocessed_batch,
                original_batches=original_batches,
                **kwargs)

    def on_sample_end(self, *, worker: "RolloutWorker", samples: SampleBatch,
                      **kwargs) -> None:
        for callback in self._callback_list:
            callback.on_sample_end(worker=worker, samples=samples, **kwargs)

    def on_learn_on_batch(self, *, policy: Policy, train_batch: SampleBatch,
                          result: dict, **kwargs) -> None:
        for callback in self._callback_list:
            callback.on_learn_on_batch(
                policy=policy,
                train_batch=train_batch,
                result=result,
                **kwargs)

    def on_train_result(self, *, trainer, result: dict, **kwargs) -> None:
        for callback in self._callback_list:
            callback.on_train_result(trainer=trainer, result=result, **kwargs)
