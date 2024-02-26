from typing import Any, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class AddColumnsToTrainBatch(ConnectorV2):

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Optional[Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:

        # Infos.
        if SampleBatch.INFOS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    SampleBatch.INFOS,
                    items_to_add=[
                        sa_episode.get_infos(indices=ts)
                        for ts in range(len(sa_episode))
                    ],
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )

        # Actions.
        if SampleBatch.ACTIONS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    SampleBatch.ACTIONS,
                    items_to_add=[
                        sa_episode.get_actions(indices=ts)
                        for ts in range(len(sa_episode))
                    ],
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        # Rewards.
        if SampleBatch.REWARDS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    SampleBatch.REWARDS,
                    items_to_add=[
                        sa_episode.get_rewards(indices=ts)
                        for ts in range(len(sa_episode))
                    ],
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        # Terminateds.
        if SampleBatch.TERMINATEDS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    SampleBatch.TERMINATEDS,
                    items_to_add=(
                        [False] * (len(sa_episode) - 1) + [sa_episode.is_terminated]
                    ),
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        # Truncateds.
        if SampleBatch.TRUNCATEDS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    SampleBatch.TRUNCATEDS,
                    items_to_add=(
                        [False] * (len(sa_episode) - 1) + [sa_episode.is_truncated]
                    ),
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        # Extra model outputs (except for STATE_OUT, which will be handled by another
        # default connector piece).
        ref_sa_eps = (
            episodes[0] if isinstance(episodes[0], SingleAgentEpisode)
            else next(iter(episodes[0].agent_episodes.values()))
        )
        for column in ref_sa_eps.extra_model_outputs.keys():
            if column != STATE_OUT and column not in data:
                for sa_episode in self.single_agent_episode_iterator(
                        episodes,
                        agents_that_stepped_only=False,
                ):
                    self.add_n_batch_items(
                        data,
                        column,
                        items_to_add=[
                            sa_episode.get_extra_model_outputs(key=column, indices=ts)
                            for ts in range(len(sa_episode))
                        ],
                        num_items=len(sa_episode),
                        single_agent_episode=sa_episode,
                    )

        return data
