from typing import Any, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class AddColumnsFromEpisodesToTrainBatch(ConnectorV2):
    """Adds infos/actions/rewards/terminateds/... to train batch.

    Does NOT add observations to train batch (these should have already been added
    by a different ConnectorV2 piece: AddObservationsToTrainBatch)

    If provided with `episodes` data, this connector piece makes sure that the final
    train batch going into the RLModule for updating (`forward_train()` call) contains
    at the minimum:
    - Observations: From all episodes under the Columns.OBS key.
    - Actions, rewards, terminal/truncation flags: From all episodes under the
    respective keys.
    - All data inside the episodes' `extra_model_outs` property, e.g. action logp and
    action probs under the respective keys.
    - Internal states: These will NOT be added to the batch by this connector piece
    as this functionality is handled by a different default connector piece:
    `AddStatesFromEpisodesToBatch`.

    If the user wants to customize their own data under the given keys (e.g. obs,
    actions, ...), they can extract from the episodes or recompute from `data`
    their own data and store it in `data` under those keys. In this case, the default
    connector will not change the data under these keys and simply act as a
    pass-through.
    """

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
        if Columns.INFOS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    Columns.INFOS,
                    items_to_add=sa_episode.get_infos(slice(0, len(sa_episode))),
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )

        # Actions.
        if Columns.ACTIONS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    Columns.ACTIONS,
                    items_to_add=[
                        sa_episode.get_actions(indices=ts)
                        for ts in range(len(sa_episode))
                    ],
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        # Rewards.
        if Columns.REWARDS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    Columns.REWARDS,
                    items_to_add=[
                        sa_episode.get_rewards(indices=ts)
                        for ts in range(len(sa_episode))
                    ],
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        # Terminateds.
        if Columns.TERMINATEDS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    Columns.TERMINATEDS,
                    items_to_add=(
                        [False] * (len(sa_episode) - 1) + [sa_episode.is_terminated]
                    ),
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        # Truncateds.
        if Columns.TRUNCATEDS not in data:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    data,
                    Columns.TRUNCATEDS,
                    items_to_add=(
                        [False] * (len(sa_episode) - 1) + [sa_episode.is_truncated]
                    ),
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        # Extra model outputs (except for STATE_OUT, which will be handled by another
        # default connector piece). Also, like with all the fields above, skip
        # those that the user already seemed to have populated via custom connector
        # pieces.
        skip_columns = set(data.keys()) | {Columns.STATE_IN, Columns.STATE_OUT}
        for sa_episode in self.single_agent_episode_iterator(
            episodes,
            agents_that_stepped_only=False,
        ):
            for column in sa_episode.extra_model_outputs.keys():
                if column not in skip_columns:
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
