import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.spaces.space_utils import batch, get_dummy_batch_for_space


class PrevRewardPrevActionConnector(ConnectorV2):
    def __init__(
        self,
        *,
        ctx,
        n_prev_actions: int = 1,
        n_prev_rewards: int = 1,
        as_learner_connector=False,
    ):
        """Initializes a PrevRewardPrevActionConnector instance.

        Args:
            ctx: The ConnectorContextV2 for this connector.
            n_prev_actions: The number of previous actions to include in the output
                data. Discrete actions are ont-hot'd. If > 1, will concatenate the
                individual action tensors.
            n_prev_rewards: The number of previous rewards to include in the output
                data.
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to a env-to-module pipeline.
        """
        super().__init__(ctx=ctx)

        self.n_prev_actions = n_prev_actions
        self.n_prev_rewards = n_prev_rewards
        self.as_learner_connector = as_learner_connector

        self.action_space = ctx.rl_module.config.action_space

        self.r0 = [0.0] * self.n_prev_rewards
        self.a0 = get_dummy_batch_for_space(
            self.action_space,
            batch_size=self.n_prev_actions,
            fill_value=0.0,
            one_hot_discrete=True,
        )

    def __call__(self, *, input_, episodes, ctx, **kwargs):
        # This is a data-in-data-out connector, so we expect `input_` to be a dict
        # with: key=column name, e.g. "obs" and value=[data to be processed by RLModule].
        # We will just extract the most recent rewards and/or most recent actions from
        # all episodes and store them inside the `input_` data dict.

        prev_a = []
        prev_r = []
        for episode in episodes:
            len_ = len(episode) + episode._len_lookback_buffer
            a0_idx = len_ - self.n_prev_actions
            r0_idx = len_ - self.n_prev_rewards
            # Learner connector pipeline. Episodes have been finalized/numpy'ized.
            if self.as_learner_connector:
                assert episode.is_numpy
                prev_r.extend([
                    (self.r0[idx:] if idx < 0 else [])
                    + episode.rewards[max(idx, 0):idx + self.n_prev_rewards]
                    for idx in range(-self.n_prev_rewards, len_ - self.n_prev_rewards)
                ])
                TODO: episodes.actions need to be one-hot'd
                prev_a.append(
                    tree.map_structure(
                        lambda s0, s: np.concatenate((s0[idx:] if idx < 0 else [])
                            + s[max(idx, 0):idx + self.n_prev_actions]),
                        self.a0,
                        episode.get_actions(),
                    )
                    for idx in range(-self.n_prev_actions, len_ - self.n_prev_actions)
                )
            # Env-to-module pipeline. Episodes still operate on lists.
            else:
                assert not episode.is_numpy
                prev_r.append(
                    episode.get_rewards(slice(-self.n_prev_rewards, None), fill=0.0)
                )
                TODO: episodes.actions need to be one-hot'd
                prev_a.append(
                    (self.a0[a0_idx:] if a0_idx < 0 else [])
                    + episode.actions[-self.n_prev_actions:]
                )

        input_[SampleBatch.PREV_ACTIONS] = batch(prev_a)
        input_[SampleBatch.PREV_REWARDS] = np.array(prev_r)
        return input_
