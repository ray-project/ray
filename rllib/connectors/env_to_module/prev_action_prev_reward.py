import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.spaces.space_utils import batch


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

    def __call__(self, *, input_, episodes, ctx, **kwargs):
        # This is a data-in-data-out connector, so we expect `input_` to be a dict
        # with: key=column name, e.g. "obs" and value=[data to be processed by RLModule].
        # We will just extract the most recent rewards and/or most recent actions from
        # all episodes and store them inside the `input_` data dict.

        # 0th reward == 0.0.
        r0 = [0.0] * self.n_prev_rewards
        # Set 0th action (prior to first action taken in episode) to all 0s.
        a0 = tree.map_structure(
            lambda s: np.zeros_like(s),
            (
                self.action_space.sample()
            ),
        )

        prev_a = []
        prev_r = []
        for episode in episodes:
            # Learner connector pipeline. Episodes have been numpy'ized.
            if self.as_learner_connector:
                assert episode.is_numpy
                prev_r.extend([r0] + list(episode.rewards[:-1]))
                prev_a.extend([a0] + list(episode.actions[:-1]))
            # Env-to-module pipeline. Episodes still operate on lists.
            else:
                assert not episode.is_numpy
                prev_a.append(episode.actions[-1] if len(episode) else a0)
                prev_r.append(episode.rewards[-1] if len(episode) else r0)

        input_[SampleBatch.PREV_ACTIONS] = batch(prev_a)
        input_[SampleBatch.PREV_REWARDS] = np.array(prev_r)
        return input_
