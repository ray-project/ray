from functools import partial
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.spaces.space_utils import batch, get_dummy_batch_for_space


class _PrevRewardPrevActionConnector(ConnectorV2):
    """A connector piece that adds previous rewards and actions to the input."""
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

    def __call__(self, *, input_, episodes, ctx, **kwargs):
        # This is a data-in-data-out connector, so we expect `input_` to be a dict
        # with: key=column name, e.g. "obs" and value=[data to be processed by RLModule].
        # We will just extract the most recent rewards and/or most recent actions from
        # all episodes and store them inside the `input_` data dict.

        prev_a = []
        prev_r = []
        for episode in episodes:
            # Learner connector pipeline. Episodes have been finalized/numpy'ized.
            if self.as_learner_connector:
                assert episode.is_finalized
                # Loop through each timestep in the episode and add the previous n
                # actions and previous m rewards (based on that timestep) to the batch.
                for ts in range(len(episode)):
                    prev_a.append(episode.get_actions(
                        # Extract n actions from `ts - n` to `ts` (excluding `ts`).
                        indices=slice(ts - self.n_prev_actions, ts),
                        # Make sure negative indices are NOT interpreted as "counting
                        # from the end", but as absolute indices meaning they refer
                        # to timesteps before 0 (which is the lookback buffer).
                        neg_indices_left_of_zero=True,
                        # In case we are at the very beginning of the episode, e.g.
                        # ts==0, fill the left side with zero-actions.
                        fill=0.0,
                        # Return one-hot arrays for those action components that are
                        # discrete or multi-discrete.
                        one_hot_discrete=True,
                    ))
                    # Do the same for rewards.
                    prev_r.append(episode.get_rewards(
                        indices=slice(ts - self.n_prev_rewards, ts),
                        neg_indices_left_of_zero=True,
                        fill=0.0,
                    ))
            # Env-to-module pipeline. Episodes still operate on lists.
            else:
                assert not episode.is_finalized
                prev_a.append(batch(episode.get_actions(
                    indices=slice(-self.n_prev_actions, None),
                    fill=0.0,
                    one_hot_discrete=True,
                )))
                prev_r.append(np.array(episode.get_rewards(
                    indices=slice(-self.n_prev_rewards, None),
                    fill=0.0,
                )))

        input_[SampleBatch.PREV_ACTIONS] = batch(prev_a)
        input_[SampleBatch.PREV_REWARDS] = np.array(prev_r)
        return input_


PrevRewardPrevActionEnvToModule = partial(
    _PrevRewardPrevActionConnector, as_learner_connector=False
)
