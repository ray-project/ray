from functools import partial
import numpy as np
from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import EpisodeType


class _PrevRewardPrevActionConnector(ConnectorV2):
    """A connector piece that adds previous rewards and actions to the input."""

    def __init__(
        self,
        *,
        # Base class constructor args.
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
        # Specific prev. r/a args.
        n_prev_actions: int = 1,
        n_prev_rewards: int = 1,
        as_learner_connector: bool = False,
        **kwargs,
    ):
        """Initializes a _PrevRewardPrevActionConnector instance.

        Args:
            n_prev_actions: The number of previous actions to include in the output
                data. Discrete actions are ont-hot'd. If > 1, will concatenate the
                individual action tensors.
            n_prev_rewards: The number of previous rewards to include in the output
                data.
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to a env-to-module pipeline.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

        self.n_prev_actions = n_prev_actions
        self.n_prev_rewards = n_prev_rewards
        self._as_learner_connector = as_learner_connector

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
        # This is a data-in-data-out connector, so we expect `data` to be a dict
        # with: key=column name, e.g. "obs" and value=[data to be processed by
        # RLModule]. We will just extract the most recent rewards and/or most recent
        # actions from all episodes and store them inside the `data` data dict.

        prev_a = []
        prev_r = []
        for episode in episodes:
            # TODO (sven): Get rid of this distinction. With the new Episode APIs,
            #  this should work the same, whether on finalized or non-finalized
            #  episodes.
            # Learner connector pipeline. Episodes have been finalized/numpy'ized.
            if self._as_learner_connector:
                assert episode.is_finalized
                # Loop through each timestep in the episode and add the previous n
                # actions and previous m rewards (based on that timestep) to the batch.
                for ts in range(len(episode)):
                    prev_a.append(
                        episode.get_actions(
                            # Extract n actions from `ts - n` to `ts` (excluding `ts`).
                            indices=slice(ts - self.n_prev_actions, ts),
                            # Make sure negative indices are NOT interpreted as
                            # "counting from the end", but as absolute indices meaning
                            # they refer to timesteps before 0 (which is the lookback
                            # buffer).
                            neg_indices_left_of_zero=True,
                            # In case we are at the very beginning of the episode, e.g.
                            # ts==0, fill the left side with zero-actions.
                            fill=0.0,
                            # Return one-hot arrays for those action components that are
                            # discrete or multi-discrete.
                            one_hot_discrete=True,
                        )
                    )
                    # Do the same for rewards.
                    prev_r.append(
                        episode.get_rewards(
                            indices=slice(ts - self.n_prev_rewards, ts),
                            neg_indices_left_of_zero=True,
                            fill=0.0,
                        )
                    )
            # Env-to-module pipeline.
            # Episode is not finalized yet and thus still operates on lists of items.
            else:
                assert not episode.is_finalized
                prev_a.append(
                    batch(
                        episode.get_actions(
                            indices=slice(-self.n_prev_actions, None),
                            fill=0.0,
                            one_hot_discrete=True,
                        )
                    )
                )
                prev_r.append(
                    np.array(
                        episode.get_rewards(
                            indices=slice(-self.n_prev_rewards, None),
                            fill=0.0,
                        )
                    )
                )

        data[SampleBatch.PREV_ACTIONS] = batch(prev_a)
        data[SampleBatch.PREV_REWARDS] = np.array(prev_r)
        return data


PrevRewardPrevActionEnvToModule = partial(
    _PrevRewardPrevActionConnector, as_learner_connector=False
)
