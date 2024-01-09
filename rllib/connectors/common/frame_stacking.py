import numpy as np
from typing import Any, List, Optional

import gymnasium as gym
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import EpisodeType


class _FrameStackingConnector(ConnectorV2):
    """A connector piece that stacks the previous n observations into one."""

    def __init__(
        self,
        *,
        # Base class constructor args.
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
        # Specific framestacking args.
        num_frames: int = 1,
        as_learner_connector: bool = False,
        **kwargs,
    ):
        """Initializes a _FrameStackingConnector instance.

        Args:
            num_frames: The number of observation frames to stack up (into a single
                observation) for the RLModule's forward pass.
            as_preprocessor: Whether this connector should simply postprocess the
                received observations from the env and store these directly in the
                episode object. In this mode, the connector can only be used in
                an `EnvToModulePipeline` and it will act as a classic
                RLlib framestacking postprocessor.
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to an env-to-module pipeline.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

        self.num_frames = num_frames
        self.as_learner_connector = as_learner_connector

        # Some assumptions: Space is box AND last dim (the stacking one) is 1.
        assert isinstance(self.observation_space, gym.spaces.Box)
        assert self.observation_space.shape[-1] == 1

        # Change our observation space according to the given stacking settings.
        self.observation_space = gym.spaces.Box(
            low=np.repeat(self.observation_space.low, repeats=self.num_frames, axis=-1),
            high=np.repeat(
                self.observation_space.high, repeats=self.num_frames, axis=-1
            ),
            shape=list(self.observation_space.shape)[:-1] + [self.num_frames],
            dtype=self.observation_space.dtype,
        )

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
        # RLModule]. We will add to `data` the last n observations.
        observations = []

        # Learner connector pipeline. Episodes have been finalized/numpy'ized.
        if self.as_learner_connector:
            for episode in episodes:

                def _map_fn(s):
                    # Squeeze out last dim.
                    s = np.squeeze(s, axis=-1)
                    # Calculate new shape and strides
                    new_shape = (len(episode), self.num_frames) + s.shape[1:]
                    new_strides = (s.strides[0],) + s.strides
                    # Create a strided view of the array.
                    return np.lib.stride_tricks.as_strided(
                        s, shape=new_shape, strides=new_strides
                    )

                # Get all observations from the episode in one np array (except for
                # the very last one, which is the final observation not needed for
                # learning).
                observations.append(
                    tree.map_structure(
                        _map_fn,
                        episode.get_observations(
                            indices=slice(-self.num_frames + 1, len(episode)),
                            neg_indices_left_of_zero=True,
                            fill=0.0,
                        ),
                    )
                )

            # Move stack-dimension to the end and concatenate along batch axis.
            data[SampleBatch.OBS] = tree.map_structure(
                lambda *s: np.transpose(np.concatenate(s, axis=0), axes=[0, 2, 3, 1]),
                *observations,
            )

        # Env-to-module pipeline. Episodes still operate on lists.
        else:
            for episode in episodes:
                assert not episode.is_finalized
                # Get the list of observations to stack.
                obs_stack = episode.get_observations(
                    indices=slice(-self.num_frames, None),
                    fill=0.0,
                )
                # Observation components are (w, h, 1)
                # -> stack to (w, h, [num_frames], 1), then squeeze out last dim to get
                # (w, h, [num_frames]).
                stacked_obs = tree.map_structure(
                    lambda *s: np.squeeze(np.stack(s, axis=2), axis=-1),
                    *obs_stack,
                )
                observations.append(stacked_obs)

            data[SampleBatch.OBS] = batch(observations)

        return data
