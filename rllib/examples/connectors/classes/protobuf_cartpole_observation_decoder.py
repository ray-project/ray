from typing import Any, List, Optional

import gymnasium as gym
import numpy as np

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.examples.envs.classes.utils.cartpole_observations_proto import (
    CartPoleObservation,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class ProtobufCartPoleObservationDecoder(ConnectorV2):
    """Env-to-module ConnectorV2 piece decoding protobuf obs into CartPole-v1 obs.

    Add this connector piece to your env-to-module pipeline, through your algo config:
    ```
    config.env_runners(
        env_to_module_connector=lambda env: ProtobufCartPoleObservationDecoder()
    )
    ```

    The incoming observation space must be a 1D Box of dtype uint8
    (which is the same as a binary string). The outgoing observation space is the
    normal CartPole-v1 1D space: Box(-inf, inf, (4,), float32).
    """

    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        # Make sure the incoming observation space is a protobuf (binary string).
        assert (
            isinstance(input_observation_space, gym.spaces.Box)
            and len(input_observation_space.shape) == 1
            and input_observation_space.dtype.name == "uint8"
        )
        # Return CartPole-v1's natural observation space.
        return gym.spaces.Box(float("-inf"), float("inf"), (4,), np.float32)

    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # Loop through all episodes and change the observation from a binary string
        # to an actual 1D np.ndarray (normal CartPole-v1 obs).
        for sa_episode in self.single_agent_episode_iterator(episodes=episodes):
            # Get last obs (binary string).
            obs = sa_episode.get_observations(-1)
            obs_bytes = obs.tobytes()
            obs_protobuf = CartPoleObservation()
            obs_protobuf.ParseFromString(obs_bytes)

            # Set up the natural CartPole-v1 observation tensor from the protobuf
            # values.
            new_obs = np.array(
                [
                    obs_protobuf.x_pos,
                    obs_protobuf.x_veloc,
                    obs_protobuf.angle_pos,
                    obs_protobuf.angle_veloc,
                ],
                np.float32,
            )

            # Write the new observation (1D tensor) back into the Episode.
            sa_episode.set_observations(new_data=new_obs, at_indices=-1)

        # Return `data` as-is.
        return batch
