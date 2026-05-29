import gymnasium as gym
import numpy as np
from gymnasium.envs.classic_control import CartPoleEnv

from ray.rllib.examples.envs.classes.utils.cartpole_observations_proto import (
    CartPoleObservation,
)


class CartPoleWithProtobufObservationSpace(CartPoleEnv):
    """CartPole gym environment that has a protobuf observation space.

    Sometimes, it is more performant for an environment to publish its observations
    as a protobuf message (instead of a heavily nested Dict).

    The protobuf message used here is originally defined in the
    `./utils/cartpole_observations.proto` file. We converted this file into a python
    importable module by compiling it with:

    `protoc --python_out=. cartpole_observations.proto`

    .. which yielded the `cartpole_observations_proto.py` file in the same directory
    (we import this file's `CartPoleObservation` message here).

    The new observation space is a (binary) Box(0, 255, ([len of protobuf],), uint8).

    A ConnectorV2 pipeline or simpler gym.Wrapper will have to be used to convert this
    observation format into an NN-readable (e.g. float32) 1D tensor.
    """

    def __init__(self, config=None):
        super().__init__()
        dummy_obs = self._convert_observation_to_protobuf(
            np.array([1.0, 1.0, 1.0, 1.0])
        )
        bin_length = len(dummy_obs)
        self.observation_space = gym.spaces.Box(0, 255, (bin_length,), np.uint8)

    def step(self, action):
        observation, reward, terminated, truncated, info = super().step(action)
        proto_observation = self._convert_observation_to_protobuf(observation)
        return proto_observation, reward, terminated, truncated, info

    def reset(self, **kwargs):
        observation, info = super().reset(**kwargs)
        proto_observation = self._convert_observation_to_protobuf(observation)
        return proto_observation, info

    def _convert_observation_to_protobuf(self, observation):
        x_pos, x_veloc, angle_pos, angle_veloc = observation

        # Create the Protobuf message
        cartpole_observation = CartPoleObservation()
        cartpole_observation.x_pos = x_pos
        cartpole_observation.x_veloc = x_veloc
        cartpole_observation.angle_pos = angle_pos
        cartpole_observation.angle_veloc = angle_veloc

        # Serialize to binary string.
        return np.frombuffer(cartpole_observation.SerializeToString(), np.uint8)


if __name__ == "__main__":
    env = CartPoleWithProtobufObservationSpace()
    obs, info = env.reset()

    # Test loading a protobuf object with data from the obs binary string
    # (uint8 ndarray).
    byte_str = obs.tobytes()
    obs_protobuf = CartPoleObservation()
    obs_protobuf.ParseFromString(byte_str)
    print(obs_protobuf)

    terminated = truncated = False
    while not terminated and not truncated:
        action = env.action_space.sample()
        obs, reward, terminated, truncated, info = env.step(action)

        print(obs)
