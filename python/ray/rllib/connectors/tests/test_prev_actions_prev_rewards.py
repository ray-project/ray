import gymnasium as gym
import numpy as np
import pytest

from ray.rllib.connectors.env_to_module.prev_actions_prev_rewards import (
    PrevActionsPrevRewards,
)


def test_convert_individual_space_discrete():
    connector = PrevActionsPrevRewards(n_prev_actions=2, n_prev_rewards=1)
    converted = connector._convert_individual_space(
        gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
        gym.spaces.Discrete(5),
    )
    prev_actions_space = converted[connector.PREV_ACTIONS_KEY]
    assert prev_actions_space.shape == (10,)
    assert prev_actions_space.contains(np.zeros(10, np.float32))


def test_convert_individual_space_box():
    connector = PrevActionsPrevRewards(n_prev_actions=3, n_prev_rewards=1)
    converted = connector._convert_individual_space(
        gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
        gym.spaces.Box(-2.0, 2.0, (3,), np.float32),
    )
    prev_actions_space = converted[connector.PREV_ACTIONS_KEY]
    # One flattened Box action per previous step.
    assert prev_actions_space.shape == (9,)
    flat_action = np.zeros(9, np.float32)
    assert prev_actions_space.contains(flat_action)
    # The flattened bounds must match the tiled action-space bounds.
    assert np.allclose(prev_actions_space.low, np.tile([-2.0] * 3, 3))
    assert np.allclose(prev_actions_space.high, np.tile([2.0] * 3, 3))


def test_convert_individual_space_multi_discrete():
    connector = PrevActionsPrevRewards(n_prev_actions=2, n_prev_rewards=1)
    converted = connector._convert_individual_space(
        gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
        gym.spaces.MultiDiscrete([2, 3]),
    )
    prev_actions_space = converted[connector.PREV_ACTIONS_KEY]
    # One concatenated one-hot vector per previous step.
    assert prev_actions_space.shape == (10,)
    assert prev_actions_space.contains(np.zeros(10, np.float32))


def test_convert_individual_space_unsupported_space_raises():
    connector = PrevActionsPrevRewards(n_prev_actions=1, n_prev_rewards=1)
    with pytest.raises(ValueError, match="does not support action space"):
        connector._convert_individual_space(
            gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
            gym.spaces.Tuple([gym.spaces.Discrete(2)]),
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
