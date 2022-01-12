import unittest
from unittest.mock import Mock, MagicMock

from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.utils.pre_checks.env import check_gym_environments


class TestGymCheckEnv(unittest.TestCase):
    def test_has_observation_and_action_space(self):
        env = Mock(spec=[])
        with pytest.raises(
                AttributeError, match="Env must have observation_space."):
            check_gym_environments(env)
        env = Mock(spec=["observation_space"])
        with pytest.raises(
                AttributeError, match="Env must have action_space."):
            check_gym_environments(env)
        del env

    def test_obs_and_action_spaces_are_gym_spaces(self):
        env = RandomEnv()
        observation_space = env.observation_space
        env.observation_space = "not a gym space"
        with pytest.raises(
                ValueError, match="Observation space must be a gym.space"):
            check_gym_environments(env)
        env.observation_space = observation_space
        env.action_space = "not an action space"
        with pytest.raises(
                ValueError, match="Action space must be a gym.space"):
            check_gym_environments(env)
        del env

    def test_sampled_observation_contained(self):
        env = RandomEnv()
        # check for observation that is out of bounds
        error = ".*A sampled observation from your env wasn't contained .*"
        env.observation_space.sample = MagicMock(return_value=5)
        with pytest.raises(ValueError, match=error):
            check_gym_environments(env)
        # check for observation that is in bounds, but the wrong type
        env.observation_space.sample = MagicMock(return_value=float(1))
        with pytest.raises(ValueError, match=error):
            check_gym_environments(env)
        del env

    def test_sampled_action_contained(self):
        env = RandomEnv()
        error = ".*A sampled action from your env wasn't contained .*"
        env.action_space.sample = MagicMock(return_value=5)
        with pytest.raises(ValueError, match=error):
            check_gym_environments(env)
        # check for observation that is in bounds, but the wrong type
        env.action_space.sample = MagicMock(return_value=float(1))
        with pytest.raises(ValueError, match=error):
            check_gym_environments(env)
        del env

    def test_reset(self):
        reset = MagicMock(return_value=5)
        env = RandomEnv()
        env.reset = reset
        # check reset with out of bounds fails
        error = ".*The observation collected from env.reset().*"
        with pytest.raises(ValueError, match=error):
            check_gym_environments(env)
        # check reset with obs of incorrect type fails
        reset = MagicMock(return_value=float(1))
        env.reset = reset
        with pytest.raises(ValueError, match=error):
            check_gym_environments(env)
        del env

    def test_step(self):
        step = MagicMock(return_value=(5, 5, True, {}))
        env = RandomEnv()
        env.step = step
        error = ".*The observation collected from env.step.*"
        with pytest.raises(ValueError, match=error):
            check_gym_environments(env)

        # check reset that returns obs of incorrect type fails
        step = MagicMock(return_value=(float(1), 5, True, {}))
        env.step = step
        with pytest.raises(ValueError, match=error):
            check_gym_environments(env)

        # check step that returns reward of non float/int fails
        step = MagicMock(return_value=(1, "Not a valid reward", True, {}))
        env.step = step
        error = ("Your step function must return a reward that is integer or "
                 "float.")
        with pytest.raises(AssertionError, match=error):
            check_gym_environments(env)

        # check step that returns a non bool fails
        step = MagicMock(
            return_value=(1, float(5), "not a valid done signal", {}))
        env.step = step
        error = "Your step function must return a done that is a boolean."
        with pytest.raises(AssertionError, match=error):
            check_gym_environments(env)

        # check step that returns a non dict fails
        step = MagicMock(
            return_value=(1, float(5), True, "not a valid env info"))
        env.step = step
        error = "Your step function must return a info that is a dict."
        with pytest.raises(AssertionError, match=error):
            check_gym_environments(env)
        del env


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
