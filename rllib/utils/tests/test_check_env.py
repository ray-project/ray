import logging
import numpy as np
import pytest
import unittest
from unittest.mock import Mock, MagicMock

from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.utils.pre_checks.env import check_env, check_gym_environments, \
    check_multiagent_environments


class TestGymCheckEnv(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        caplog.set_level(logging.CRITICAL)

    def test_has_observation_and_action_space(self):
        env = Mock(spec=[])
        with pytest.raises(
                AttributeError, match="Env must have observation_space."):
            check_gym_environments(env)
        env = Mock(spec=["observation_space"])
        with pytest.raises(
                AttributeError, match="Env must have action_space."):
            check_gym_environments(env)

    def test_obs_and_action_spaces_are_gym_spaces(self):
        env = RandomEnv()
        observation_space = env.observation_space
        env.observation_space = "not a gym space"
        with pytest.raises(
                ValueError, match="Observation space must be a gym.space"):
            check_env(env)
        env.observation_space = observation_space
        env.action_space = "not an action space"
        with pytest.raises(
                ValueError, match="Action space must be a gym.space"):
            check_env(env)

    def test_sampled_observation_contained(self):
        env = RandomEnv()
        # check for observation that is out of bounds
        error = ".*A sampled observation from your env wasn't contained .*"
        env.observation_space.sample = MagicMock(return_value=5)
        with pytest.raises(ValueError, match=error):
            check_env(env)
        # check for observation that is in bounds, but the wrong type
        env.observation_space.sample = MagicMock(return_value=float(1))
        with pytest.raises(ValueError, match=error):
            check_env(env)

    def test_sampled_action_contained(self):
        env = RandomEnv()
        error = ".*A sampled action from your env wasn't contained .*"
        env.action_space.sample = MagicMock(return_value=5)
        with pytest.raises(ValueError, match=error):
            check_env(env)
        # check for observation that is in bounds, but the wrong type
        env.action_space.sample = MagicMock(return_value=float(1))
        with pytest.raises(ValueError, match=error):
            check_env(env)

    def test_reset(self):
        reset = MagicMock(return_value=5)
        env = RandomEnv()
        env.reset = reset
        # check reset with out of bounds fails
        error = ".*The observation collected from env.reset().*"
        with pytest.raises(ValueError, match=error):
            check_env(env)
        # check reset with obs of incorrect type fails
        reset = MagicMock(return_value=float(1))
        env.reset = reset
        with pytest.raises(ValueError, match=error):
            check_env(env)

    def test_step(self):
        step = MagicMock(return_value=(5, 5, True, {}))
        env = RandomEnv()
        env.step = step
        error = ".*The observation collected from env.step.*"
        with pytest.raises(ValueError, match=error):
            check_env(env)

        # check reset that returns obs of incorrect type fails
        step = MagicMock(return_value=(float(1), 5, True, {}))
        env.step = step
        with pytest.raises(ValueError, match=error):
            check_env(env)

        # check step that returns reward of non float/int fails
        step = MagicMock(return_value=(1, "Not a valid reward", True, {}))
        env.step = step
        error = ("Your step function must return a reward that is integer or "
                 "float.")
        with pytest.raises(AssertionError, match=error):
            check_env(env)

        # check step that returns a non bool fails
        step = MagicMock(
            return_value=(1, float(5), "not a valid done signal", {}))
        env.step = step
        error = "Your step function must return a done that is a boolean."
        with pytest.raises(AssertionError, match=error):
            check_env(env)

        # check step that returns a non dict fails
        step = MagicMock(
            return_value=(1, float(5), True, "not a valid env info"))
        env.step = step
        error = "Your step function must return a info that is a dict."
        with pytest.raises(AssertionError, match=error):
            check_env(env)


class TestCheckMultiAgentEnv(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        caplog.set_level(logging.CRITICAL)

    def test_check_env_not_correct_type_error(self):
        env = RandomEnv()
        with pytest.raises(ValueError, match="The passed env is not"):
            check_multiagent_environments(env)

    def test_check_env_reset_incorrect_error(self):
        reset = MagicMock(return_value=5)
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        env.reset = reset
        with pytest.raises(ValueError, match="The observation returned by "):
            check_env(env)

    def test_check_incorrect_space_contains_functions_error(self):
        def bad_contains_function(self, x):
            raise ValueError("This is a bad contains function")

        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        bad_obs = {
            0: np.array([np.inf, np.inf, np.inf, np.inf]),
            1: np.array([np.inf, np.inf, np.inf, np.inf])
        }
        env.reset = lambda *_: bad_obs
        with pytest.raises(
                ValueError, match="The observation collected from "
                "env"):
            check_env(env)
        env.observation_space_contains = bad_contains_function
        with pytest.raises(
                ValueError,
                match="Your observation_space_contains "
                "function has some"):
            check_env(env)
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        bad_action = {0: 2, 1: 2}
        env.action_space_sample = lambda *_: bad_action
        with pytest.raises(
                ValueError,
                match="The action collected from "
                "action_space_sample"):
            check_env(env)

        env.action_space_contains = bad_contains_function
        with pytest.raises(
                ValueError,
                match="Your action_space_contains "
                "function has some error"):
            check_env(env)

    def test_check_env_step_incorrect_error(self):
        step = MagicMock(return_value=(5, 5, True, {}))
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        sampled_obs = env.reset()
        env.step = step
        with pytest.raises(
                ValueError, match="The observation returned by env"):
            check_env(env)

        step = MagicMock(return_value=(sampled_obs, "Not a reward", True, {}))
        env.step = step
        with pytest.raises(
                AssertionError,
                match="Your step function must "
                "return a reward "):
            check_env(env)
        step = MagicMock(return_value=(sampled_obs, 5, "Not a bool", {}))
        env.step = step
        with pytest.raises(
                AssertionError, match="Your step function must "
                "return a done"):
            check_env(env)

        step = MagicMock(return_value=(sampled_obs, 5, False, "Not a Dict"))
        env.step = step
        with pytest.raises(
                AssertionError, match="Your step function must "
                "return a info"):
            check_env(env)


if __name__ == "__main__":
    pytest.main()
