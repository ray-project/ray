import gym
from gym.spaces import Box, Dict, Discrete
import logging
import numpy as np
import pytest
import unittest
from unittest.mock import Mock, MagicMock

from ray.rllib.env.base_env import convert_to_base_env
from ray.rllib.env.multi_agent_env import make_multi_agent, MultiAgentEnvWrapper
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.utils.pre_checks.env import (
    check_env,
    check_gym_environments,
    check_multiagent_environments,
    check_base_env,
)


class TestGymCheckEnv(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        caplog.set_level(logging.CRITICAL)

    def test_has_observation_and_action_space(self):
        env = Mock(spec=[])
        with pytest.raises(AttributeError, match="Env must have observation_space."):
            check_gym_environments(env)
        env = Mock(spec=["observation_space"])
        with pytest.raises(AttributeError, match="Env must have action_space."):
            check_gym_environments(env)

    def test_obs_and_action_spaces_are_gym_spaces(self):
        env = RandomEnv()
        observation_space = env.observation_space
        env.observation_space = "not a gym space"
        with pytest.raises(ValueError, match="Observation space must be a gym.space"):
            check_env(env)
        env.observation_space = observation_space
        env.action_space = "not an action space"
        with pytest.raises(ValueError, match="Action space must be a gym.space"):
            check_env(env)

    def test_reset(self):
        reset = MagicMock(return_value=5)
        env = RandomEnv()
        env.reset = reset
        # Check reset with out of bounds fails.
        error = ".*The observation collected from env.reset().*"
        with pytest.raises(ValueError, match=error):
            check_env(env)

        # Check reset with obs of incorrect type fails.
        reset = MagicMock(return_value=float(0.1))
        env.reset = reset
        with pytest.raises(ValueError, match=error):
            check_env(env)

        # Check reset with complex obs in which one sub-space is incorrect.
        env = RandomEnv(
            config={
                "observation_space": Dict(
                    {"a": Discrete(4), "b": Box(-1.0, 1.0, (1,))}
                ),
            }
        )
        reset = MagicMock(return_value={"a": float(0.1), "b": np.array([0.5])})
        error = ".*The observation collected from env.reset.*\\n path: 'a'.*"
        env.reset = reset
        self.assertRaisesRegex(ValueError, error, lambda: check_env(env))

    def test_step(self):
        step = MagicMock(return_value=(5, 5, True, {}))
        env = RandomEnv()
        env.step = step
        error = ".*The observation collected from env.step.*"
        with pytest.raises(ValueError, match=error):
            check_env(env)

        # check reset that returns obs of incorrect type fails
        step = MagicMock(return_value=(float(0.1), 5, True, {}))
        env.step = step
        with pytest.raises(ValueError, match=error):
            check_env(env)

        # check step that returns reward of non float/int fails
        step = MagicMock(return_value=(1, "Not a valid reward", True, {}))
        env.step = step
        error = "Your step function must return a reward that is integer or float."
        with pytest.raises(ValueError, match=error):
            check_env(env)

        # check step that returns a non bool fails
        step = MagicMock(return_value=(1, float(5), "not a valid done signal", {}))
        env.step = step
        error = "Your step function must return a done that is a boolean."
        with pytest.raises(ValueError, match=error):
            check_env(env)

        # check step that returns a non dict fails
        step = MagicMock(return_value=(1, float(5), True, "not a valid env info"))
        env.step = step
        error = "Your step function must return a info that is a dict."
        with pytest.raises(ValueError, match=error):
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
        with pytest.raises(ValueError, match="The element returned by reset"):
            check_env(env)
        bad_obs = {
            0: np.array([np.inf, np.inf, np.inf, np.inf]),
            1: np.array([np.inf, np.inf, np.inf, np.inf]),
        }
        env.reset = lambda *_: bad_obs
        with pytest.raises(ValueError, match="The observation collected from env"):
            check_env(env)

    def test_check_incorrect_space_contains_functions_error(self):
        def bad_contains_function(self, x):
            raise ValueError("This is a bad contains function")

        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        env.observation_space_contains = bad_contains_function
        with pytest.raises(
            ValueError, match="Your observation_space_contains function has some"
        ):
            check_env(env)
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        bad_action = {0: 2, 1: 2}
        env.action_space_sample = lambda *_: bad_action
        with pytest.raises(
            ValueError, match="The action collected from action_space_sample"
        ):
            check_env(env)

        env.action_space_contains = bad_contains_function
        with pytest.raises(
            ValueError, match="Your action_space_contains function has some error"
        ):
            check_env(env)

    def test_check_env_step_incorrect_error(self):
        step = MagicMock(return_value=(5, 5, True, {}))
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        sampled_obs = env.reset()
        env.step = step
        with pytest.raises(ValueError, match="The element returned by step"):
            check_env(env)

        step = MagicMock(return_value=(sampled_obs, {0: "Not a reward"}, {0: True}, {}))
        env.step = step
        with pytest.raises(ValueError, match="Your step function must return rewards"):
            check_env(env)
        step = MagicMock(return_value=(sampled_obs, {0: 5}, {0: "Not a bool"}, {}))
        env.step = step
        with pytest.raises(ValueError, match="Your step function must return dones"):
            check_env(env)

        step = MagicMock(
            return_value=(sampled_obs, {0: 5}, {0: False}, {0: "Not a Dict"})
        )
        env.step = step
        with pytest.raises(ValueError, match="Your step function must return infos"):
            check_env(env)

    def test_bad_sample_function(self):
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        bad_action = {0: 2, 1: 2}
        env.action_space_sample = lambda *_: bad_action
        with pytest.raises(
            ValueError, match="The action collected from action_space_sample"
        ):
            check_env(env)
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        bad_obs = {
            0: np.array([np.inf, np.inf, np.inf, np.inf]),
            1: np.array([np.inf, np.inf, np.inf, np.inf]),
        }
        env.observation_space_sample = lambda *_: bad_obs
        with pytest.raises(
            ValueError,
            match="The observation collected from observation_space_sample",
        ):
            check_env(env)


class TestCheckBaseEnv:
    def _make_base_env(self):
        del self
        num_envs = 2
        sub_envs = [
            make_multi_agent("CartPole-v1")({"num_agents": 2}) for _ in range(num_envs)
        ]
        env = MultiAgentEnvWrapper(None, sub_envs, 2)
        return env

    def test_check_env_not_correct_type_error(self):
        env = RandomEnv()
        with pytest.raises(ValueError, match="The passed env is not"):
            check_base_env(env)

    def test_check_env_reset_incorrect_error(self):
        reset = MagicMock(return_value=5)
        env = self._make_base_env()
        env.try_reset = reset
        with pytest.raises(ValueError, match=("MultiEnvDict. Instead, it is of type")):
            check_env(env)
        obs_with_bad_agent_ids = {
            2: np.array([np.inf, np.inf, np.inf, np.inf]),
            1: np.array([np.inf, np.inf, np.inf, np.inf]),
        }
        obs_with_bad_env_ids = {"bad_env_id": obs_with_bad_agent_ids}
        reset = MagicMock(return_value=obs_with_bad_env_ids)
        env.try_reset = reset
        with pytest.raises(ValueError, match="has dict keys that don't correspond to"):
            check_env(env)
        reset = MagicMock(return_value={0: obs_with_bad_agent_ids})
        env.try_reset = reset

        with pytest.raises(
            ValueError,
            match="The element returned by "
            "try_reset has agent_ids that are"
            " not the names of the agents",
        ):
            check_env(env)
        out_of_bounds_obs = {
            0: {
                0: np.array([np.inf, np.inf, np.inf, np.inf]),
                1: np.array([np.inf, np.inf, np.inf, np.inf]),
            }
        }
        env.try_reset = lambda *_: out_of_bounds_obs
        with pytest.raises(
            ValueError, match="The observation collected from try_reset"
        ):
            check_env(env)

    def test_check_space_contains_functions_errors(self):
        def bad_contains_function(self, x):
            raise ValueError("This is a bad contains function")

        env = self._make_base_env()

        env.observation_space_contains = bad_contains_function
        with pytest.raises(
            ValueError, match="Your observation_space_contains function has some"
        ):
            check_env(env)

        env = self._make_base_env()
        env.action_space_contains = bad_contains_function
        with pytest.raises(
            ValueError, match="Your action_space_contains function has some error"
        ):
            check_env(env)

    def test_bad_sample_function(self):
        env = self._make_base_env()
        bad_action = {0: {0: 2, 1: 2}}
        env.action_space_sample = lambda *_: bad_action
        with pytest.raises(
            ValueError, match="The action collected from action_space_sample"
        ):
            check_env(env)

        env = self._make_base_env()
        bad_obs = {
            0: {
                0: np.array([np.inf, np.inf, np.inf, np.inf]),
                1: np.array([np.inf, np.inf, np.inf, np.inf]),
            }
        }
        env.observation_space_sample = lambda *_: bad_obs
        with pytest.raises(
            ValueError,
            match="The observation collected from observation_space_sample",
        ):
            check_env(env)

    def test_check_env_step_incorrect_error(self):
        good_reward = {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}}
        good_done = {0: {0: False, 1: False}, 1: {0: False, 1: False}}
        good_info = {0: {0: {}, 1: {}}, 1: {0: {}, 1: {}}}

        env = self._make_base_env()
        bad_multi_env_dict_obs = {0: 1, 1: {0: np.zeros(4)}}
        poll = MagicMock(
            return_value=(bad_multi_env_dict_obs, good_reward, good_done, good_info, {})
        )
        env.poll = poll
        with pytest.raises(
            ValueError,
            match="The element returned by step, "
            "next_obs contains values that are not"
            " MultiAgentDicts",
        ):
            check_env(env)

        bad_reward = {0: {0: "not_reward", 1: 1}}
        good_obs = env.observation_space_sample()
        poll = MagicMock(return_value=(good_obs, bad_reward, good_done, good_info, {}))
        env.poll = poll
        with pytest.raises(
            ValueError, match="Your step function must return rewards that are"
        ):
            check_env(env)
        bad_done = {0: {0: "not_done", 1: False}}
        poll = MagicMock(return_value=(good_obs, good_reward, bad_done, good_info, {}))
        env.poll = poll
        with pytest.raises(
            ValueError,
            match="Your step function must return dones that are boolean.",
        ):
            check_env(env)
        bad_info = {0: {0: "not_info", 1: {}}}
        poll = MagicMock(return_value=(good_obs, good_reward, good_done, bad_info, {}))
        env.poll = poll
        with pytest.raises(
            ValueError,
            match="Your step function must return infos that are a dict.",
        ):
            check_env(env)

    def test_check_correct_env(self):
        env = self._make_base_env()
        check_env(env)
        env = gym.make("CartPole-v0")
        env = convert_to_base_env(env)
        check_env(env)


if __name__ == "__main__":
    pytest.main()
