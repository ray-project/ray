from ray.rllib.utils.pre_checks import check_gym_environments

import pytest
import gym

from ray.rllib.utils.pre_checks import check_gym_environments


class TestGymCheckEnv():
    def test_has_observation_space(self):
        env = gym.make("CartPole-v1")
        del env.observation_space
        with pytest.raises(AttributeError):
            check_gym_environments(env)

    def test_has_action_space(self):
        env = gym.make("CartPole-v1")
        del env.observation_space
        with pytest.raises(AttributeError):
            check_gym_environments(env)


if __name__ == '__main__':
    pytest.main()
