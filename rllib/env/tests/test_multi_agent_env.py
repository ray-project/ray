import pytest
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.tests.test_nested_observation_spaces import NestedMultiAgentEnv


class TestMultiAgentEnv:
    def test_space_in_preferred_format(self):
        env = NestedMultiAgentEnv()
        spaces_in_preferred_format = \
            env._check_if_space_maps_agent_id_to_sub_space()
        assert spaces_in_preferred_format, "Space is not in preferred " \
                                           "format"
        env2 = make_multi_agent("CartPole-v1")()
        spaces_in_preferred_format = \
            env2._check_if_space_maps_agent_id_to_sub_space()
        assert not spaces_in_preferred_format, "Space should not be in " \
                                               "preferred format but is."

    def test_spaces_sample_contain_in_preferred_format(self):
        env = NestedMultiAgentEnv()
        # this environment has spaces that are in the preferred format
        # for multi-agent environments where the spaces are dict spaces
        # mapping agent-ids to sub-spaces
        obs = env.observation_space_sample()
        assert env.observation_space_contains(obs), "Observation space does " \
                                                    "not contain obs"

        action = env.action_space_sample()
        assert env.action_space_contains(action), "Action space does " \
                                                  "not contain action"

    def test_spaces_sample_contain_not_in_preferred_format(self):
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        # this environment has spaces that are not in the preferred format
        # for multi-agent environments where the spaces not in the preferred
        # format, users must override the observation_space_contains,
        # action_space_contains observation_space_sample,
        # and action_space_sample methods in order to do proper checks
        obs = env.observation_space_sample()
        assert env.observation_space_contains(obs), "Observation space does " \
                                                    "not contain obs"
        action = env.action_space_sample()
        assert env.action_space_contains(action), "Action space does " \
                                                  "not contain action"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
