import unittest
from copy import deepcopy

import ray
from ray.tune.registry import register_env
from ray.rllib.env import PettingZooEnv
from ray.rllib.agents.registry import get_trainer_class

from pettingzoo.mpe import simple_spread_v2


class TestPettingZooEnv(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_pettingzoo_env(self):
        register_env("simple_spread",
                     lambda _: PettingZooEnv(simple_spread_v2.env()))

        agent_class = get_trainer_class("PPO")

        config = deepcopy(agent_class._default_config)

        test_env = PettingZooEnv(simple_spread_v2.env())
        obs_space = test_env.observation_space
        act_space = test_env.action_space
        test_env.close()

        config["multiagent"] = {
            "policies": {
                # the first tuple value is None -> uses default policy
                "av": (None, obs_space, act_space, {}),
            },
            "policy_mapping_fn": lambda agent_id: "av"
        }

        config["log_level"] = "DEBUG"
        config["num_workers"] = 0
        config["rollout_fragment_length"] = 30
        config["train_batch_size"] = 200
        config["horizon"] = 200  # After n steps, force reset simulation
        config["no_done_at_end"] = False

        agent = agent_class(env="simple_spread", config=config)
        agent.train()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
