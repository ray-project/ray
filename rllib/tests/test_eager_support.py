import unittest

import ray
from ray import tune
from ray.rllib.agents.registry import get_agent_class


def check_support(alg, config):
    config["eager"] = True
    if alg in ["APEX_DDPG", "TD3", "DDPG", "SAC"]:
        config["env"] = "Pendulum-v0"
    else:
        config["env"] = "CartPole-v0"
    a = get_agent_class(alg)
    tune.run(a, config=config, stop={"training_iteration": 0})


class TestEagerSupport(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def testSimpleQ(self):
        check_support("SimpleQ", {"num_workers": 0, "learning_starts": 0})

    def testDQN(self):
        check_support("DQN", {"num_workers": 0, "learning_starts": 0})

    def testA2C(self):
        check_support("A2C", {"num_workers": 0})

    def testA3C(self):
        check_support("A3C", {"num_workers": 1})

    def testPG(self):
        check_support("PG", {"num_workers": 0})

    def testPPO(self):
        check_support("PPO", {"num_workers": 0})

    def testAPPO(self):
        check_support("APPO", {"num_workers": 1, "num_gpus": 0})

    def testIMPALA(self):
        check_support("IMPALA", {"num_workers": 1, "num_gpus": 0})

    def testAPEX_DQN(self):
        check_support(
            "APEX", {
                "num_workers": 2,
                "learning_starts": 0,
                "num_gpus": 0,
                "min_iter_time_s": 1,
                "timesteps_per_iteration": 100
            })

    def testDDPG(self):
        check_support("DDPG", {
            "num_workers": 0,
            "learning_starts": 0,
            "timesteps_per_iteration": 10
        })

    def testTD3(self):
        check_support("TD3", {
            "num_workers": 0,
            "learning_starts": 0,
            "timesteps_per_iteration": 10
        })

    def testAPEX_DDPG(self):
        check_support(
            "APEX_DDPG", {
                "num_workers": 2,
                "learning_starts": 0,
                "num_gpus": 0,
                "min_iter_time_s": 1,
                "timesteps_per_iteration": 100
            })

    def testSAC(self):
        check_support("SAC", {
            "num_workers": 0,
            "learning_starts": 0,
            "timesteps_per_iteration": 100
        })


if __name__ == "__main__":
    unittest.main(verbosity=2)
