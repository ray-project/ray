import unittest

import ray
from ray import tune
from ray.rllib.agents.registry import get_agent_class


def check_support(alg, config, test_trace=True):
    config["framework"] = "tfe"
    # Test both continuous and discrete actions.
    for cont in [True, False]:
        if cont and alg in ["DQN", "APEX", "SimpleQ"]:
            continue
        elif not cont and alg in ["DDPG", "APEX_DDPG", "TD3"]:
            continue

        print("run={} cont. actions={}".format(alg, cont))

        if cont:
            config["env"] = "Pendulum-v0"
        else:
            config["env"] = "CartPole-v0"

        a = get_agent_class(alg)
        config["log_level"] = "ERROR"
        config["eager_tracing"] = False
        tune.run(a, config=config, stop={"training_iteration": 1})

        if test_trace:
            config["eager_tracing"] = True
            tune.run(a, config=config, stop={"training_iteration": 1})


class TestEagerSupport(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4, local_mode=True)

    def tearDown(self):
        ray.shutdown()

    def test_simple_q(self):
        check_support("SimpleQ", {"num_workers": 0, "learning_starts": 0})

    def test_dqn(self):
        check_support("DQN", {"num_workers": 0, "learning_starts": 0})

    # TODO(sven): Add these once DDPG supports eager.
    # def test_ddpg(self):
    #     check_support("DDPG", {"num_workers": 0})

    # def test_apex_ddpg(self):
    #     check_support("APEX_DDPG", {"num_workers": 1})

    # def test_td3(self):
    #     check_support("TD3", {"num_workers": 0})

    def test_a2c(self):
        check_support("A2C", {"num_workers": 0})

    def test_a3c(self):
        check_support("A3C", {"num_workers": 1})

    def test_pg(self):
        check_support("PG", {"num_workers": 0})

    def test_ppo(self):
        check_support("PPO", {"num_workers": 0})

    def test_appo(self):
        check_support("APPO", {"num_workers": 1, "num_gpus": 0})

    def test_impala(self):
        check_support("IMPALA", {"num_workers": 1, "num_gpus": 0})

    def test_apex_dqn(self):
        check_support(
            "APEX", {
                "num_workers": 2,
                "learning_starts": 0,
                "num_gpus": 0,
                "min_iter_time_s": 1,
                "timesteps_per_iteration": 100,
                "optimizer": {
                    "num_replay_buffer_shards": 1,
                },
            })

    # TODO(sven): Add this once SAC supports eager.
    # def test_sac(self):
    #    check_support("SAC", {"num_workers": 0, "learning_starts": 0})


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
