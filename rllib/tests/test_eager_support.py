import unittest

import ray
from ray import tune
from ray.rllib.agents.registry import get_trainer_class
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


def check_support(alg, config, test_eager=False, test_trace=True):
    config["framework"] = "tfe"
    config["log_level"] = "ERROR"
    # Test both continuous and discrete actions.
    for cont in [True, False]:
        if cont and alg in ["DQN", "APEX", "SimpleQ"]:
            continue
        elif not cont and alg in ["DDPG", "APEX_DDPG", "TD3"]:
            continue

        if cont:
            config["env"] = "Pendulum-v1"
        else:
            config["env"] = "CartPole-v0"

        a = get_trainer_class(alg)
        if test_eager:
            print("tf-eager: alg={} cont.act={}".format(alg, cont))
            config["eager_tracing"] = False
            tune.run(a, config=config, stop={"training_iteration": 1}, verbose=1)
        if test_trace:
            config["eager_tracing"] = True
            print("tf-eager-tracing: alg={} cont.act={}".format(alg, cont))
            tune.run(a, config=config, stop={"training_iteration": 1}, verbose=1)


class TestEagerSupportPG(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_simple_q(self):
        check_support("SimpleQ", {"num_workers": 0, "learning_starts": 0})

    def test_dqn(self):
        check_support("DQN", {"num_workers": 0, "learning_starts": 0})

    def test_ddpg(self):
        check_support("DDPG", {"num_workers": 0})

    # TODO(sven): Add these once APEX_DDPG supports eager.
    # def test_apex_ddpg(self):
    #     check_support("APEX_DDPG", {"num_workers": 1})

    def test_td3(self):
        check_support("TD3", {"num_workers": 0})

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
        check_support("IMPALA", {"num_workers": 1, "num_gpus": 0}, test_eager=True)


class TestEagerSupportOffPolicy(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_simple_q(self):
        check_support("SimpleQ", {"num_workers": 0, "learning_starts": 0})

    def test_dqn(self):
        check_support("DQN", {"num_workers": 0, "learning_starts": 0})

    def test_ddpg(self):
        check_support("DDPG", {"num_workers": 0})

    # def test_apex_ddpg(self):
    #     check_support("APEX_DDPG", {"num_workers": 1})

    def test_td3(self):
        check_support("TD3", {"num_workers": 0})

    def test_apex_dqn(self):
        check_support(
            "APEX",
            {
                "num_workers": 2,
                "learning_starts": 0,
                "num_gpus": 0,
                "min_time_s_per_reporting": 1,
                "min_sample_timesteps_per_reporting": 100,
                "optimizer": {
                    "num_replay_buffer_shards": 1,
                },
            },
        )

    def test_sac(self):
        check_support("SAC", {"num_workers": 0, "learning_starts": 0})


if __name__ == "__main__":
    import sys

    # Don't test anything for version 2.x (all tests are eager anyways).
    # TODO: (sven) remove entire file in the future.
    if tfv == 2:
        print("\tskip due to tf==2.x")
        sys.exit(0)

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    import pytest

    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
