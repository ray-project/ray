from rllib.offline.estimators.utils import lookup_state_value_fn, lookup_action_value_fn
import unittest
import ray
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.algorithms import Algorithm
from ray.rllib.policy import Policy
from ray.rllib.utils.test_utils import framework_iterator
import os
from pathlib import Path

DISCRETE = ["DQN", "APEX", "CRR", "SAC"]#, "SimpleQ"]
CONTINUOUS = ["APEX_DDPG", "DDPG", "TD3", "CRR", "SAC", "CQL"]
rllib_dir = Path(__file__).parent.parent.parent.parent
print("rllib dir={}".format(rllib_dir))
cartpole_data = os.path.join(rllib_dir, "tests/data/cartpole/small.json")
pendulum_data = os.path.join(rllib_dir, "tests/data/pendulum/small.json")


class TestValueFnLookup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_discrete_action_value_fn(self):
        for algo_name in DISCRETE:
            algo_class, config = get_algorithm_class(algo_name, return_config=True)
            config["env"] = "CartPole-v0"
            config["input"] = cartpole_data
            config["num_workers"] = 0
            frameworks = ["tf2", "torch"] if algo_name != "CRR" else ["torch"]
            for framework in framework_iterator(config, frameworks=frameworks):
                print(algo_name + framework)
                with self.subTest(algo_name + framework):
                    algo: Algorithm = algo_class(config=config)
                    policy: Policy = algo.get_policy()
                    action_value_fn = lookup_action_value_fn(policy)
                    batch = algo.workers.local_worker().sample()
                    q_values = action_value_fn(policy, batch)
                    assert q_values.shape == (batch.count,)

    def test_discrete_state_value_fn(self):
        for algo_name in DISCRETE:
            algo_class, config = get_algorithm_class(algo_name, return_config=True)
            config["env"] = "CartPole-v0"
            config["input"] = cartpole_data
            config["num_workers"] = 0
            frameworks = ["tf2", "torch"] if algo_name != "CRR" else ["torch"]
            for framework in framework_iterator(config, frameworks=frameworks):
                print(algo_name + framework)
                with self.subTest(algo_name + framework):
                    algo: Algorithm = algo_class(config=config)
                    policy: Policy = algo.get_policy()
                    state_value_fn = lookup_state_value_fn(policy)
                    batch = algo.workers.local_worker().sample()
                    v_values = state_value_fn(policy, batch)
                    assert v_values.shape == (batch.count,)

    def test_continuous_action_value_fn(self):
        for algo_name in CONTINUOUS:
            algo_class, config = get_algorithm_class(algo_name, return_config=True)
            config["env"] = "Pendulum-v1"
            config["input"] = pendulum_data
            config["num_workers"] = 0
            frameworks = ["tf2", "torch"] if algo_name != "CRR" else ["torch"]
            for framework in framework_iterator(config, frameworks=frameworks):
                print(algo_name + framework)
                with self.subTest(algo_name + framework):
                    algo: Algorithm = algo_class(config=config)
                    policy: Policy = algo.get_policy()
                    action_value_fn = lookup_action_value_fn(policy)
                    batch = algo.workers.local_worker().sample()
                    q_values = action_value_fn(policy, batch)
                    assert q_values.shape == (batch.count,)

    def test_continuous_state_value_fn(self):
        for algo_name in CONTINUOUS:
            algo_class, config = get_algorithm_class(algo_name, return_config=True)
            config["env"] = "Pendulum-v1"
            config["input"] = pendulum_data
            config["num_workers"] = 0
            frameworks = ["tf2", "torch"] if algo_name != "CRR" else ["torch"]
            for framework in framework_iterator(config, frameworks=frameworks):
                print(algo_name + framework)
                with self.subTest(algo_name + framework):
                    algo: Algorithm = algo_class(config=config)
                    policy: Policy = algo.get_policy()
                    state_value_fn = lookup_state_value_fn(policy)
                    batch = algo.workers.local_worker().sample()
                    v_values = state_value_fn(policy, batch)
                    assert v_values.shape == (batch.count,)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
