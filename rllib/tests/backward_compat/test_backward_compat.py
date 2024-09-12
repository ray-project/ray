import sys
import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn import DQN
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.policy import PolicySpec
from ray.tune.registry import register_env


class TestBackwardCompatibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(runtime_env={"pip_packages": ["gym==0.23.1"]})

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_old_algorithm_config_dicts(self):
        """Tests, whether we can build Algorithm objects with old config dicts."""

        config_dict = {
            "evaluation_config": {
                "lr": 0.1,
            },
            "lr": 0.2,
            # Old-style multi-agent dict.
            "multiagent": {
                "policies": {"pol1", "pol2"},
                "policies_to_train": ["pol1"],
                "policy_mapping_fn": lambda aid, episode, worker, **kwargs: "pol1",
            },
            # Test, whether both keys (that map to the same new key) still work.
            "num_workers": 2,
            "num_rollout_workers": 2,
            # Resource settings.
            "num_cpus_for_local_worker": 2,
            "num_cpus_per_learner_worker": 3,
            "num_gpus_per_learner_worker": 4,
            "num_learner_workers": 5,
        }
        config = AlgorithmConfig.from_dict(config_dict)
        self.assertFalse(config.in_evaluation)
        self.assertTrue(config.lr == 0.2)
        self.assertTrue(set(config.policies.keys()) == {"pol1", "pol2"})
        self.assertTrue(config.policy_mapping_fn(1, 2, 3) == "pol1")
        eval_config = config.get_evaluation_config_object()
        self.assertTrue(eval_config.in_evaluation)
        self.assertTrue(eval_config.lr == 0.1)
        self.assertTrue(config.num_env_runners == 2)
        self.assertTrue(config.num_cpus_for_main_process == 2)
        self.assertTrue(config.num_cpus_per_learner == 3)
        self.assertTrue(config.num_gpus_per_learner == 4)
        self.assertTrue(config.num_learners == 5)

        register_env(
            "test",
            lambda ctx: MultiAgentCartPole(config={"num_agents": ctx["num_agents"]}),
        )

        config = {
            "env": "test",
            "env_config": {
                "num_agents": 1,
            },
            "lr": 0.001,
            "evaluation_config": {
                "num_envs_per_worker": 4,  # old key -> num_envs_per_env_runner
                "explore": False,
            },
            "evaluation_num_workers": 1,  # old key -> evaluation_num_env_runners
            "multiagent": {
                "policies": {
                    "policy1": PolicySpec(),
                },
                "policy_mapping_fn": lambda aid, episode, worker, **kw: "policy1",
                "policies_to_train": ["policy1"],
            },
        }
        algo = DQN(config=config)
        self.assertTrue(algo.config.lr == 0.001)
        self.assertTrue(algo.config.evaluation_num_env_runners == 1)
        self.assertTrue(list(algo.config.policies.keys()) == ["policy1"])
        self.assertTrue(algo.config.explore is True)
        self.assertTrue(algo.evaluation_config.explore is False)
        print(algo.train())
        algo.stop()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
