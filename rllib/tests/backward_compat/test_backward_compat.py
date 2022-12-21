import os
import importlib
from pathlib import Path
from packaging import version
import sys
import unittest

import ray
import ray.cloudpickle as pickle
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPO
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.utils.checkpoints import get_checkpoint_info
from ray.rllib.utils.test_utils import framework_iterator
from ray.tune.registry import register_env


class TestBackwardCompatibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.system("pip install gym==0.23.1")
        importlib.reload(sys.modules["gym"])
        ray.init()

    @classmethod
    def tearDownClass(cls):
        os.system("pip install gym==0.26.2")
        ray.shutdown()

    def test_old_checkpoint_formats(self):
        """Tests, whether we remain backward compatible (>=2.0.0) wrt checkpoints."""

        rllib_dir = Path(__file__).parent.parent.parent
        print(f"rllib dir={rllib_dir} exists={os.path.isdir(rllib_dir)}")

        # TODO: Once checkpoints are python version independent (once we stop using
        #  pickle), add 1.0 here as well.
        # Broken due to gymnasium move (old gym envs not recoverable via pickle due to
        # gym version conflict (gym==0.23.x not compatible with gym==0.26.x)).
        for v in []:  # "0.1"
            v = version.Version(v)
            for fw in framework_iterator(with_eager_tracing=True):
                path_to_checkpoint = os.path.join(
                    rllib_dir,
                    "tests",
                    "backward_compat",
                    "checkpoints",
                    "v" + str(v),
                    "ppo_frozenlake_" + fw,
                )

                print(
                    f"path_to_checkpoint={path_to_checkpoint} "
                    f"exists={os.path.isdir(path_to_checkpoint)}"
                )

                checkpoint_info = get_checkpoint_info(path_to_checkpoint)
                # v0.1: Need to create algo first, then restore.
                if checkpoint_info["checkpoint_version"] == version.Version("0.1"):
                    # For checkpoints <= v0.1, we need to magically know the original
                    # config used as well as the algo class.
                    with open(checkpoint_info["state_file"], "rb") as f:
                        state = pickle.load(f)
                    worker_state = pickle.loads(state["worker"])
                    algo = PPO(config=worker_state["policy_config"])
                    # Note, we can not use restore() here because the testing
                    # checkpoints are created with Algorithm.save() by
                    # checkpoints/create_checkpoints.py. I.e, they are missing
                    # all the Tune checkpoint metadata.
                    algo.load_checkpoint(path_to_checkpoint)
                # > v0.1: Simply use new `Algorithm.from_checkpoint()` staticmethod.
                else:
                    algo = Algorithm.from_checkpoint(path_to_checkpoint)

                    # Also test restoring a Policy from an algo checkpoint.
                    policies = Policy.from_checkpoint(path_to_checkpoint)
                    self.assertTrue("default_policy" in policies)

                print(algo.train())
                algo.stop()

    def test_v1_policy_from_checkpoint(self):
        """Tests, whether we can load Policy checkpoints for different frameworks."""

        # We wouldn't need this test once we get rid of V1 policy implementations.

        rllib_dir = Path(__file__).parent.parent.parent
        print(f"rllib dir={rllib_dir} exists={os.path.isdir(rllib_dir)}")

        for fw in framework_iterator(with_eager_tracing=True):
            path_to_checkpoint = os.path.join(
                rllib_dir,
                "tests",
                "backward_compat",
                "checkpoints",
                "v1.0",
                "dqn_frozenlake_" + fw,
                "policies",
                "default_policy",
            )

            print(
                f"path_to_checkpoint={path_to_checkpoint} "
                f"exists={os.path.isdir(path_to_checkpoint)}"
            )

            policy = Policy.from_checkpoint(path_to_checkpoint)
            self.assertTrue(isinstance(policy, Policy))

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
        }
        config = AlgorithmConfig.from_dict(config_dict)
        self.assertFalse(config.in_evaluation)
        self.assertTrue(config.lr == 0.2)
        self.assertTrue(config.policies == {"pol1", "pol2"})
        self.assertTrue(config.policy_mapping_fn(1, 2, 3) == "pol1")
        eval_config = config.get_evaluation_config_object()
        self.assertTrue(eval_config.in_evaluation)
        self.assertTrue(eval_config.lr == 0.1)

        register_env(
            "test",
            lambda ctx: MultiAgentCartPole(config={"num_agents": ctx["num_agents"]}),
        )

        config = {
            "env_config": {
                "num_agents": 1,
            },
            "lr": 0.001,
            "evaluation_config": {
                "num_envs_per_worker": 4,
                "explore": False,
            },
            "evaluation_num_workers": 1,
            "multiagent": {
                "policies": {
                    "policy1": PolicySpec(),
                },
                "policy_mapping_fn": lambda aid, episode, worker, **kw: "policy1",
                "policies_to_train": ["policy1"],
            },
        }
        algo = PPO(config=config, env="test")
        self.assertTrue(algo.config.lr == 0.001)
        self.assertTrue(algo.config.evaluation_num_workers == 1)
        self.assertTrue(list(algo.config.policies.keys()) == ["policy1"])
        self.assertTrue(algo.config.explore is True)
        self.assertTrue(algo.evaluation_config.explore is False)
        print(algo.train())
        algo.stop()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
