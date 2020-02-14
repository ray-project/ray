import numpy as np
import unittest

import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check

tf = try_import_tf()


class TestComputeLogLikelihood(unittest.TestCase):
    def test_dqn(self):
        """Tests, whether DQN correctly computes logp in soft-q mode."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["env_config"] = {"is_slippery": False, "map_name": "4x4"}
        config["exploration_config"] = {"type": "SoftQ", "temperature": 0.5}
        obs = np.array(0)

        # Test against all frameworks.
        for fw in ["tf", "eager", "torch"]:
            if fw == "torch":
                continue
            config["eager"] = True if fw == "eager" else False
            config["use_pytorch"] = True if fw == "torch" else False

            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            policy = trainer.get_policy()
            # Sample n actions, then roughly check their logp against their
            # counts.
            num_actions = 500
            actions = []
            for _ in range(num_actions):
                actions.append(trainer.compute_action(obs, explore=True))

            for a in [0, 1, 2, 3]:
                count = actions.count(a)
                expected_logp = np.log(count / num_actions)
                logp = policy.compute_log_likelihood(a, obs)
                check(logp, expected_logp)
