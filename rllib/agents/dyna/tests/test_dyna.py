import copy
import gym
import numpy as np
import unittest

import ray
import ray.rllib.agents.dyna as dyna
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator

torch, _ = try_import_torch()


class TestDYNA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dyna_compilation(self):
        """Test whether a DYNATrainer can be built with both frameworks."""
        config = copy.deepcopy(dyna.DEFAULT_CONFIG)
        config["num_workers"] = 1
        config["train_batch_size"] = 1000
        num_iterations = 30
        env = "CartPole-v0"
        test_env = gym.make(env)

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = dyna.DYNATrainer(config=config, env=env)
            policy = trainer.get_policy()
            # Do n supervised epochs, each over `train_batch_size`.
            # Ignore validation loss here as a stopping criteria.
            for i in range(num_iterations):
                info = trainer.train()["info"]["learner"]["default_policy"]
                print("SL iteration: {}".format(i))
                print("train loss {}".format(info["dynamics_train_loss"]))
                print("validation loss {}".format(
                    info["dynamics_validation_loss"]))
            # Check, whether normal action stepping works with DYNA's policy.
            # Note that DYNA does not train its Policy. It must be pushed
            # down from the main model-based algo from time to time.
            check_compute_single_action(trainer)

            # Check, whether env dynamics were actually learnt - more or less.
            obs = test_env.reset()
            for _ in range(10):
                action = trainer.compute_action(obs)
                obs = torch.from_numpy(np.array([obs])).float()
                # Make the prediction over the next state (deterministic delta
                # like in MBMPO).
                predicted_next_obs_delta = \
                    policy.dynamics_model.get_next_observation(
                        obs,
                        torch.from_numpy(np.array([action])))
                predicted_next_obs = obs + predicted_next_obs_delta
                obs, _, done, _ = test_env.step(action)
                self.assertLess(
                    np.sum(obs - predicted_next_obs.detach().numpy()), 0.05)
                # Reset if done.
                if done:
                    obs = test_env.reset()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
