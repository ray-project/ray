import numpy as np
import unittest

import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check, framework_iterator

tf = try_import_tf()


class TestDQN(unittest.TestCase):
    def test_dqn_compilation(self):
        """Test whether a DQNTrainer can be built with both frameworks."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        num_iterations = 2

        for _ in framework_iterator(config, frameworks=["tf", "eager"]):
            # Rainbow.
            rainbow_config = config.copy()
            rainbow_config["num_atoms"] = 10
            rainbow_config["noisy"] = True
            rainbow_config["double_q"] = True
            rainbow_config["dueling"] = True
            rainbow_config["n_step"] = 5
            trainer = dqn.DQNTrainer(config=rainbow_config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                print(results)

            # double-dueling DQN.
            plain_config = config.copy()
            trainer = dqn.DQNTrainer(config=plain_config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                print(results)

    def test_dqn_exploration_and_soft_q_config(self):
        """Tests, whether a DQN Agent outputs exploration/softmaxed actions."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["env_config"] = {"is_slippery": False, "map_name": "4x4"}
        obs = np.array(0)

        # Test against all frameworks.
        for _ in framework_iterator(config, ["tf", "eager"]):
            # Default EpsilonGreedy setup.
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            # Setting explore=False should always return the same action.
            a_ = trainer.compute_action(obs, explore=False)
            for _ in range(50):
                a = trainer.compute_action(obs, explore=False)
                check(a, a_)
            # explore=None (default: explore) should return different actions.
            actions = []
            for _ in range(50):
                actions.append(trainer.compute_action(obs))
            check(np.std(actions), 0.0, false=True)

            # Low softmax temperature. Behaves like argmax
            # (but no epsilon exploration).
            config["exploration_config"] = {
                "type": "SoftQ",
                "temperature": 0.001
            }
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            # Due to the low temp, always expect the same action.
            a_ = trainer.compute_action(obs)
            for _ in range(50):
                a = trainer.compute_action(obs)
                check(a, a_)

            # Higher softmax temperature.
            config["exploration_config"]["temperature"] = 1.0
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")

            # Even with the higher temperature, if we set explore=False, we
            # should expect the same actions always.
            a_ = trainer.compute_action(obs, explore=False)
            for _ in range(50):
                a = trainer.compute_action(obs, explore=False)
                check(a, a_)

            # Due to the higher temp, expect different actions avg'ing
            # around 1.5.
            actions = []
            for _ in range(300):
                actions.append(trainer.compute_action(obs))
            check(np.std(actions), 0.0, false=True)

            # With Random exploration.
            config["exploration_config"] = {"type": "Random"}
            config["explore"] = True
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            actions = []
            for _ in range(300):
                actions.append(trainer.compute_action(obs))
            check(np.std(actions), 0.0, false=True)

    def test_dqn_parameter_noise_exploration(self):
        """Tests, whether a DQN Agent works with ParameterNoise."""
        obs = np.array(0)
        core_config = dqn.DEFAULT_CONFIG.copy()
        core_config["num_workers"] = 0  # Run locally.
        core_config["env_config"] = {"is_slippery": False, "map_name": "4x4"}

        for fw in framework_iterator(core_config, ["tf", "eager"]):

            config = core_config.copy()

            # DQN with ParameterNoise exploration (config["explore"]=True).
            # ----
            config["exploration_config"] = {"type": "ParameterNoise"}
            config["explore"] = True

            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            policy = trainer.get_policy()
            self.assertFalse(policy.exploration.weights_are_currently_noisy)
            noise_before = self._get_current_noise(policy, fw)
            check(noise_before, 0.0)
            initial_weights = self._get_current_weight(policy, fw)

            # Pseudo-start an episode and compare the weights before and after.
            policy.exploration.on_episode_start(policy, tf_sess=policy._sess)
            self.assertFalse(policy.exploration.weights_are_currently_noisy)
            noise_after_ep_start = self._get_current_noise(policy, fw)
            weights_after_ep_start = self._get_current_weight(policy, fw)
            # Should be the same, as we don't do anything at the beginning of
            # the episode, only one step later.
            check(noise_after_ep_start, noise_before)
            check(initial_weights, weights_after_ep_start)

            # Setting explore=False should always return the same action.
            a_ = trainer.compute_action(obs, explore=False)
            self.assertFalse(policy.exploration.weights_are_currently_noisy)
            noise = self._get_current_noise(policy, fw)
            # We sampled the first noise (not zero anymore).
            check(noise, 0.0, false=True)
            # But still not applied b/c explore=False.
            check(self._get_current_weight(policy, fw), initial_weights)
            for _ in range(10):
                a = trainer.compute_action(obs, explore=False)
                check(a, a_)
                # Noise never gets applied.
                check(self._get_current_weight(policy, fw), initial_weights)
                self.assertFalse(
                    policy.exploration.weights_are_currently_noisy)

            # Explore=None (default: True) should return different actions.
            # However, this is only due to the underlying epsilon-greedy
            # exploration.
            actions = []
            current_weight = None
            for _ in range(10):
                actions.append(trainer.compute_action(obs))
                self.assertTrue(policy.exploration.weights_are_currently_noisy)
                # Now, noise actually got applied (explore=True).
                current_weight = self._get_current_weight(policy, fw)
                check(current_weight, initial_weights, false=True)
                check(current_weight, initial_weights + noise)
            check(np.std(actions), 0.0, false=True)

            # Pseudo-end the episode and compare weights again.
            # Make sure they are the original ones.
            policy.exploration.on_episode_end(policy, tf_sess=policy._sess)
            weights_after_ep_end = self._get_current_weight(policy, fw)
            check(current_weight - noise, weights_after_ep_end, decimals=5)

            # DQN with ParameterNoise exploration (config["explore"]=False).
            # ----
            config = core_config.copy()
            config["exploration_config"] = {"type": "ParameterNoise"}
            config["explore"] = False
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            policy = trainer.get_policy()
            self.assertFalse(policy.exploration.weights_are_currently_noisy)
            initial_weights = self._get_current_weight(policy, fw)

            # Noise before anything (should be 0.0, no episode started yet).
            noise = self._get_current_noise(policy, fw)
            check(noise, 0.0)

            # Pseudo-start an episode and compare the weights before and after
            # (they should be the same).
            policy.exploration.on_episode_start(policy, tf_sess=policy._sess)
            self.assertFalse(policy.exploration.weights_are_currently_noisy)

            # Should be the same, as we don't do anything at the beginning of
            # the episode, only one step later.
            noise = self._get_current_noise(policy, fw)
            check(noise, 0.0)
            noisy_weights = self._get_current_weight(policy, fw)
            check(initial_weights, noisy_weights)

            # Setting explore=False or None should always return the same
            # action.
            a_ = trainer.compute_action(obs, explore=False)
            # Now we have re-sampled.
            noise = self._get_current_noise(policy, fw)
            check(noise, 0.0, false=True)
            for _ in range(5):
                a = trainer.compute_action(obs, explore=None)
                check(a, a_)
                a = trainer.compute_action(obs, explore=False)
                check(a, a_)

            # Pseudo-end the episode and compare weights again.
            # Make sure they are the original ones (no noise permanently
            # applied throughout the episode).
            policy.exploration.on_episode_end(policy, tf_sess=policy._sess)
            weights_after_episode_end = self._get_current_weight(policy, fw)
            check(initial_weights, weights_after_episode_end)
            # Noise should still be the same (re-sampling only happens at
            # beginning of episode).
            noise_after = self._get_current_noise(policy, fw)
            check(noise, noise_after)

            # Switch off EpsilonGreedy underlying exploration.
            # ----
            config = core_config.copy()
            config["exploration_config"] = {
                "type": "ParameterNoise",
                "sub_exploration": {
                    "type": "EpsilonGreedy",
                    "action_space": trainer.get_policy().action_space,
                    "initial_epsilon": 0.0,  # <- no randomness whatsoever
                }
            }
            config["explore"] = True
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            # Now, when we act - even with explore=True - we would expect
            # the same action for the same input (parameter noise is
            # deterministic).
            policy = trainer.get_policy()
            policy.exploration.on_episode_start(policy, tf_sess=policy._sess)
            a_ = trainer.compute_action(obs)
            for _ in range(10):
                a = trainer.compute_action(obs, explore=True)
                check(a, a_)

    def _get_current_noise(self, policy, fw):
        # If noise not even created yet, return 0.0.
        if policy.exploration.noise is None:
            return 0.0

        noise = policy.exploration.noise[0][0][0]
        if fw == "tf":
            noise = policy.get_session().run(noise)
        else:
            noise = noise.numpy()
        return noise

    def _get_current_weight(self, policy, fw):
        weights = policy.get_weights()
        key = 0 if fw == "eager" else list(weights.keys())[0]
        return weights[key][0][0]


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
