import numpy as np
import unittest

import ray
import ray.rllib.agents.ddpg as ddpg
import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.test_utils import check, framework_iterator


class TestParameterNoise(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_ddpg_parameter_noise(self):
        self.do_test_parameter_noise_exploration(
            ddpg.DDPGTrainer,
            ddpg.DEFAULT_CONFIG,
            "Pendulum-v1",
            {},
            np.array([1.0, 0.0, -1.0]),
        )

    def test_dqn_parameter_noise(self):
        self.do_test_parameter_noise_exploration(
            dqn.DQNTrainer,
            dqn.DEFAULT_CONFIG,
            "FrozenLake-v1",
            {"is_slippery": False, "map_name": "4x4"},
            np.array(0),
        )

    def do_test_parameter_noise_exploration(
        self, trainer_cls, config, env, env_config, obs
    ):
        """Tests, whether an Agent works with ParameterNoise."""
        core_config = config.copy()
        core_config["num_workers"] = 0  # Run locally.
        core_config["env_config"] = env_config

        for fw in framework_iterator(core_config):
            config = core_config.copy()

            # Algo with ParameterNoise exploration (config["explore"]=True).
            # ----
            config["exploration_config"] = {"type": "ParameterNoise"}
            config["explore"] = True

            trainer = trainer_cls(config=config, env=env)
            policy = trainer.get_policy()
            pol_sess = policy.get_session()
            # Remove noise that has been added during policy initialization
            # (exploration.postprocess_trajectory does add noise to measure
            # the delta).
            policy.exploration._remove_noise(tf_sess=pol_sess)

            self.assertFalse(policy.exploration.weights_are_currently_noisy)
            noise_before = self._get_current_noise(policy, fw)
            check(noise_before, 0.0)
            initial_weights = self._get_current_weight(policy, fw)

            # Pseudo-start an episode and compare the weights before and after.
            policy.exploration.on_episode_start(policy, tf_sess=pol_sess)
            self.assertFalse(policy.exploration.weights_are_currently_noisy)
            noise_after_ep_start = self._get_current_noise(policy, fw)
            weights_after_ep_start = self._get_current_weight(policy, fw)
            # Should be the same, as we don't do anything at the beginning of
            # the episode, only one step later.
            check(noise_after_ep_start, noise_before)
            check(initial_weights, weights_after_ep_start)

            # Setting explore=False should always return the same action.
            a_ = trainer.compute_single_action(obs, explore=False)
            self.assertFalse(policy.exploration.weights_are_currently_noisy)
            noise = self._get_current_noise(policy, fw)
            # We sampled the first noise (not zero anymore).
            check(noise, 0.0, false=True)
            # But still not applied b/c explore=False.
            check(self._get_current_weight(policy, fw), initial_weights)
            for _ in range(10):
                a = trainer.compute_single_action(obs, explore=False)
                check(a, a_)
                # Noise never gets applied.
                check(self._get_current_weight(policy, fw), initial_weights)
                self.assertFalse(policy.exploration.weights_are_currently_noisy)

            # Explore=None (default: True) should return different actions.
            # However, this is only due to the underlying epsilon-greedy
            # exploration.
            actions = []
            current_weight = None
            for _ in range(10):
                actions.append(trainer.compute_single_action(obs))
                self.assertTrue(policy.exploration.weights_are_currently_noisy)
                # Now, noise actually got applied (explore=True).
                current_weight = self._get_current_weight(policy, fw)
                check(current_weight, initial_weights, false=True)
                check(current_weight, initial_weights + noise)
            check(np.std(actions), 0.0, false=True)

            # Pseudo-end the episode and compare weights again.
            # Make sure they are the original ones.
            policy.exploration.on_episode_end(policy, tf_sess=pol_sess)
            weights_after_ep_end = self._get_current_weight(policy, fw)
            check(current_weight - noise, weights_after_ep_end, decimals=5)
            trainer.stop()

            # DQN with ParameterNoise exploration (config["explore"]=False).
            # ----
            config = core_config.copy()
            config["exploration_config"] = {"type": "ParameterNoise"}
            config["explore"] = False
            trainer = trainer_cls(config=config, env=env)
            policy = trainer.get_policy()
            pol_sess = policy.get_session()
            # Remove noise that has been added during policy initialization
            # (exploration.postprocess_trajectory does add noise to measure
            # the delta).
            policy.exploration._remove_noise(tf_sess=pol_sess)

            self.assertFalse(policy.exploration.weights_are_currently_noisy)
            initial_weights = self._get_current_weight(policy, fw)

            # Noise before anything (should be 0.0, no episode started yet).
            noise = self._get_current_noise(policy, fw)
            check(noise, 0.0)

            # Pseudo-start an episode and compare the weights before and after
            # (they should be the same).
            policy.exploration.on_episode_start(policy, tf_sess=pol_sess)
            self.assertFalse(policy.exploration.weights_are_currently_noisy)

            # Should be the same, as we don't do anything at the beginning of
            # the episode, only one step later.
            noise = self._get_current_noise(policy, fw)
            check(noise, 0.0)
            noisy_weights = self._get_current_weight(policy, fw)
            check(initial_weights, noisy_weights)

            # Setting explore=False or None should always return the same
            # action.
            a_ = trainer.compute_single_action(obs, explore=False)
            # Now we have re-sampled.
            noise = self._get_current_noise(policy, fw)
            check(noise, 0.0, false=True)
            for _ in range(5):
                a = trainer.compute_single_action(obs, explore=None)
                check(a, a_)
                a = trainer.compute_single_action(obs, explore=False)
                check(a, a_)

            # Pseudo-end the episode and compare weights again.
            # Make sure they are the original ones (no noise permanently
            # applied throughout the episode).
            policy.exploration.on_episode_end(policy, tf_sess=pol_sess)
            weights_after_episode_end = self._get_current_weight(policy, fw)
            check(initial_weights, weights_after_episode_end)
            # Noise should still be the same (re-sampling only happens at
            # beginning of episode).
            noise_after = self._get_current_noise(policy, fw)
            check(noise, noise_after)
            trainer.stop()

            # Switch off underlying exploration entirely.
            # ----
            config = core_config.copy()
            if trainer_cls is dqn.DQNTrainer:
                sub_config = {
                    "type": "EpsilonGreedy",
                    "initial_epsilon": 0.0,  # <- no randomness whatsoever
                    "final_epsilon": 0.0,
                }
            else:
                sub_config = {
                    "type": "OrnsteinUhlenbeckNoise",
                    "initial_scale": 0.0,  # <- no randomness whatsoever
                    "final_scale": 0.0,
                    "random_timesteps": 0,
                }
            config["exploration_config"] = {
                "type": "ParameterNoise",
                "sub_exploration": sub_config,
            }
            config["explore"] = True
            trainer = trainer_cls(config=config, env=env)
            # Now, when we act - even with explore=True - we would expect
            # the same action for the same input (parameter noise is
            # deterministic).
            policy = trainer.get_policy()
            policy.exploration.on_episode_start(policy, tf_sess=pol_sess)
            a_ = trainer.compute_single_action(obs)
            for _ in range(10):
                a = trainer.compute_single_action(obs, explore=True)
                check(a, a_)
            trainer.stop()

    def _get_current_noise(self, policy, fw):
        # If noise not even created yet, return 0.0.
        if policy.exploration.noise is None:
            return 0.0

        noise = policy.exploration.noise[0][0][0]
        if fw == "tf":
            noise = policy.get_session().run(noise)
        elif fw == "torch":
            noise = noise.detach().cpu().numpy()
        else:
            noise = noise.numpy()
        return noise

    def _get_current_weight(self, policy, fw):
        weights = policy.get_weights()
        if fw == "torch":
            # DQN model.
            if "_hidden_layers.0._model.0.weight" in weights:
                return weights["_hidden_layers.0._model.0.weight"][0][0]
            # DDPG model.
            else:
                return weights["policy_model.action_0._model.0.weight"][0][0]
        key = 0 if fw in ["tf2", "tfe"] else list(weights.keys())[0]
        return weights[key][0][0]


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
