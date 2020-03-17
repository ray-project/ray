import numpy as np
from tensorflow.python.eager.context import eager_mode
import unittest

import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check

tf = try_import_tf()


class TestDQN(unittest.TestCase):
    def test_dqn_compilation(self):
        """Test whether a DQNTrainer can be built with both frameworks."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        # tf.
        config["eager"] = False
        trainer = dqn.DQNTrainer(config=config, env="CartPole-v0")
        num_iterations = 2
        for i in range(num_iterations):
            results = trainer.train()
            print(results)

        config["eager"] = True
        trainer = dqn.DQNTrainer(config=config, env="CartPole-v0")
        num_iterations = 2
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
        for fw in ["tf", "eager", "torch"]:
            if fw == "torch":
                continue

            print("framework={}".format(fw))

            config["eager"] = fw == "eager"
            config["use_pytorch"] = fw == "torch"

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
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["env_config"] = {"is_slippery": False, "map_name": "4x4"}
        obs = np.array(0)

        # Test against all frameworks.
        for fw in ["eager", "tf", "torch"]:
            if fw == "torch":
                continue

            print("framework={}".format(fw))

            config["eager"] = True if fw == "eager" else False
            config["use_pytorch"] = True if fw == "torch" else False
            # Chose ParameterNoise as exploration class.
            config["exploration_config"] = {"type": "ParameterNoise"}
            config["explore"] = True

            eager_mode_ctx = eager_mode()
            if fw == "eager":
                eager_mode_ctx.__enter__()
            else:
                assert not tf.executing_eagerly()

            # DQN with ParameterNoise exploration (config["explore"]=True).
            # ----
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            policy = trainer.get_policy()
            weights_before = policy.get_weights()
            # Pseudo-start an episode and compare the weights before and after.
            policy.exploration.on_episode_start(
                policy, policy.model, None, None, tf_sess=policy._sess)
            weights_after = policy.get_weights()
            key = 0 if fw == "eager" else list(weights_before.keys())[0]
            noise = policy.exploration.noise[0][0][0]
            if fw == "tf":
                noise = policy._sess.run(noise)
            check(weights_before[key][0][0] + noise, weights_after[key][0][0])

            # Setting explore=False should always return the same action.
            a_ = trainer.compute_action(obs, explore=False)
            for _ in range(25):
                a = trainer.compute_action(obs, explore=False)
                check(a, a_)

            # Explore=None (default: explore) should return different actions.
            # However, this is only due to the underlying epsilon-greedy
            # exploration.
            actions = []
            for _ in range(25):
                actions.append(trainer.compute_action(obs))
            check(np.std(actions), 0.0, false=True)

            # Pseudo-end the episode and compare weights again.
            # Make sure they are the original ones.
            policy.exploration.on_episode_end(
                policy, policy.model, None, None, tf_sess=policy._sess)
            weights_after_episode_end = policy.get_weights()
            check(weights_before[key][0][0],
                  weights_after_episode_end[key][0][0])

            # DQN with ParameterNoise exploration (config["explore"]=False).
            # ----
            config["explore"] = False
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            policy = trainer.get_policy()
            weights_before = policy.get_weights()
            # Pseudo-start an episode and compare the weights before and after
            # (they should be the same).
            policy.exploration.on_episode_start(
                policy, policy.model, None, None, tf_sess=policy._sess)
            weights_after = policy.get_weights()
            check(weights_before[key][0][0], weights_after[key][0][0])

            # Setting explore=False or None should always return the same
            # action.
            a_ = trainer.compute_action(obs, explore=False)
            for _ in range(10):
                a = trainer.compute_action(obs, explore=None)
                check(a, a_)
                a = trainer.compute_action(obs, explore=False)
                check(a, a_)

            # Pseudo-end the episode and compare weights again.
            # Make sure they are the original ones (no noise permanently
            # applied throughout the episode).
            policy.exploration.on_episode_end(
                policy, policy.model, None, None, tf_sess=policy._sess)
            weights_after_episode_end = policy.get_weights()
            check(weights_before[key][0][0],
                  weights_after_episode_end[key][0][0])

            # Switch off EpsilonGreedy underlying exploration.
            # ----
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
            policy.exploration.on_episode_start(
                policy, policy.model, None, None, tf_sess=policy._sess)
            a_ = trainer.compute_action(obs)
            for _ in range(25):
                a = trainer.compute_action(obs, explore=True)
                check(a, a_)

            if fw == "eager":
                eager_mode_ctx.__exit__(None, None, None)


if __name__ == "__main__":
    import unittest
    unittest.main(verbosity=1)
