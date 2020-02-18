import numpy as np
from scipy.stats import norm
import unittest

import ray.rllib.agents.dqn as dqn
import ray.rllib.agents.ppo as ppo
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.numpy import one_hot, fc

tf = try_import_tf()


def test_log_likelihood(run, config, obs_batch, preprocessed_obs_batch,
                        prev_a=None, prev_r=None, continuous=False):
    config = config.copy()
    # Run locally.
    config["num_workers"] = 0
    # Env setup.
    if continuous:
        env = "Pendulum-v0"
    else:
        env = "FrozenLake-v0"
        config["env_config"] = {"is_slippery": False, "map_name": "4x4"}

    # Use Soft-Q for DQNs.
    if run is dqn.DQNTrainer:
        config["exploration_config"] = {"type": "SoftQ", "temperature": 0.5}

    # Test against all frameworks.
    for fw in ["tf", "eager", "torch"]:
        if run in [dqn.DQNTrainer] and fw == "torch":
            continue
        print("Testing {} with framework={}".format(run, fw))
        config["eager"] = True if fw == "eager" else False
        config["use_pytorch"] = True if fw == "torch" else False

        trainer = run(config=config, env=env)
        policy = trainer.get_policy()
        vars = policy.get_weights()
        # Sample n actions, then roughly check their logp against their
        # counts.
        num_actions = 500
        actions = []
        for _ in range(num_actions):
            # Single action from single obs.
            actions.append(trainer.compute_action(
                obs_batch[0], prev_action=prev_a, prev_reward=prev_r,
                explore=True))

        if continuous:
            # Test 50 actions for their log-likelihoods vs expected values.
            for idx in range(50):
                a = actions[idx]
                if fw == "tf":
                    expected_mean_logstd = fc(
                        fc(obs_batch, vars["default_policy/fc_1/kernel"],
                           vars["default_policy/fc_1/bias"]),
                        vars["default_policy/fc_out/kernel"],
                        vars["default_policy/fc_out/bias"])
                elif fw == "eager":
                    expected_mean_logstd = fc(
                        fc(obs_batch, vars[0], vars[1]), vars[4], vars[5])
                else:
                    print()
                    expected_mean_logstd = fc(
                        fc(obs_batch, vars["_hidden_layers.0._model.0.weight"],
                           vars["_hidden_layers.0._model.0.bias"]),
                        vars["_logits.0._model.0.weight"],
                        vars["_logits.0._model.0.bias"])
                mean, log_std = np.split(expected_mean_logstd, 2, axis=-1)
                expected_logp = np.log(norm.pdf(a, mean, np.exp(log_std)))
                logp = policy.compute_log_likelihood(
                    np.array([a]), preprocessed_obs_batch,
                    prev_action_batch=np.array([prev_a]),
                    prev_reward_batch=np.array([prev_r]))
                check(logp, expected_logp[0], rtol=0.2)
        else:
            # Test all available actions for their logp values.
            for a in [0, 1, 2, 3]:
                count = actions.count(a)
                expected_logp = np.log(count / num_actions)
                logp = policy.compute_log_likelihood(
                    np.array([a]), preprocessed_obs_batch,
                    prev_action_batch=np.array([prev_a]),
                    prev_reward_batch=np.array([prev_r]))
                check(logp, expected_logp, rtol=0.2)


class TestComputeLogLikelihood(unittest.TestCase):
    def test_dqn(self):
        """Tests, whether DQN correctly computes logp in soft-q mode."""
        obs_batch = np.array([0])
        preprocessed_obs = one_hot(obs_batch, depth=16)
        test_log_likelihood(dqn.DQNTrainer, dqn.DEFAULT_CONFIG, obs_batch,
                            preprocessed_obs)

    def test_ppo_cont(self):
        """Tests PPO's (cont. actions) compute_log_likelihood method."""
        obs_batch = preprocessed_obs_batch = np.array([[0.0, 0.1, -0.1]])
        config = ppo.DEFAULT_CONFIG.copy()
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = "linear"
        prev_a, prev_r = np.array([0.0]), np.array(0.0)
        test_log_likelihood(
            ppo.PPOTrainer, config,
            obs_batch, preprocessed_obs_batch,
            prev_a, prev_r, continuous=True)

    def test_ppo_discr(self):
        """Tests PPO's (discr. actions) compute_log_likelihood method."""
        obs_batch = np.array([0])
        preprocessed_obs_batch = one_hot(obs_batch, depth=16)
        prev_a, prev_r = np.array(0), np.array(0.0)
        test_log_likelihood(
            ppo.PPOTrainer, ppo.DEFAULT_CONFIG,
            obs_batch, preprocessed_obs_batch,
            prev_a, prev_r)
