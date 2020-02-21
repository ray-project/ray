import numpy as np
from scipy.stats import norm
import unittest

import ray.rllib.agents.dqn as dqn
import ray.rllib.agents.ppo as ppo
import ray.rllib.agents.sac as sac
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.numpy import one_hot, fc, MIN_LOG_NN_OUTPUT, \
    MAX_LOG_NN_OUTPUT

tf = try_import_tf()


def test_log_likelihood(run,
                        config,
                        prev_a=None,
                        continuous=False,
                        layer_key=("fc", (0, 4)),
                        logp_func=None):
    config = config.copy()
    # Run locally.
    config["num_workers"] = 0
    # Env setup.
    if continuous:
        env = "Pendulum-v0"
        obs_batch = preprocessed_obs_batch = np.array([[0.0, 0.1, -0.1]])
    else:
        env = "FrozenLake-v0"
        config["env_config"] = {"is_slippery": False, "map_name": "4x4"}
        obs_batch = np.array([0])
        preprocessed_obs_batch = one_hot(obs_batch, depth=16)

    # Use Soft-Q for DQNs.
    if run is dqn.DQNTrainer:
        config["exploration_config"] = {"type": "SoftQ", "temperature": 0.5}

    prev_r = None if prev_a is None else np.array(0.0)

    # Test against all frameworks.
    for fw in ["tf", "eager", "torch"]:
        if run in [dqn.DQNTrainer, sac.SACTrainer] and fw == "torch":
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
            actions.append(
                trainer.compute_action(
                    obs_batch[0],
                    prev_action=prev_a,
                    prev_reward=prev_r,
                    explore=True))

        # Test 50 actions for their log-likelihoods vs expected values.
        if continuous:
            for idx in range(50):
                a = actions[idx]
                if fw == "tf" or fw == "eager":
                    if isinstance(vars, list):
                        expected_mean_logstd = fc(
                            fc(obs_batch, vars[layer_key[1][0]]),
                            vars[layer_key[1][1]])
                    else:
                        expected_mean_logstd = fc(
                            fc(
                                obs_batch,
                                vars["default_policy/{}_1/kernel".format(
                                    layer_key[0])]),
                            vars["default_policy/{}_out/kernel".format(
                                layer_key[0])])
                else:
                    expected_mean_logstd = fc(
                        fc(obs_batch,
                           vars["_hidden_layers.0._model.0.weight"]),
                        vars["_logits._model.0.weight"])
                mean, log_std = np.split(expected_mean_logstd, 2, axis=-1)
                if logp_func is None:
                    expected_logp = np.log(norm.pdf(a, mean, np.exp(log_std)))
                else:
                    expected_logp = logp_func(mean, log_std, a)
                logp = policy.compute_log_likelihoods(
                    np.array([a]),
                    preprocessed_obs_batch,
                    prev_action_batch=np.array([prev_a]),
                    prev_reward_batch=np.array([prev_r]))
                check(logp, expected_logp[0], rtol=0.2)
        # Test all available actions for their logp values.
        else:
            for a in [0, 1, 2, 3]:
                count = actions.count(a)
                expected_logp = np.log(count / num_actions)
                logp = policy.compute_log_likelihoods(
                    np.array([a]),
                    preprocessed_obs_batch,
                    prev_action_batch=np.array([prev_a]),
                    prev_reward_batch=np.array([prev_r]))
                check(logp, expected_logp, rtol=0.3)


class TestComputeLogLikelihood(unittest.TestCase):
    def test_dqn(self):
        """Tests, whether DQN correctly computes logp in soft-q mode."""
        test_log_likelihood(dqn.DQNTrainer, dqn.DEFAULT_CONFIG)

    def test_ppo_cont(self):
        """Tests PPO's (cont. actions) compute_log_likelihoods method."""
        config = ppo.DEFAULT_CONFIG.copy()
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = "linear"
        prev_a = np.array([0.0])
        test_log_likelihood(ppo.PPOTrainer, config, prev_a, continuous=True)

    def test_ppo_discr(self):
        """Tests PPO's (discr. actions) compute_log_likelihoods method."""
        prev_a = np.array(0)
        test_log_likelihood(ppo.PPOTrainer, ppo.DEFAULT_CONFIG, prev_a)

    def test_sac(self):
        """Tests SAC's compute_log_likelihoods method."""
        config = sac.DEFAULT_CONFIG.copy()
        config["policy_model"]["hidden_layer_sizes"] = [10]
        config["policy_model"]["hidden_activation"] = "linear"
        prev_a = np.array([0.0])

        def logp_func(means, log_stds, values, low=-1.0, high=1.0):
            stds = np.exp(
                np.clip(log_stds, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT))
            unsquashed_values = np.arctanh((values - low) /
                                           (high - low) * 2.0 - 1.0)
            log_prob_unsquashed = \
                np.sum(np.log(norm.pdf(unsquashed_values, means, stds)), -1)
            return log_prob_unsquashed - \
                np.sum(np.log(1 - np.tanh(unsquashed_values) ** 2),
                       axis=-1)

        test_log_likelihood(
            sac.SACTrainer,
            config,
            prev_a,
            continuous=True,
            layer_key=("sequential/action", (0, 2)),
            logp_func=logp_func)
