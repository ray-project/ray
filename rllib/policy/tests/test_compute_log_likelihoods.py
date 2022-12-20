import unittest

import gym
import numpy as np
import torch
from scipy.stats import norm

import ray
import ray.rllib.algorithms.dqn as dqn
import ray.rllib.algorithms.pg as pg
import ray.rllib.algorithms.ppo as ppo
import ray.rllib.algorithms.sac as sac
from ray.rllib.algorithms.crr import CRRConfig
from ray.rllib.algorithms.crr.torch import CRRTorchPolicy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import MAX_LOG_NN_OUTPUT, MIN_LOG_NN_OUTPUT, fc, one_hot
from ray.rllib.utils.test_utils import check, framework_iterator

tf1, tf, tfv = try_import_tf()


def do_test_log_likelihood(
    run,
    config,
    prev_a=None,
    continuous=False,
    layer_key=("fc", (0, 4), ("_hidden_layers.0.", "_logits.")),
    logp_func=None,
):
    config = config.copy(copy_frozen=False)
    # Run locally.
    config.num_rollout_workers = 0
    # Env setup.
    if continuous:
        config.env = "Pendulum-v1"
        obs_batch = preprocessed_obs_batch = np.array([[0.0, 0.1, -0.1]])
    else:
        config.env = "FrozenLake-v1"
        config.env_config = {"is_slippery": False, "map_name": "4x4"}
        obs_batch = np.array([0])
        # PG does not preprocess anymore by default.
        preprocessed_obs_batch = (
            one_hot(obs_batch, depth=16) if run is not pg.PG else obs_batch
        )

    prev_r = None if prev_a is None else np.array(0.0)

    # Test against all frameworks.
    for fw in framework_iterator(config):
        algo = config.build()

        policy = algo.get_policy()
        vars = policy.get_weights()
        # Sample n actions, then roughly check their logp against their
        # counts.
        num_actions = 1000 if not continuous else 50
        actions = []
        for _ in range(num_actions):
            # Single action from single obs.
            actions.append(
                algo.compute_single_action(
                    obs_batch[0],
                    prev_action=prev_a,
                    prev_reward=prev_r,
                    explore=True,
                    # Do not unsquash actions
                    # (remain in normalized [-1.0; 1.0] space).
                    unsquash_action=False,
                )
            )

        # Test all taken actions for their log-likelihoods vs expected values.
        if continuous:
            for idx in range(num_actions):
                a = actions[idx]
                if fw != "torch":
                    if isinstance(vars, list):
                        expected_mean_logstd = fc(
                            fc(obs_batch, vars[layer_key[1][0]]), vars[layer_key[1][1]]
                        )
                    else:
                        expected_mean_logstd = fc(
                            fc(
                                obs_batch,
                                vars["default_policy/{}_1/kernel".format(layer_key[0])],
                            ),
                            vars["default_policy/{}_out/kernel".format(layer_key[0])],
                        )
                else:
                    expected_mean_logstd = fc(
                        fc(
                            obs_batch,
                            vars["{}_model.0.weight".format(layer_key[2][0])],
                            framework=fw,
                        ),
                        vars["{}_model.0.weight".format(layer_key[2][1])],
                        framework=fw,
                    )
                mean, log_std = np.split(expected_mean_logstd, 2, axis=-1)
                if logp_func is None:
                    expected_logp = np.log(norm.pdf(a, mean, np.exp(log_std)))
                else:
                    expected_logp = logp_func(mean, log_std, a)
                logp = policy.compute_log_likelihoods(
                    np.array([a]),
                    preprocessed_obs_batch,
                    prev_action_batch=np.array([prev_a]) if prev_a else None,
                    prev_reward_batch=np.array([prev_r]) if prev_r else None,
                    actions_normalized=True,
                )
                check(logp, expected_logp[0], rtol=0.2)
        # Test all available actions for their logp values.
        else:
            for a in [0, 1, 2, 3]:
                count = actions.count(a)
                expected_prob = count / num_actions
                logp = policy.compute_log_likelihoods(
                    np.array([a]),
                    preprocessed_obs_batch,
                    prev_action_batch=np.array([prev_a]) if prev_a else None,
                    prev_reward_batch=np.array([prev_r]) if prev_r else None,
                )
                check(np.exp(logp), expected_prob, atol=0.2)


class TestComputeLogLikelihood(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_dqn(self):
        """Tests, whether DQN correctly computes logp in soft-q mode."""
        config = dqn.DQNConfig()
        # Soft-Q for DQN.
        config.exploration(exploration_config={"type": "SoftQ", "temperature": 0.5})
        config.debugging(seed=42)
        do_test_log_likelihood(dqn.DQN, config)

    def test_pg_cont(self):
        """Tests PG's (cont. actions) compute_log_likelihoods method."""
        config = pg.PGConfig()
        config.training(
            model={
                "fcnet_hiddens": [10],
                "fcnet_activation": "linear",
            }
        )
        config.debugging(seed=42)
        prev_a = np.array([0.0])
        do_test_log_likelihood(
            pg.PG,
            config,
            prev_a,
            continuous=True,
            layer_key=("fc", (0, 2), ("_hidden_layers.0.", "_logits.")),
        )

    def test_pg_discr(self):
        """Tests PG's (cont. actions) compute_log_likelihoods method."""
        config = pg.PGConfig()
        config.debugging(seed=42)
        prev_a = np.array(0)
        do_test_log_likelihood(pg.PG, config, prev_a)

    def test_ppo_cont(self):
        """Tests PPO's (cont. actions) compute_log_likelihoods method."""
        config = ppo.PPOConfig()
        config.training(
            model={
                "fcnet_hiddens": [10],
                "fcnet_activation": "linear",
            }
        )
        config.debugging(seed=42)
        prev_a = np.array([0.0])
        do_test_log_likelihood(ppo.PPO, config, prev_a, continuous=True)

    def test_ppo_discr(self):
        """Tests PPO's (discr. actions) compute_log_likelihoods method."""
        config = ppo.PPOConfig()
        config.debugging(seed=42)
        prev_a = np.array(0)
        do_test_log_likelihood(ppo.PPO, config, prev_a)

    def test_sac_cont(self):
        """Tests SAC's (cont. actions) compute_log_likelihoods method."""
        config = sac.SACConfig()
        config.training(
            policy_model_config={
                "fcnet_hiddens": [10],
                "fcnet_activation": "linear",
            }
        )
        config.debugging(seed=42)
        prev_a = np.array([0.0])

        # SAC cont uses a squashed normal distribution. Implement it's logp
        # logic here in numpy for comparing results.
        def logp_func(means, log_stds, values, low=-1.0, high=1.0):
            stds = np.exp(np.clip(log_stds, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT))
            unsquashed_values = np.arctanh((values - low) / (high - low) * 2.0 - 1.0)
            log_prob_unsquashed = np.sum(
                np.log(norm.pdf(unsquashed_values, means, stds)), -1
            )
            return log_prob_unsquashed - np.sum(
                np.log(1 - np.tanh(unsquashed_values) ** 2), axis=-1
            )

        do_test_log_likelihood(
            sac.SAC,
            config,
            prev_a,
            continuous=True,
            layer_key=(
                "fc",
                (0, 2),
                ("action_model._hidden_layers.0.", "action_model._logits."),
            ),
            logp_func=logp_func,
        )

    def test_sac_discr(self):
        """Tests SAC's (discrete actions) compute_log_likelihoods method."""
        config = sac.SACConfig()
        config.training(
            policy_model_config={
                "fcnet_hiddens": [10],
                "fcnet_activation": "linear",
            }
        )
        config.debugging(seed=42)
        prev_a = np.array(0)

        do_test_log_likelihood(sac.SAC, config, prev_a)

    def test_cql_cont(self):
        env = gym.make("Pendulum-v1")
        obs_space = env.observation_space
        act_space = env.action_space
        config = CRRConfig().framework(framework="torch").to_dict()
        policy = CRRTorchPolicy(obs_space, act_space, config=config)
        num_actions = 50
        actions = []
        obs_batch = np.array([[0.0, 0.1, -0.1]])
        for _ in range(num_actions):
            actions.append(
                policy.compute_single_action(
                    obs_batch[0], explore=True, unsquash_action=False
                )
            )
        input_batch = SampleBatch({"obs": torch.Tensor(obs_batch)})
        expected_mean_logstd = policy.action_distribution_fn(
            policy.model, obs_batch=input_batch, state_batches=None
        )
        # note this only works since CRR implements `action_distribution_fn`
        expected_mean_logstd = expected_mean_logstd[0].flatten().detach().numpy()
        mean, log_std = expected_mean_logstd
        for idx in range(num_actions):
            action = actions[idx][0]
            expected_logp = np.log(norm.pdf(action, mean, np.exp(log_std)))[0]
            computed_logp = policy.compute_log_likelihoods(
                torch.Tensor(action),
                obs_batch,
                actions_normalized=True,
            ).item()
            check(expected_logp, computed_logp, rtol=0.2)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
