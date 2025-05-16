import unittest

import numpy as np
from scipy.stats import norm

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.utils.numpy import fc, one_hot
from ray.rllib.utils.test_utils import check


def _get_expected_logp(vars, obs_batch, a, layer_key, logp_func=None):
    """Get the expected logp for the given obs_batch and action.

    Args:
        vars: The ModelV2 weights.
        obs_batch: The observation batch.
        a: The action batch.
        layer_key: The layer key to use for the fc layers.
        logp_func: Optional custom logp function to use.

    Returns:
        The expected logp.
    """
    expected_mean_logstd = fc(
        fc(
            obs_batch,
            vars["{}_model.0.weight".format(layer_key[2][0])],
            framework="torch",
        ),
        vars["{}_model.0.weight".format(layer_key[2][1])],
        framework="torch",
    )
    mean, log_std = np.split(expected_mean_logstd, 2, axis=-1)
    if logp_func is None:
        expected_logp = np.log(norm.pdf(a, mean, np.exp(log_std)))
    else:
        expected_logp = logp_func(mean, log_std, a)

    return expected_logp


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
    config.num_env_runners = 0
    # Env setup.
    if continuous:
        config.env = "Pendulum-v1"
        obs_batch = preprocessed_obs_batch = np.array([[0.0, 0.1, -0.1]])
    else:
        config.env = "FrozenLake-v1"
        config.env_config = {"is_slippery": False, "map_name": "4x4"}
        obs_batch = np.array([0])
        # PG does not preprocess anymore by default.
        preprocessed_obs_batch = one_hot(obs_batch, depth=16)

    prev_r = None if prev_a is None else np.array(0.0)

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

            logp = policy.compute_log_likelihoods(
                np.array([a]),
                preprocessed_obs_batch,
                prev_action_batch=np.array([prev_a]) if prev_a else None,
                prev_reward_batch=np.array([prev_r]) if prev_r else None,
                actions_normalized=True,
                in_training=False,
            )

            expected_logp = _get_expected_logp(vars, obs_batch, a, layer_key, logp_func)
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
                in_training=False,
            )

            check(np.exp(logp), expected_prob, atol=0.2)


class TestComputeLogLikelihood(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_ppo_cont(self):
        """Tests PPO's (cont. actions) compute_log_likelihoods method."""
        config = (
            ppo.PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .training(
                model={
                    "fcnet_hiddens": [10],
                    "fcnet_activation": "linear",
                }
            )
            .debugging(seed=42)
        )
        prev_a = np.array([0.0])
        do_test_log_likelihood(ppo.PPO, config, prev_a, continuous=True)

    def test_ppo_discr(self):
        """Tests PPO's (discr. actions) compute_log_likelihoods method."""
        config = ppo.PPOConfig()
        config.api_stack(
            enable_env_runner_and_connector_v2=False,
            enable_rl_module_and_learner=False,
        )
        config.debugging(seed=42)
        prev_a = np.array(0)
        do_test_log_likelihood(ppo.PPO, config, prev_a)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
