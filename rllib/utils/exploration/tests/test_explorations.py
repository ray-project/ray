import numpy as np
import sys
import unittest

import ray
import ray.rllib.algorithms.impala as impala
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.utils import check


def do_test_explorations(config, dummy_obs, prev_a=None, expected_mean_action=None):
    """Calls an Agent's `compute_actions` with different `explore` options."""

    print(f"Algorithm={config.algo_class}")

    # Test for both the default Agent's exploration AND the `Random`
    # exploration class.
    for exploration in [None, "Random"]:
        local_config = config.copy()
        if exploration == "Random":
            local_config.env_runners(exploration_config={"type": "Random"})
        print("exploration={}".format(exploration or "default"))

        algo = local_config.build()

        # Make sure all actions drawn are the same, given same
        # observations.
        actions = []
        for _ in range(25):
            actions.append(
                algo.compute_single_action(
                    observation=dummy_obs,
                    explore=False,
                    prev_action=prev_a,
                    prev_reward=1.0 if prev_a is not None else None,
                )
            )
            check(actions[-1], actions[0])

        # Make sure actions drawn are different
        # (around some mean value), given constant observations.
        actions = []
        for _ in range(500):
            actions.append(
                algo.compute_single_action(
                    observation=dummy_obs,
                    explore=True,
                    prev_action=prev_a,
                    prev_reward=1.0 if prev_a is not None else None,
                )
            )
        check(
            np.mean(actions),
            expected_mean_action if expected_mean_action is not None else 0.5,
            atol=0.4,
        )
        # Check that the stddev is not 0.0 (values differ).
        check(np.std(actions), 0.0, false=True)


class TestExplorations(unittest.TestCase):
    """
    Tests all Exploration components and the deterministic flag for
    compute_action calls.
    """

    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_impala(self):
        config = (
            impala.IMPALAConfig()
            .api_stack(
                enable_rl_module_and_learner=False,
                enable_env_runner_and_connector_v2=False,
            )
            .environment("CartPole-v1")
            .env_runners(num_env_runners=0)
            .resources(num_gpus=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0),
        )

    def test_ppo_discr(self):
        config = (
            ppo.PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .environment("CartPole-v1")
            .env_runners(num_env_runners=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0),
        )

    def test_ppo_cont(self):
        config = (
            ppo.PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .environment("Pendulum-v1")
            .env_runners(num_env_runners=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0]),
            prev_a=np.array([0.0]),
            expected_mean_action=0.0,
        )


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
