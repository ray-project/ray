import numpy as np
import sys
import unittest

import ray
import ray.rllib.algorithms.a2c as a2c
import ray.rllib.algorithms.a3c as a3c
import ray.rllib.algorithms.ddpg as ddpg
import ray.rllib.algorithms.dqn as dqn
import ray.rllib.algorithms.impala as impala
import ray.rllib.algorithms.pg as pg
import ray.rllib.algorithms.ppo as ppo
import ray.rllib.algorithms.sac as sac
import ray.rllib.algorithms.simple_q as simple_q
import ray.rllib.algorithms.td3.td3 as td3
from ray.rllib.utils import check, framework_iterator


def do_test_explorations(config, dummy_obs, prev_a=None, expected_mean_action=None):
    """Calls an Agent's `compute_actions` with different `explore` options."""

    # Test all frameworks.
    for _ in framework_iterator(config):
        print(f"Algorithm={config.algo_class}")

        # Test for both the default Agent's exploration AND the `Random`
        # exploration class.
        for exploration in [None, "Random"]:
            local_config = config.copy()
            if exploration == "Random":
                if local_config._enable_rl_module_api:
                    # TODO(Artur): Support Random exploration with RL Modules.
                    continue
                local_config.exploration(exploration_config={"type": "Random"})
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

    def test_a2c(self):
        config = (
            a2c.A2CConfig().environment("CartPole-v1").rollouts(num_rollout_workers=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1),
        )

    def test_a3c(self):
        config = a3c.A3CConfig().environment("CartPole-v1")
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1),
        )

    def test_ddpg(self):
        # Switch off random timesteps at beginning. We want to test actual
        # GaussianNoise right away.
        config = (
            ddpg.DDPGConfig()
            .environment("Pendulum-v1")
            .rollouts(num_rollout_workers=0)
            .exploration(exploration_config={"random_timesteps": 0})
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0,
        )

    def test_simple_dqn(self):
        config = (
            simple_q.SimpleQConfig()
            .environment("CartPole-v1")
            .rollouts(num_rollout_workers=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0, 0.0]),
        )

    def test_dqn(self):
        config = (
            dqn.DQNConfig().environment("CartPole-v1").rollouts(num_rollout_workers=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0, 0.0]),
        )

    def test_impala(self):
        config = (
            impala.ImpalaConfig()
            .environment("CartPole-v1")
            .rollouts(num_rollout_workers=0)
            .resources(num_gpus=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0),
        )

    def test_pg(self):
        config = (
            pg.PGConfig().environment("CartPole-v1").rollouts(num_rollout_workers=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1),
        )

    def test_ppo_discr(self):
        config = (
            ppo.PPOConfig().environment("CartPole-v1").rollouts(num_rollout_workers=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0),
        )

    def test_ppo_cont(self):
        config = (
            ppo.PPOConfig().environment("Pendulum-v1").rollouts(num_rollout_workers=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0]),
            prev_a=np.array([0.0]),
            expected_mean_action=0.0,
        )

    def test_sac(self):
        config = (
            sac.SACConfig().environment("Pendulum-v1").rollouts(num_rollout_workers=0)
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0,
        )

    def test_td3(self):
        config = (
            td3.TD3Config()
            .environment("Pendulum-v1")
            .rollouts(num_rollout_workers=0)
            # Switch off random timesteps at beginning. We want to test actual
            # GaussianNoise right away.
            .exploration(exploration_config={"random_timesteps": 0})
        )
        do_test_explorations(
            config,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0,
        )


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
