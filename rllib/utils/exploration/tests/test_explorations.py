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


def do_test_explorations(
    run, env, config, dummy_obs, prev_a=None, expected_mean_action=None
):
    """Calls an Agent's `compute_actions` with different `explore` options."""

    core_config = config.copy()
    if run not in [a3c.A3C]:
        core_config["num_workers"] = 0

    # Test all frameworks.
    for _ in framework_iterator(core_config):
        print("Agent={}".format(run))

        # Test for both the default Agent's exploration AND the `Random`
        # exploration class.
        for exploration in [None, "Random"]:
            local_config = core_config.copy()
            if exploration == "Random":
                # TODO(sven): Random doesn't work for IMPALA yet.
                if run is impala.Impala:
                    continue
                local_config["exploration_config"] = {"type": "Random"}
            print("exploration={}".format(exploration or "default"))

            algo = run(config=local_config, env=env)

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
        do_test_explorations(
            a2c.A2C,
            "CartPole-v1",
            a2c.A2C_DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1),
        )

    def test_a3c(self):
        do_test_explorations(
            a3c.A3C,
            "CartPole-v1",
            a3c.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1),
        )

    def test_ddpg(self):
        # Switch off random timesteps at beginning. We want to test actual
        # GaussianNoise right away.
        config = ddpg.DEFAULT_CONFIG.copy()
        config["exploration_config"]["random_timesteps"] = 0
        do_test_explorations(
            ddpg.DDPG,
            "Pendulum-v1",
            config,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0,
        )

    def test_simple_dqn(self):
        do_test_explorations(
            simple_q.SimpleQ,
            "CartPole-v1",
            simple_q.SimpleQConfig().to_dict(),
            np.array([0.0, 0.1, 0.0, 0.0]),
        )

    def test_dqn(self):
        do_test_explorations(
            dqn.DQN,
            "CartPole-v1",
            dqn.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
        )

    def test_impala(self):
        do_test_explorations(
            impala.Impala,
            "CartPole-v1",
            dict(impala.DEFAULT_CONFIG.copy(), num_gpus=0),
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0),
        )

    def test_pg(self):
        do_test_explorations(
            pg.PG,
            "CartPole-v1",
            pg.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1),
        )

    def test_ppo_discr(self):
        do_test_explorations(
            ppo.PPO,
            "CartPole-v1",
            ppo.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0),
        )

    def test_ppo_cont(self):
        do_test_explorations(
            ppo.PPO,
            "Pendulum-v1",
            ppo.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0]),
            prev_a=np.array([0.0]),
            expected_mean_action=0.0,
        )

    def test_sac(self):
        do_test_explorations(
            sac.SAC,
            "Pendulum-v1",
            sac.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0,
        )

    def test_td3(self):
        config = td3.TD3_DEFAULT_CONFIG.copy()
        # Switch off random timesteps at beginning. We want to test actual
        # GaussianNoise right away.
        config["exploration_config"]["random_timesteps"] = 0
        do_test_explorations(
            td3.TD3,
            "Pendulum-v1",
            config,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0,
        )


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
