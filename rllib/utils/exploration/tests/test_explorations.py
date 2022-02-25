import numpy as np
import sys
import unittest

import ray
import ray.rllib.agents.a3c as a3c
import ray.rllib.agents.ddpg as ddpg
import ray.rllib.agents.ddpg.td3 as td3
import ray.rllib.agents.dqn as dqn
import ray.rllib.agents.impala as impala
import ray.rllib.agents.pg as pg
import ray.rllib.agents.ppo as ppo
import ray.rllib.agents.sac as sac
from ray.rllib.utils import check, framework_iterator


def do_test_explorations(
    run, env, config, dummy_obs, prev_a=None, expected_mean_action=None
):
    """Calls an Agent's `compute_actions` with different `explore` options."""

    core_config = config.copy()
    if run not in [a3c.A3CTrainer]:
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
                if run is impala.ImpalaTrainer:
                    continue
                local_config["exploration_config"] = {"type": "Random"}
            print("exploration={}".format(exploration or "default"))

            trainer = run(config=local_config, env=env)

            # Make sure all actions drawn are the same, given same
            # observations.
            actions = []
            for _ in range(25):
                actions.append(
                    trainer.compute_single_action(
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
                    trainer.compute_single_action(
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
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_a2c(self):
        do_test_explorations(
            a3c.A2CTrainer,
            "CartPole-v0",
            a3c.a2c.A2C_DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1),
        )

    def test_a3c(self):
        do_test_explorations(
            a3c.A3CTrainer,
            "CartPole-v0",
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
            ddpg.DDPGTrainer,
            "Pendulum-v1",
            config,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0,
        )

    def test_simple_dqn(self):
        do_test_explorations(
            dqn.SimpleQTrainer,
            "CartPole-v0",
            dqn.SIMPLE_Q_DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
        )

    def test_dqn(self):
        do_test_explorations(
            dqn.DQNTrainer,
            "CartPole-v0",
            dqn.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
        )

    def test_impala(self):
        do_test_explorations(
            impala.ImpalaTrainer,
            "CartPole-v0",
            dict(impala.DEFAULT_CONFIG.copy(), num_gpus=0),
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0),
        )

    def test_pg(self):
        do_test_explorations(
            pg.PGTrainer,
            "CartPole-v0",
            pg.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1),
        )

    def test_ppo_discr(self):
        do_test_explorations(
            ppo.PPOTrainer,
            "CartPole-v0",
            ppo.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0),
        )

    def test_ppo_cont(self):
        do_test_explorations(
            ppo.PPOTrainer,
            "Pendulum-v1",
            ppo.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0]),
            prev_a=np.array([0.0]),
            expected_mean_action=0.0,
        )

    def test_sac(self):
        do_test_explorations(
            sac.SACTrainer,
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
            td3.TD3Trainer,
            "Pendulum-v1",
            config,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0,
        )


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
