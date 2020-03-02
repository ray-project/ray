import numpy as np
from tensorflow.python.eager.context import eager_mode
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
from ray.rllib.utils import check


def test_explorations(run,
                      env,
                      config,
                      dummy_obs,
                      prev_a=None,
                      expected_mean_action=None):
    """Calls an Agent's `compute_actions` with different `explore` options."""

    config = config.copy()
    if run not in [a3c.A3CTrainer]:
        config["num_workers"] = 0

    # Test all frameworks.
    for fw in ["torch", "eager", "tf"]:
        if fw == "torch" and \
                run in [ddpg.DDPGTrainer, dqn.DQNTrainer, dqn.SimpleQTrainer,
                        impala.ImpalaTrainer, sac.SACTrainer, td3.TD3Trainer]:
            continue
        elif fw == "eager" and run in [ddpg.DDPGTrainer, td3.TD3Trainer]:
            continue

        print("Testing {} in framework={}".format(run, fw))
        config["eager"] = (fw == "eager")
        config["use_pytorch"] = (fw == "torch")

        # Test for both the default Agent's exploration AND the `Random`
        # exploration class.
        for exploration in [None, "Random"]:
            if exploration == "Random":
                # TODO(sven): Random doesn't work for IMPALA yet.
                if run is impala.ImpalaTrainer:
                    continue
                config["exploration_config"] = {"type": "Random"}
            print("exploration={}".format(exploration or "default"))

            eager_mode_ctx = eager_mode()
            if fw == "eager":
                eager_mode_ctx.__enter__()

            trainer = run(config=config, env=env)

            # Make sure all actions drawn are the same, given same
            # observations.
            actions = []
            for _ in range(100):
                actions.append(
                    trainer.compute_action(
                        observation=dummy_obs,
                        explore=False,
                        prev_action=prev_a,
                        prev_reward=1.0 if prev_a is not None else None))
                check(actions[-1], actions[0])

            # Make sure actions drawn are different
            # (around some mean value), given constant observations.
            actions = []
            for _ in range(100):
                actions.append(
                    trainer.compute_action(
                        observation=dummy_obs,
                        explore=True,
                        prev_action=prev_a,
                        prev_reward=1.0 if prev_a is not None else None))
            check(
                np.mean(actions),
                expected_mean_action
                if expected_mean_action is not None else 0.5,
                atol=0.3)
            # Check that the stddev is not 0.0 (values differ).
            check(np.std(actions), 0.0, false=True)

            if fw == "eager":
                eager_mode_ctx.__exit__(None, None, None)


class TestExplorations(unittest.TestCase):
    """
    Tests all Exploration components and the deterministic flag for
    compute_action calls.
    """
    ray.init(ignore_reinit_error=True)

    def test_a2c(self):
        test_explorations(
            a3c.A2CTrainer,
            "CartPole-v0",
            a3c.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1))

    def test_a3c(self):
        test_explorations(
            a3c.A3CTrainer,
            "CartPole-v0",
            a3c.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1))

    def test_ddpg(self):
        test_explorations(
            ddpg.DDPGTrainer,
            "Pendulum-v0",
            ddpg.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0)

    def test_simple_dqn(self):
        test_explorations(dqn.SimpleQTrainer, "CartPole-v0",
                          dqn.DEFAULT_CONFIG, np.array([0.0, 0.1, 0.0, 0.0]))

    def test_dqn(self):
        test_explorations(dqn.DQNTrainer, "CartPole-v0", dqn.DEFAULT_CONFIG,
                          np.array([0.0, 0.1, 0.0, 0.0]))

    def test_impala(self):
        test_explorations(
            impala.ImpalaTrainer,
            "CartPole-v0",
            impala.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0))

    def test_pg(self):
        test_explorations(
            pg.PGTrainer,
            "CartPole-v0",
            pg.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1))

    def test_ppo_discr(self):
        test_explorations(
            ppo.PPOTrainer,
            "CartPole-v0",
            ppo.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(0))

    def test_ppo_cont(self):
        test_explorations(
            ppo.PPOTrainer,
            "Pendulum-v0",
            ppo.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0]),
            prev_a=np.array([0.0]),
            expected_mean_action=0.0)

    def test_sac(self):
        test_explorations(
            sac.SACTrainer,
            "Pendulum-v0",
            sac.DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0)

    def test_td3(self):
        test_explorations(
            td3.TD3Trainer,
            "Pendulum-v0",
            td3.TD3_DEFAULT_CONFIG,
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
