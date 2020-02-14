import numpy as np
import unittest

import ray
import ray.rllib.agents.a3c as a3c
import ray.rllib.agents.dqn as dqn
import ray.rllib.agents.impala as impala
import ray.rllib.agents.pg as pg
import ray.rllib.agents.ppo as ppo
import ray.rllib.agents.sac as sac
from ray.rllib.utils import check, merge_dicts


def test_explorations(run,
                      env,
                      config,
                      dummy_obs,
                      prev_a=None,
                      expected_mean_action=None):
    """Calls an Agent's `compute_actions` with different `explore` options."""
    # Test all frameworks.
    for fw in ["tf", "eager", "torch"]:
        if fw == "torch" and \
                run in [dqn.DQNTrainer, dqn.SimpleQTrainer,
                        impala.ImpalaTrainer, sac.SACTrainer]:
            continue
        print("Testing {} in framework={}".format(run, fw))
        config["eager"] = True if fw == "eager" else False
        config["use_pytorch"] = True if fw == "torch" else False

        trainer = run(config=config, env=env)

        # Make sure all actions drawn are the same, given same observations.
        actions = []
        for _ in range(100):
            actions.append(
                trainer.compute_action(
                    observation=dummy_obs,
                    explore=False,
                    prev_action=prev_a,
                    prev_reward=1.0 if prev_a is not None else None))
            check(actions[-1], actions[0])

        # Make sure actions drawn are different (around some mean value),
        # given constant observations.
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
            expected_mean_action if expected_mean_action is not None else 0.5,
            atol=0.3)
        # Check that the stddev is not 0.0 (values differ).
        check(np.std(actions), 0.0, false=True)


class TestExplorations(unittest.TestCase):
    """
    Tests all Exploration components and the deterministic flag for
    compute_action calls.
    """
    ray.init()

    def test_a2c(self):
        test_explorations(
            a3c.A2CTrainer,
            "CartPole-v0",
            merge_dicts(a3c.DEFAULT_CONFIG, {
                "eager": True,
                "num_workers": 0
            }),
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1))

    def test_a3c(self):
        test_explorations(
            a3c.A3CTrainer,
            "CartPole-v0",
            merge_dicts(a3c.DEFAULT_CONFIG, {
                "eager": True,
                "num_workers": 1
            }),
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array(1))

    def test_simple_dqn(self):
        test_explorations(
            dqn.SimpleQTrainer, "CartPole-v0",
            merge_dicts(dqn.DEFAULT_CONFIG, {
                "eager": True,
                "num_workers": 0
            }), np.array([0.0, 0.1, 0.0, 0.0]))

        # config = dqn.DEFAULT_CONFIG.copy()
        # config["exploration_config"] = {"type": "Random"}
        # config["eager"] = True
        # config["num_workers"] = 0
        # test_exploration_support(
        #     dqn.SimpleQTrainer, "CartPole-v0", config,
        #     np.array([0.0, 0.1, 0.0, 0.0]))

    def test_dqn(self):
        test_explorations(dqn.DQNTrainer, "CartPole-v0",
                          merge_dicts(dqn.DEFAULT_CONFIG, {"num_workers": 0}),
                          np.array([0.0, 0.1, 0.0, 0.0]))

    def test_impala(self):
        test_explorations(
            impala.ImpalaTrainer,
            "CartPole-v0",
            merge_dicts(impala.DEFAULT_CONFIG, {"num_workers": 0}),
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array([0]))

    def test_pg(self):
        test_explorations(
            pg.PGTrainer,
            "CartPole-v0",
            merge_dicts(pg.DEFAULT_CONFIG, {"num_workers": 0}),
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array([1]))

    def test_ppo_discr(self):
        test_explorations(
            ppo.PPOTrainer,
            "CartPole-v0",
            merge_dicts(ppo.DEFAULT_CONFIG, {"num_workers": 0}),
            np.array([0.0, 0.1, 0.0, 0.0]),
            prev_a=np.array([0]))

    def test_ppo_cont(self):
        test_explorations(
            ppo.PPOTrainer,
            "Pendulum-v0",
            merge_dicts(ppo.DEFAULT_CONFIG, {"num_workers": 0}),
            np.array([0.0, 0.1, 0.0]),
            prev_a=np.array([0]),
            expected_mean_action=0.0)

    def test_sac(self):
        test_explorations(
            sac.SACTrainer,
            "Pendulum-v0",
            merge_dicts(sac.DEFAULT_CONFIG, {"num_workers": 0}),
            np.array([0.0, 0.1, 0.0]),
            expected_mean_action=0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
