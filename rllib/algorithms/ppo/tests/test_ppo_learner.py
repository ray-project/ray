import tempfile
import unittest

import gymnasium as gym
import numpy as np

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_KL_KEY,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import DEFAULT_MODULE_ID
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    LEARNER_RESULTS,
    LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME,
    LEARNER_UPDATE_SKIPPED_FOR_PEER_LIFETIME,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.test_utils import check
from ray.tune.registry import register_env

# Fake CartPole episode of n time steps.
FAKE_BATCH = {
    Columns.OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    Columns.NEXT_OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    Columns.ACTIONS: np.array([0, 1, 1]),
    Columns.REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
    Columns.TERMINATEDS: np.array([False, False, True]),
    Columns.TRUNCATEDS: np.array([False, False, False]),
    Columns.VF_PREDS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
    Columns.ACTION_DIST_INPUTS: np.array(
        [[-2.0, 0.5], [-3.0, -0.3], [-0.1, 2.5]], dtype=np.float32
    ),
    Columns.ACTION_LOGP: np.array([-0.5, -0.1, -0.2], dtype=np.float32),
    Columns.EPS_ID: np.array([0, 0, 0]),
}


class TestPPO(unittest.TestCase):
    ENV = gym.make("CartPole-v1")

    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_save_to_path_and_restore_from_path(self):
        """Tests saving and loading the state of the PPO Learner Group."""
        config = (
            ppo.PPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=0,
            )
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
            )
        )

        algo_config = config.copy(copy_frozen=False)
        algo_config.validate()
        algo_config.freeze()
        learner_group1 = algo_config.build_learner_group(env=self.ENV)
        learner_group2 = algo_config.build_learner_group(env=self.ENV)
        with tempfile.TemporaryDirectory() as tmpdir:
            learner_group1.save_to_path(tmpdir)
            learner_group2.restore_from_path(tmpdir)
            # Remove functions from state b/c they are not comparable via `check`.
            s1 = learner_group1.get_state()
            s2 = learner_group2.get_state()
            check(s1, s2)

    def test_skipped_update_leaves_the_kl_coeff_alone(self):
        """A skipped update measures no KL, so it must not move the KL coefficient.

        `after_gradient_based_update` does not run for a skipped update, which is
        what keeps this hook away from a metric it never measured: the key survives
        from earlier updates and peeks as NaN once its window is empty, which is
        indistinguishable from an update that really did diverge -- and warned the
        user about a model problem that is not there.
        """
        learner = ppo.PPOConfig().training(kl_coeff=0.01).build_learner(env=self.ENV)
        # What an earlier, real update leaves behind once its window is empty.
        learner.metrics.log_value(
            (DEFAULT_MODULE_ID, LEARNER_RESULTS_KL_KEY), float("nan"), window=1
        )
        before = learner.curr_kl_coeffs_per_module[DEFAULT_MODULE_ID].item()

        with self.assertNoLogs(
            "ray.rllib.algorithms.ppo.torch.ppo_torch_learner", level="WARNING"
        ):
            learner.update(batch=MultiAgentBatch(policy_batches={}, env_steps=0))

        check(before, learner.curr_kl_coeffs_per_module[DEFAULT_MODULE_ID].item())

    def test_update_without_episodes_is_skipped(self):
        """`update(episodes=[])` is skipped like an empty batch, not built into one.

        A Learner receives no episodes when its shard of a short list of episodes or
        episode refs is empty, or when every episode it was sent was lost with its
        EnvRunner. PPO's learner connector pipeline starts with
        `AddOneTsToEpisodesAndTruncate`, which indexes `episodes[0]`, so the pipeline
        must not run at all in that case -- it would raise before the skip decision.
        """
        learner = ppo.PPOConfig().build_learner(env=self.ENV)

        results = learner.update(episodes=[])

        self.assertEqual(
            1, results[ALL_MODULES][LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME]
        )
        self.assertNotIn(learner.TOTAL_LOSS_KEY, results.get(DEFAULT_MODULE_ID, {}))

    def test_learner_group_skips_when_a_learner_receives_no_episodes(self):
        """A Learner whose shard of episode refs is empty makes the group skip.

        `ShardObjectRefIterator` hands a Learner `[]` whenever an update carries fewer
        episode refs than there are Learners, which is routine when a single
        EnvRunner's sample already fills an update. That Learner must take part in
        the skip agreement with an empty batch; if it raised in its connector pipeline
        instead, its peer would wait in the agreement forever -- and so would this
        test.
        """
        config = (
            ppo.PPOConfig()
            .learners(num_learners=2)
            .training(
                model=dict(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
            )
        )
        config.validate()
        config.freeze()
        learner_group = config.build_learner_group(env=self.ENV)
        try:
            episode = SingleAgentEpisode(
                observation_space=self.ENV.observation_space,
                action_space=self.ENV.action_space,
                observations=[
                    np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
                    np.array([0.5, 0.6, 0.7, 0.8], dtype=np.float32),
                    np.array([0.9, 1.0, 1.1, 1.2], dtype=np.float32),
                    np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
                    np.array([-0.1, -0.2, -0.3, -0.4], dtype=np.float32),
                ],
                actions=[0, 1, 1, 0],
                rewards=[1.0, -1.0, 0.5, 0.3],
                terminated=True,
                len_lookback_buffer=0,
            )
            episode.to_numpy()
            # One episode ref for two Learners: the second Learner's shard is `[]`.
            fed, starved = MetricsLogger.peek_results(
                learner_group.update(episodes_refs=[ray.put([episode])])
            )
            self.assertEqual(
                1, starved[ALL_MODULES][LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME]
            )
            self.assertEqual(
                1, fed[ALL_MODULES][LEARNER_UPDATE_SKIPPED_FOR_PEER_LIFETIME]
            )
        finally:
            learner_group.shutdown()

    def test_kl_coeff_changes(self):
        # Simple environment with 4 independent cartpole entities
        register_env(
            "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": 2})
        )

        initial_kl_coeff = 0.01
        config = (
            ppo.PPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=50,
                exploration_config={},
            )
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
                kl_coeff=initial_kl_coeff,
            )
            .environment("multi_agent_cartpole")
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda agent_id, episode, **kwargs: (
                    "p{}".format(agent_id % 2)
                ),
            )
        )

        algo = config.build()
        # Call train while results aren't returned because this is
        # a asynchronous Algorithm and results are returned asynchronously.
        curr_kl_coeff_1 = None
        curr_kl_coeff_2 = None
        while not curr_kl_coeff_1 or not curr_kl_coeff_2:
            results = algo.train()

            # Attempt to get the current KL coefficient from the learner.
            # Iterate until we have found both coefficients at least once.
            if "p0" in results[LEARNER_RESULTS]:
                curr_kl_coeff_1 = results[LEARNER_RESULTS]["p0"][
                    LEARNER_RESULTS_CURR_KL_COEFF_KEY
                ]
            if "p1" in results[LEARNER_RESULTS]:
                curr_kl_coeff_2 = results[LEARNER_RESULTS]["p1"][
                    LEARNER_RESULTS_CURR_KL_COEFF_KEY
                ]

        self.assertNotEqual(curr_kl_coeff_1, initial_kl_coeff)
        self.assertNotEqual(curr_kl_coeff_2, initial_kl_coeff)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
