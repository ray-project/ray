import time
import unittest

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

import ray
import ray.rllib.algorithms.appo as appo
from ray.rllib.algorithms.appo.appo import LEARNER_RESULTS_CURR_KL_COEFF_KEY
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    LEARNER_RESULTS,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

frag_length = 50

FAKE_BATCH = {
    Columns.OBS: np.random.uniform(low=0, high=1, size=(frag_length, 4)).astype(
        np.float32
    ),
    Columns.ACTIONS: np.random.choice(2, frag_length).astype(np.float32),
    Columns.REWARDS: np.random.uniform(low=-1, high=1, size=(frag_length,)).astype(
        np.float32
    ),
    Columns.TERMINATEDS: np.array(
        [False for _ in range(frag_length - 1)] + [True]
    ).astype(np.float32),
    Columns.VF_PREDS: np.array(list(reversed(range(frag_length))), dtype=np.float32),
    Columns.ACTION_LOGP: np.log(
        np.random.uniform(low=0, high=1, size=(frag_length,))
    ).astype(np.float32),
    Columns.LOSS_MASK: np.ones(shape=(frag_length,)),
}


class TestAPPOLearner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_appo_loss(self):
        """Test that appo_policy_rlm loss matches the appo learner loss."""
        config = (
            appo.APPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=frag_length,
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
        # We have to set exploration_config here manually because setting it through
        # config.env_runners() only deep-updates it
        config.exploration_config = {}

        algo = config.build()

        train_batch = SampleBatch(
            tree.map_structure(lambda x: convert_to_torch_tensor(x), FAKE_BATCH)
        )

        algo_config = config.copy(copy_frozen=False)
        algo_config.learners(num_learners=0).experimental(_validate_config=False)
        algo_config.validate()

        learner_group = algo_config.build_learner_group(env=algo.env_runner.env)
        learner_group.update(batch=train_batch.as_multi_agent())

        algo.stop()

    def test_kl_coeff_changes(self):
        initial_kl_coeff = 0.01
        config = (
            appo.APPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=frag_length,
                exploration_config={},
            )
            .learners(num_learners=0)
            .experimental(_validate_config=False)
            .training(
                use_kl_loss=True,
                kl_coeff=initial_kl_coeff,
            )
            .rl_module(
                model_config=DefaultModelConfig(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
            )
        )
        algo = config.build()
        # Call train while results aren't returned because this is
        # a asynchronous algorithm and results are returned asynchronously.
        curr_kl_coeff = None
        while curr_kl_coeff is None:
            results = algo.train()
            print(results)
            results = results.get(LEARNER_RESULTS, {})
            results = results.get(DEFAULT_MODULE_ID, {})
            curr_kl_coeff = results.get(LEARNER_RESULTS_CURR_KL_COEFF_KEY)
        self.assertNotEqual(curr_kl_coeff, initial_kl_coeff)

    def test_learner_thread_results_with_circular_buffer(self):
        """Tests that no learner results pile up when each batch is trained K times."""
        num_iterations_per_batch = 2
        num_batches = 4
        config = (
            appo.APPOConfig()
            .environment("CartPole-v1")
            .learners(num_learners=0)
            .training(
                circular_buffer_num_batches=num_batches,
                circular_buffer_iterations_per_batch=num_iterations_per_batch,
                broadcast_interval=1,
            )
        )
        env = gym.make("CartPole-v1")
        learner_group = config.build_learner_group(
            spaces={DEFAULT_MODULE_ID: (env.observation_space, env.action_space)}
        )
        self.addCleanup(learner_group.shutdown)
        learner = learner_group._learner

        # Replace the gradient update in the learner thread with a stub that only
        # logs the number of trained env steps.
        num_updates = []

        def _update(*, self, training_data, **kwargs):
            self.metrics.log_value(
                (ALL_MODULES, NUM_ENV_STEPS_TRAINED_LIFETIME),
                training_data.batch.env_steps(),
                reduce="lifetime_sum",
            )
            num_updates.append(1)

        learner._learner_thread._update_method = _update

        def _wait_for_updates(n):
            for _ in range(500):
                if len(num_updates) >= n:
                    return
                time.sleep(0.01)
            self.fail(f"Learner thread did only {len(num_updates)} of {n} updates.")

        batch = SampleBatch(FAKE_BATCH).as_multi_agent()
        results = []
        # Submit as many batches as fit into the circular buffer, so none is dropped
        # and the learner thread trains on each of them K times.
        for i in range(num_batches):
            results.extend(learner_group.update(batch=batch, timesteps={}))
            _wait_for_updates((i + 1) * num_iterations_per_batch)

        # The learner thread ran K updates per `update()` call, but at most one
        # result is waiting to be collected.
        self.assertEqual(len(num_updates), num_batches * num_iterations_per_batch)
        self.assertLessEqual(learner._learner_thread_out_queue.qsize(), 1)

        # The returned results, plus the one waiting in the queue and the metrics
        # not reduced yet, cover all updates.
        while not learner._learner_thread_out_queue.empty():
            results.append(learner._learner_thread_out_queue.get_nowait())
        results.append(learner.metrics.reduce())
        metrics = MetricsLogger(root=True)
        for result in results:
            metrics.aggregate([result], key=LEARNER_RESULTS)
        self.assertEqual(
            metrics.peek(
                (LEARNER_RESULTS, ALL_MODULES, NUM_ENV_STEPS_TRAINED_LIFETIME)
            ),
            num_batches * num_iterations_per_batch * len(FAKE_BATCH[Columns.OBS]),
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
