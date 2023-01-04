import numpy as np
import os
import unittest

import ray
import ray.rllib.algorithms.simple_q as simple_q
from ray.rllib.algorithms.simple_q.simple_q_tf_policy import SimpleQTF2Policy
from ray.rllib.algorithms.simple_q.simple_q_torch_policy import SimpleQTorchPolicy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import fc, huber_loss, one_hot
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)
from ray.rllib.utils.metrics.learner_info import (
    LEARNER_INFO,
    LEARNER_STATS_KEY,
    DEFAULT_POLICY_ID,
)

tf1, tf, tfv = try_import_tf()


class TestSimpleQ(unittest.TestCase):

    num_gpus = float(os.environ.get("RLLIB_NUM_GPUS", "0"))

    def test_simple_q_compilation(self):
        """Test whether SimpleQ can be built on all frameworks."""
        # Run locally and with compression
        config = (
            simple_q.SimpleQConfig()
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            .resources(num_gpus=self.num_gpus)
            .rollouts(num_rollout_workers=0, compress_observations=True)
            .training(num_steps_sampled_before_learning_starts=0)
        )

        num_iterations = 2

        for _ in framework_iterator(config, with_eager_tracing=True):
            algo = config.build(env="CartPole-v1")
            rw = algo.workers.local_worker()
            for i in range(num_iterations):
                sb = rw.sample()
                assert sb.count == config.rollout_fragment_length
                results = algo.train()
                check_train_results(results)
                print(results)

            check_compute_single_action(algo)

    def test_simple_q_loss_function(self):
        """Tests the Simple-Q loss function results on all frameworks."""

        # Don't run this test on the GPU (non-deterministic on GPU).
        if self.num_gpus:
            return

        config = (
            simple_q.SimpleQConfig()
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=float(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            .rollouts(num_rollout_workers=0)
            # Use very simple net (layer0=10 nodes, q-layer=2 nodes (2 actions)).
            .training(
                model={
                    "fcnet_hiddens": [10],
                    "fcnet_activation": "linear",
                },
                num_steps_sampled_before_learning_starts=0,
            )
            .environment("CartPole-v1")
        )

        for fw in framework_iterator(config):
            # Generate Algorithm and get its default Policy object.
            trainer = config.build()
            policy = trainer.get_policy()
            # Batch of size=2.
            input_ = SampleBatch(
                {
                    SampleBatch.CUR_OBS: np.random.random(size=(2, 4)),
                    SampleBatch.ACTIONS: np.array([0, 1]),
                    SampleBatch.REWARDS: np.array([0.4, -1.23]),
                    SampleBatch.TERMINATEDS: np.array([False, False]),
                    SampleBatch.NEXT_OBS: np.random.random(size=(2, 4)),
                    SampleBatch.EPS_ID: np.array([1234, 1234]),
                    SampleBatch.AGENT_INDEX: np.array([0, 0]),
                    SampleBatch.ACTION_LOGP: np.array([-0.1, -0.1]),
                    SampleBatch.ACTION_DIST_INPUTS: np.array(
                        [[0.1, 0.2], [-0.1, -0.2]]
                    ),
                    SampleBatch.ACTION_PROB: np.array([0.1, 0.2]),
                    "q_values": np.array([[0.1, 0.2], [0.2, 0.1]]),
                }
            )
            # Get model vars for computing expected model outs (q-vals).
            # 0=layer-kernel; 1=layer-bias; 2=q-val-kernel; 3=q-val-bias
            vars = policy.get_weights()
            if isinstance(vars, dict):
                vars = list(vars.values())

            vars_t = policy.target_model.variables()
            if fw == "tf":
                vars_t = policy.get_session().run(vars_t)

            # Q(s,a) outputs.
            q_t = np.sum(
                one_hot(input_[SampleBatch.ACTIONS], 2)
                * fc(
                    fc(
                        input_[SampleBatch.CUR_OBS],
                        vars[0 if fw != "torch" else 2],
                        vars[1 if fw != "torch" else 3],
                        framework=fw,
                    ),
                    vars[2 if fw != "torch" else 0],
                    vars[3 if fw != "torch" else 1],
                    framework=fw,
                ),
                1,
            )
            # max[a'](Qtarget(s',a')) outputs.
            q_target_tp1 = np.max(
                fc(
                    fc(
                        input_[SampleBatch.NEXT_OBS],
                        vars_t[0 if fw != "torch" else 2],
                        vars_t[1 if fw != "torch" else 3],
                        framework=fw,
                    ),
                    vars_t[2 if fw != "torch" else 0],
                    vars_t[3 if fw != "torch" else 1],
                    framework=fw,
                ),
                1,
            )
            # TD-errors (Bellman equation).
            td_error = q_t - config.gamma * input_[SampleBatch.REWARDS] + q_target_tp1
            # Huber/Square loss on TD-error.
            expected_loss = huber_loss(td_error).mean()

            if fw == "torch":
                input_ = policy._lazy_tensor_dict(input_)
            # Get actual out and compare.
            if fw == "tf":
                out = policy.get_session().run(
                    policy._loss,
                    feed_dict=policy._get_loss_inputs_dict(input_, shuffle=False),
                )
            else:
                out = (SimpleQTorchPolicy if fw == "torch" else SimpleQTF2Policy).loss(
                    policy, policy.model, None, input_
                )
            check(out, expected_loss, decimals=1)

    def test_simple_q_lr_schedule(self):
        """Test PG with learning rate schedule."""
        config = (
            simple_q.SimpleQConfig()
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            .resources(num_gpus=self.num_gpus)
            .reporting(
                min_sample_timesteps_per_iteration=10,
                # Make sure that results contain info on default policy
                min_train_timesteps_per_iteration=10,
                # 0 metrics reporting delay, this makes sure timestep,
                # which lr depends on, is updated after each worker rollout.
                min_time_s_per_iteration=0,
            )
            .rollouts(
                num_rollout_workers=1,
                rollout_fragment_length=50,
            )
            .training(lr=0.2, lr_schedule=[[0, 0.2], [500, 0.001]])
        )

        def _step_n_times(algo, n: int):
            """Step trainer n times.

            Returns:
                learning rate at the end of the execution.
            """
            for _ in range(n):
                results = algo.train()
            return results["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY][
                "cur_lr"
            ]

        for _ in framework_iterator(config):
            algo = config.build(env="CartPole-v1")

            lr = _step_n_times(algo, 1)  # 50 timesteps
            # Close to 0.2
            self.assertGreaterEqual(lr, 0.15)

            lr = _step_n_times(algo, 8)  # Close to 500 timesteps
            # LR Annealed to 0.001
            self.assertLessEqual(float(lr), 0.5)

            lr = _step_n_times(algo, 2)  # > 500 timesteps
            # LR == 0.001
            self.assertAlmostEqual(lr, 0.001)

            algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
