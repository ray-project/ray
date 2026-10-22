import time
import unittest
from unittest import mock

import gymnasium as gym
import numpy as np
import pytest
import torch

import ray
import ray.rllib.algorithms.impala as impala
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.metrics import LEARNER_RESULTS
from ray.rllib.utils.test_utils import check


class TestIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_minibatch_size_check(self):
        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .training(minibatch_size=100)
            .env_runners(rollout_fragment_length=30)
        )

        with pytest.raises(
            ValueError,
            match=r"`minibatch_size` \(100\) must either be None or a multiple of `rollout_fragment_length` \(30\)",
        ):
            config.validate()

    def test_impala_lr_schedule(self):
        # Test whether we correctly ignore the "lr" setting.
        # The first lr should be 0.05.
        config = (
            impala.IMPALAConfig()
            .learners(num_learners=0)
            .experimental(_validate_config=False)  #
            .training(
                lr=[
                    [0, 0.05],
                    [100000, 0.000001],
                ],
                train_batch_size=100,
            )
            .env_runners(num_envs_per_env_runner=2)
            .environment(env="CartPole-v1")
        )

        def get_lr(result):
            return result[LEARNER_RESULTS][DEFAULT_POLICY_ID][
                "default_optimizer_learning_rate"
            ]

        algo = config.build()
        optim = algo.learner_group._learner.get_optimizer()

        try:
            check(optim.param_groups[0]["lr"], 0.05)
            for _ in range(1):
                r1 = algo.train()
            for _ in range(2):
                r2 = algo.train()
            for _ in range(2):
                r3 = algo.train()
            # Due to the asynch'ness of IMPALA, learner-stats metrics
            # could be delayed by one iteration. Do 3 train() calls here
            # and measure guaranteed decrease in lr between 1st and 3rd.
            lr1 = get_lr(r1)
            lr2 = get_lr(r2)
            lr3 = get_lr(r3)
            assert lr2 <= lr1, (lr1, lr2)
            assert lr3 <= lr2, (lr2, lr3)
            assert lr3 < lr1, (lr1, lr3)
        finally:
            algo.stop()

    def test_local_learner_thread_stops_on_algo_stop(self):
        # Regression test: `algo.stop()` -> `LearnerGroup.shutdown()` ->
        # `IMPALALearner.shutdown()` must stop and join the local IMPALA
        # `_LearnerThread`. Otherwise the daemon thread keeps spinning and
        # can race against interpreter shutdown inside an auto_init-wrapped
        # Ray API.
        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .learners(num_learners=0)
            .env_runners(num_env_runners=0)
        )
        algo = config.build()
        learner_thread = algo.learner_group._learner._learner_thread
        self.assertTrue(learner_thread.is_alive())

        algo.stop()

        # `Learner.shutdown()` joins the thread, so it must be dead by the
        # time `algo.stop()` returns — no extra `join()` needed here.
        self.assertFalse(learner_thread.is_alive())

    def test_learner_thread_does_not_call_barrier_per_update(self):
        """Tests that the learner thread doesn't sync the Learners after each update.

        With GPU Learners, each `torch.distributed.barrier()` is an NCCL all-reduce
        that leaks host memory, and Ray Train initializes a process group even for a
        single Learner. DDP's gradient all-reduce already keeps Learners in lockstep.
        """
        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .learners(num_learners=0)
            .training(broadcast_interval=1)
        )
        env = gym.make("CartPole-v1")
        learner_group = config.build_learner_group(
            spaces={DEFAULT_MODULE_ID: (env.observation_space, env.action_space)}
        )
        self.addCleanup(learner_group.shutdown)
        learner = learner_group._learner
        # Replace the gradient update in the learner thread with a stub.
        num_updates = []
        learner._learner_thread._update_method = lambda **kwargs: num_updates.append(1)
        batch = SampleBatch(
            {
                Columns.OBS: np.zeros((10, 4), dtype=np.float32),
                Columns.ACTIONS: np.zeros((10,), dtype=np.int64),
            }
        ).as_multi_agent()

        num_calls = 5
        with mock.patch.object(
            torch.distributed, "is_initialized", return_value=True
        ), mock.patch.object(torch.distributed, "barrier") as barrier:
            for _ in range(num_calls):
                # Each call waits for the learner thread's next aggregation.
                learner_group.update(batch=batch, timesteps={})
            for _ in range(500):
                if len(num_updates) >= num_calls:
                    break
                time.sleep(0.01)

        self.assertEqual(len(num_updates), num_calls)
        barrier.assert_not_called()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
