import gymnasium as gym
import itertools
import numpy as np
import tempfile
import unittest

import ray
from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig
from ray.rllib.core.testing.utils import get_learner_group
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import check


FAKE_BATCH = {
    SampleBatch.OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    SampleBatch.NEXT_OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    SampleBatch.ACTIONS: np.array([0, 1, 1]),
    SampleBatch.PREV_ACTIONS: np.array([0, 1, 1]),
    SampleBatch.REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
    SampleBatch.PREV_REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
    SampleBatch.TERMINATEDS: np.array([False, False, True]),
    SampleBatch.TRUNCATEDS: np.array([False, False, False]),
    SampleBatch.VF_PREDS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
    SampleBatch.ACTION_DIST_INPUTS: np.array(
        [[-2.0, 0.5], [-3.0, -0.3], [-0.1, 2.5]], dtype=np.float32
    ),
    SampleBatch.ACTION_LOGP: np.array([-0.5, -0.1, -0.2], dtype=np.float32),
    SampleBatch.EPS_ID: np.array([0, 0, 0]),
    SampleBatch.AGENT_INDEX: np.array([0, 0, 0]),
}


REMOTE_SCALING_CONFIGS = {
    "remote-cpu": LearnerGroupScalingConfig(num_workers=1),
    "remote-gpu": LearnerGroupScalingConfig(num_workers=1, num_gpus_per_worker=1),
    "multi-gpu-ddp": LearnerGroupScalingConfig(num_workers=2, num_gpus_per_worker=1),
    "multi-cpu-ddp": LearnerGroupScalingConfig(num_workers=2, num_cpus_per_worker=2),
    # "multi-gpu-ddp-pipeline": LearnerGroupScalingConfig(
    #     num_workers=2, num_gpus_per_worker=2
    # ),
}


class TestLearnerGroupCheckpointing(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_save_load_state(self):
        fws = ["tf2", "torch"]
        scaling_modes = REMOTE_SCALING_CONFIGS.keys()
        test_iterator = itertools.product(fws, scaling_modes)

        batch = SampleBatch(FAKE_BATCH)
        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")

            scaling_config = REMOTE_SCALING_CONFIGS[scaling_mode]
            initial_learner_group = get_learner_group(
                fw, env, scaling_config, eager_tracing=True
            )

            # checkpoint the initial learner state for later comparison
            initial_learner_checkpoint_dir = tempfile.TemporaryDirectory().name
            initial_learner_group.save_state(initial_learner_checkpoint_dir)
            initial_learner_group_weights = initial_learner_group.get_weights()

            # do a single update
            initial_learner_group.update(batch.as_multi_agent(), reduce_fn=None)

            # checkpoint the learner state after 1 update for later comparison
            learner_after_1_update_checkpoint_dir = tempfile.TemporaryDirectory().name
            initial_learner_group.save_state(learner_after_1_update_checkpoint_dir)

            # remove that learner, construct a new one, and load the state of the old
            # learner into the new one
            initial_learner_group.shutdown()
            del initial_learner_group
            new_learner_group = get_learner_group(
                fw, env, scaling_config, eager_tracing=True
            )
            new_learner_group.load_state(learner_after_1_update_checkpoint_dir)

            # do another update
            results_with_break = new_learner_group.update(
                batch.as_multi_agent(), reduce_fn=None
            )
            weights_after_1_update_with_break = new_learner_group.get_weights()
            new_learner_group.shutdown()
            del new_learner_group

            # construct a new learner group and load the initial state of the learner
            learner_group = get_learner_group(
                fw, env, scaling_config, eager_tracing=True
            )
            learner_group.load_state(initial_learner_checkpoint_dir)
            check(learner_group.get_weights(), initial_learner_group_weights)
            learner_group.update(batch.as_multi_agent(), reduce_fn=None)
            results_without_break = learner_group.update(
                batch.as_multi_agent(), reduce_fn=None
            )
            weights_after_1_update_without_break = learner_group.get_weights()
            learner_group.shutdown()
            del learner_group

            # compare the results of the two updates
            check(results_with_break, results_without_break)
            check(
                weights_after_1_update_with_break, weights_after_1_update_without_break
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
