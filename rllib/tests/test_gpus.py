import copy
import unittest

import numpy as np
import torch

import ray
from ray import air
from ray import tune
from ray.rllib.algorithms.a2c.a2c import A2CConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import framework_iterator


class TestGPUs(unittest.TestCase):
    def test_gpus_in_non_local_mode(self):
        # Non-local mode.
        ray.init()

        actual_gpus = torch.cuda.device_count()
        print(f"Actual GPUs found (by torch): {actual_gpus}")

        config = A2CConfig().rollouts(num_rollout_workers=2).environment("CartPole-v1")

        # Expect errors when we run a config w/ num_gpus>0 w/o a GPU
        # and _fake_gpus=False.
        for num_gpus in [0, 0.1, 1, actual_gpus + 4]:
            # Only allow possible num_gpus_per_worker (so test would not
            # block infinitely due to a down worker).
            per_worker = (
                [0] if actual_gpus == 0 or actual_gpus < num_gpus else [0, 0.5, 1]
            )
            for num_gpus_per_worker in per_worker:
                for fake_gpus in [False] + ([] if num_gpus == 0 else [True]):
                    config.resources(
                        num_gpus=num_gpus,
                        num_gpus_per_worker=num_gpus_per_worker,
                        _fake_gpus=fake_gpus,
                    )

                    print(
                        f"\n------------\nnum_gpus={num_gpus} "
                        f"num_gpus_per_worker={num_gpus_per_worker} "
                        f"_fake_gpus={fake_gpus}"
                    )

                    frameworks = (
                        ("tf", "torch") if num_gpus > 1 else ("tf2", "tf", "torch")
                    )
                    for _ in framework_iterator(config, frameworks=frameworks):
                        # Expect that trainer creation causes a num_gpu error.
                        if (
                            actual_gpus < num_gpus + 2 * num_gpus_per_worker
                            and not fake_gpus
                        ):
                            # "Direct" RLlib (create Trainer on the driver).
                            # Cannot run through ray.tune.Tuner().fit() as it would
                            # simply wait infinitely for the resources to
                            # become available.
                            print("direct RLlib")
                            self.assertRaisesRegex(
                                RuntimeError,
                                "Found 0 GPUs on your machine",
                                lambda: config.build(),
                            )
                        # If actual_gpus >= num_gpus or faked,
                        # expect no error.
                        else:
                            print("direct RLlib")
                            algo = config.build()
                            algo.stop()
                            # Cannot run through ray.tune.Tuner().fit() w/ fake GPUs
                            # as it would simply wait infinitely for the
                            # resources to become available (even though, we
                            # wouldn't really need them).
                            if num_gpus == 0:
                                print("via ray.tune.Tuner().fit()")
                                tune.Tuner(
                                    "A2C",
                                    param_space=config,
                                    run_config=air.RunConfig(
                                        stop={"training_iteration": 0}
                                    ),
                                ).fit()
        ray.shutdown()

    def test_gpus_in_local_mode(self):
        # Local mode.
        ray.init(local_mode=True)

        actual_gpus_available = torch.cuda.device_count()

        config = A2CConfig().rollouts(num_rollout_workers=2).environment("CartPole-v1")

        # Expect no errors in local mode.
        for num_gpus in [0, 0.1, 1, actual_gpus_available + 4]:
            print(f"num_gpus={num_gpus}")
            for fake_gpus in [False, True]:
                print(f"_fake_gpus={fake_gpus}")
                config.resources(num_gpus=num_gpus, _fake_gpus=fake_gpus)
                frameworks = ("tf", "torch") if num_gpus > 1 else ("tf2", "tf", "torch")
                for _ in framework_iterator(config, frameworks=frameworks):
                    print("direct RLlib")
                    algo = config.build()
                    algo.stop()
                    print("via ray.tune.Tuner().fit()")
                    tune.Tuner(
                        "A2C",
                        param_space=config,
                        run_config=air.RunConfig(stop={"training_iteration": 0}),
                    ).fit()

        ray.shutdown()


class TestGPUsLargeBatch(unittest.TestCase):
    """Test for batches larger than GPU memory.

    If this test fails, this can mean one of the following:
    1. The size of the GPU is not fit. It should have a size such that putting the
       complete batch onto it will fail. In this case, adapt SGD_MINIBATCH_SIZE and
       TRAIN_BATCH_SIZE.
    2. Anything else

    """

    # These values make it so that one large minibatch and the optimizer
    # variables can fit onto the device, but the whole sample_batch is already too
    # large for the GPU itself.
    SGD_MINIBATCH_SIZE = int(1e4)
    TRAIN_BATCH_SIZE = int(SGD_MINIBATCH_SIZE * 5e4)

    def test_larger_train_batch_size_multi_gpu_train_one_step(self):
        # Tests that we can use a train batch size larger than GPU memory with our
        # experimental setting `_load_only_minibatch_onto_device` with
        # multi_gpu_train_one_step.

        b = self.TRAIN_BATCH_SIZE

        # Fake CartPole episode of n time steps.
        CARTPOLE_FAKE_BATCH = SampleBatch(
            {
                SampleBatch.OBS: np.zeros((b, 4), dtype=np.float32),
                SampleBatch.ACTIONS: np.zeros((b,), dtype=np.float32),
                SampleBatch.PREV_ACTIONS: np.zeros((b,), dtype=np.float32),
                SampleBatch.REWARDS: np.zeros((b,), dtype=np.float32),
                SampleBatch.PREV_REWARDS: np.zeros((b,), dtype=np.float32),
                "value_targets": np.zeros((b,), dtype=np.float32),
                SampleBatch.TERMINATEDS: np.array([False] * b),
                SampleBatch.TRUNCATEDS: np.array([False] * b),
                "advantages": np.zeros((b,), dtype=np.float32),
                SampleBatch.VF_PREDS: np.zeros((b,), dtype=np.float32),
                SampleBatch.ACTION_DIST_INPUTS: np.zeros((b, 2), dtype=np.float32),
                SampleBatch.ACTION_LOGP: np.zeros((b,), dtype=np.float32),
                SampleBatch.EPS_ID: np.zeros((b,), dtype=np.int64),
                SampleBatch.AGENT_INDEX: np.zeros((b,), dtype=np.int64),
            }
        )

        # Fail if GPU possibly has enough memory to fit whole batch
        if torch.cuda.get_device_properties(0).total_memory > 1.6e+10:
            raise ValueError("This test was calibrated to fail on a Tesla T4 (16GB GPU memory). When run on a larger device, this test might not fail and needs to be recallibrated.")

        # The following should raise a torch.cuda.OutOfMemoryError (can be used to recallibrate this test)
        # batch_copy = copy.deepcopy(CARTPOLE_FAKE_BATCH)
        # batch_copy.to_device(0)

        config = (
            PPOConfig()
            .environment(env="CartPole-v1")
            .framework("torch")
            .resources(num_gpus=1)
            .rollouts(num_rollout_workers=0)
            .training(
                train_batch_size=b,
                num_sgd_iter=1,
                sgd_minibatch_size=self.SGD_MINIBATCH_SIZE,
            )
            .experimental(
                # This setting makes it so that we don't load a batch of
                # size `train_batch_size` onto the device, but only
                # minibatches.
                _load_only_minibatch_onto_device=True,
            )
        )

        algorithm = config.build()
        policy = algorithm.get_policy()

        policy.load_batch_into_buffer(CARTPOLE_FAKE_BATCH)
        policy.learn_on_loaded_batch()


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
