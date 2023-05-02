import copy
import unittest

import numpy as np
import torch

import ray
from ray import air
from ray import tune
from ray.rllib.algorithms.a2c.a2c import A2CConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.qmix import QMixConfig
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
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
    def test_larger_train_batch_size_multi_gpu_train_one_step(self):
        # Tests that we can use a `train_batch_size` larger than GPU memory with our
        # experimental setting `_load_only_minibatch_onto_device` with
        # multi_gpu_train_one_step.

        # These values make it so that one large minibatch and the optimizer
        # variables can fit onto the device, but the whole sample_batch is already too
        # large for the GPU itself.
        sgd_minibatch_size = int(1e4)
        train_batch_size = int(sgd_minibatch_size * 1e5)

        # Fake CartPole episode of n time steps.
        CARTPOLE_FAKE_BATCH = SampleBatch(
            {
                SampleBatch.OBS: np.zeros((train_batch_size, 4), dtype=np.float32),
                SampleBatch.ACTIONS: np.zeros((train_batch_size,), dtype=np.float32),
                SampleBatch.PREV_ACTIONS: np.zeros(
                    (train_batch_size,), dtype=np.float32
                ),
                SampleBatch.REWARDS: np.zeros((train_batch_size,), dtype=np.float32),
                SampleBatch.PREV_REWARDS: np.zeros(
                    (train_batch_size,), dtype=np.float32
                ),
                "value_targets": np.zeros((train_batch_size,), dtype=np.float32),
                SampleBatch.TERMINATEDS: np.array([False] * train_batch_size),
                SampleBatch.TRUNCATEDS: np.array([False] * train_batch_size),
                "advantages": np.zeros((train_batch_size,), dtype=np.float32),
                SampleBatch.VF_PREDS: np.zeros((train_batch_size,), dtype=np.float32),
                SampleBatch.ACTION_DIST_INPUTS: np.zeros(
                    (train_batch_size, 2), dtype=np.float32
                ),
                SampleBatch.ACTION_LOGP: np.zeros(
                    (train_batch_size,), dtype=np.float32
                ),
                SampleBatch.EPS_ID: np.zeros((train_batch_size,), dtype=np.int64),
                SampleBatch.AGENT_INDEX: np.zeros((train_batch_size,), dtype=np.int64),
            }
        )

        # Test if we can even fail this test due too a GPU OOM
        try:
            batch_copy = copy.deepcopy(CARTPOLE_FAKE_BATCH)
            batch_copy.to_device(0)
            raise ValueError(
                "We should not be able to move this batch to the device. "
                "If this error occurs, this means that this test cannot fail "
                "inside multi_gpu_train_one_step."
            )
        except torch.cuda.OutOfMemoryError:
            pass

        for config_class in (PPOConfig, QMixConfig):
            config = (
                config_class()
                .environment(env="CartPole-v1")
                .framework("torch")
                .resources(num_gpus=1)
                .rollouts(num_rollout_workers=0)
                .training(
                    train_batch_size=train_batch_size,
                    num_sgd_iter=1,
                    sgd_minibatch_size=self.sgd_minibatch_size,
                    # This setting makes it so that we don't load a batch of
                    # size `train_batch_size` onto the device, but only
                    # minibatches.
                    _load_only_minibatch_onto_device=True,
                )
            )

            algorithm = config.build()
            policy = algorithm.get_policy()

            # Sanity check if we are covering both, TorchPolicy and TorchPolicyV2
            if config_class is QMixConfig:
                assert isinstance(policy, TorchPolicy)
            elif config_class is PPOConfig:
                assert isinstance(policy, TorchPolicyV2)

            policy.load_batch_into_buffer(CARTPOLE_FAKE_BATCH)
            policy.learn_on_loaded_batch()


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
