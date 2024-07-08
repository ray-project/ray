import unittest

import ray
from ray import air
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import framework_iterator
from ray import tune

torch, _ = try_import_torch()


class TestGPUs(unittest.TestCase):
    def test_gpus_in_non_local_mode(self):
        # Non-local mode.
        ray.init()

        actual_gpus = torch.cuda.device_count()
        print(f"Actual GPUs found (by torch): {actual_gpus}")

        config = PPOConfig().env_runners(num_env_runners=2).environment("CartPole-v1")

        # Expect errors when we run a config w/ num_gpus>0 w/o a GPU
        # and _fake_gpus=False.
        for num_gpus in [0, 0.1, 1, actual_gpus + 4]:
            # Only allow possible num_gpus_per_env_runner (so test would not
            # block infinitely due to a down worker).
            per_worker = (
                [0] if actual_gpus == 0 or actual_gpus < num_gpus else [0, 0.5, 1]
            )
            for num_gpus_per_env_runner in per_worker:
                for fake_gpus in [False] + ([] if num_gpus == 0 else [True]):
                    config.resources(
                        num_gpus=num_gpus,
                        _fake_gpus=fake_gpus,
                    )
                    config.env_runners(num_gpus_per_env_runner=num_gpus_per_env_runner)

                    print(
                        f"\n------------\nnum_gpus={num_gpus} "
                        f"num_gpus_per_env_runner={num_gpus_per_env_runner} "
                        f"_fake_gpus={fake_gpus}"
                    )

                    frameworks = (
                        ("tf", "torch") if num_gpus > 1 else ("tf2", "tf", "torch")
                    )
                    for _ in framework_iterator(config, frameworks=frameworks):
                        # Expect that Algorithm creation causes a num_gpu error.
                        if (
                            actual_gpus < num_gpus + 2 * num_gpus_per_env_runner
                            and not fake_gpus
                        ):
                            # "Direct" RLlib (create Algorithm on the driver).
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
                                    "PPO",
                                    param_space=config,
                                    run_config=air.RunConfig(
                                        stop={TRAINING_ITERATION: 0}
                                    ),
                                ).fit()
        ray.shutdown()

    def test_gpus_in_local_mode(self):
        # Local mode.
        ray.init(local_mode=True)

        actual_gpus_available = torch.cuda.device_count()

        config = PPOConfig().env_runners(num_env_runners=2).environment("CartPole-v1")

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
                        "PPO",
                        param_space=config,
                        run_config=air.RunConfig(stop={TRAINING_ITERATION: 0}),
                    ).fit()

        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
