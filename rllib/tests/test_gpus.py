import unittest

import ray
from ray.rllib.agents.a3c.a2c import A2CTrainer, A2C_DEFAULT_CONFIG
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

        config = A2C_DEFAULT_CONFIG.copy()
        config["num_workers"] = 2
        config["env"] = "CartPole-v0"

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
                    config["num_gpus"] = num_gpus
                    config["num_gpus_per_worker"] = num_gpus_per_worker
                    config["_fake_gpus"] = fake_gpus

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
                            # Cannot run through ray.tune.run() as it would
                            # simply wait infinitely for the resources to
                            # become available.
                            print("direct RLlib")
                            self.assertRaisesRegex(
                                RuntimeError,
                                "Found 0 GPUs on your machine",
                                lambda: A2CTrainer(config, env="CartPole-v0"),
                            )
                        # If actual_gpus >= num_gpus or faked,
                        # expect no error.
                        else:
                            print("direct RLlib")
                            trainer = A2CTrainer(config, env="CartPole-v0")
                            trainer.stop()
                            # Cannot run through ray.tune.run() w/ fake GPUs
                            # as it would simply wait infinitely for the
                            # resources to become available (even though, we
                            # wouldn't really need them).
                            if num_gpus == 0:
                                print("via ray.tune.run()")
                                tune.run(
                                    "A2C", config=config, stop={"training_iteration": 0}
                                )
        ray.shutdown()

    def test_gpus_in_local_mode(self):
        # Local mode.
        ray.init(local_mode=True)

        actual_gpus_available = torch.cuda.device_count()

        config = A2C_DEFAULT_CONFIG.copy()
        config["num_workers"] = 2
        config["env"] = "CartPole-v0"

        # Expect no errors in local mode.
        for num_gpus in [0, 0.1, 1, actual_gpus_available + 4]:
            print(f"num_gpus={num_gpus}")
            for fake_gpus in [False, True]:
                print(f"_fake_gpus={fake_gpus}")
                config["num_gpus"] = num_gpus
                config["_fake_gpus"] = fake_gpus
                frameworks = ("tf", "torch") if num_gpus > 1 else ("tf2", "tf", "torch")
                for _ in framework_iterator(config, frameworks=frameworks):
                    print("direct RLlib")
                    trainer = A2CTrainer(config, env="CartPole-v0")
                    trainer.stop()
                    print("via ray.tune.run()")
                    tune.run("A2C", config=config, stop={"training_iteration": 0})

        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
