import unittest

import ray
from ray.rllib.agents.pg import PGTrainer, DEFAULT_CONFIG
from ray.rllib.utils.test_utils import framework_iterator


class TestGPUs(unittest.TestCase):
    def test_non_existing_gpus_in_non_local_mode(self):
        # Non-local mode.
        ray.init()
        config = DEFAULT_CONFIG.copy()
        config["num_workers"] = 2

        # Expect errors when we run a config w/ num_gpus>0 w/o a GPU
        # and _fake_gpus=False.
        for num_gpus in [0, 0.1, 1, 4]:
            print(f"num_gpus={num_gpus}")
            for fake_gpus in [False, True]:
                print(f"_fake_gpus={fake_gpus}")
                config["num_gpus"] = num_gpus
                config["_fake_gpus"] = fake_gpus
                frameworks = ("tf", "torch") if num_gpus > 1 else \
                    ("tf2", "tf", "torch")
                for _ in framework_iterator(config, frameworks=frameworks):
                    # Expect that trainer creation causes a num_gpu error.
                    if num_gpus != 0 and not fake_gpus:
                        self.assertRaisesRegexp(
                            RuntimeError,
                            f"Not enough GPUs found.+for num_gpus={num_gpus}",
                            lambda: PGTrainer(config, env="CartPole-v0"),
                        )
                    else:
                        trainer = PGTrainer(config, env="CartPole-v0")
                        trainer.stop()
        ray.shutdown()

    def test_non_existing_gpus_in_local_mode(self):
        # Local mode.
        ray.init(local_mode=True)
        config = DEFAULT_CONFIG.copy()
        config["num_workers"] = 2

        # Expect no errors in local mode.
        for num_gpus in [0, 0.1, 1, 4]:
            print(f"num_gpus={num_gpus}")
            for fake_gpus in [False, True]:
                print(f"_fake_gpus={fake_gpus}")
                config["num_gpus"] = num_gpus
                config["_fake_gpus"] = fake_gpus
                frameworks = ("tf", "torch") if num_gpus > 1 else \
                    ("tf2", "tf", "torch")
                for _ in framework_iterator(config, frameworks=frameworks):
                    trainer = PGTrainer(config, env="CartPole-v0")
                    trainer.stop()
        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
