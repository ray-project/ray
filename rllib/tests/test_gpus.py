import unittest

import ray
from ray.rllib.agents.pg import PGTrainer, DEFAULT_CONFIG
from ray.rllib.utils.test_utils import framework_iterator


class TestGPUs(unittest.TestCase):
    def test_non_existing_gpus_in_non_local_mode(self):
        # Non-local mode.
        ray.init()
        conf = DEFAULT_CONFIG.copy()
        conf["num_workers"] = 2

        # Expect errors when we run this config (num_gpus>0) w/o a GPU.
        conf["num_gpus"] = 1
        for _ in framework_iterator(conf):
            # Expect that trainer creation causes a num_gpu error.
            self.assertRaisesRegexp(
                RuntimeError,
                "Not enough GPUs found.+for num_gpus=1",
                lambda: PGTrainer(conf, env="CartPole-v0"),
            )

        # Same for 2 GPUs.
        conf["num_gpus"] = 2
        for _ in framework_iterator(conf, frameworks=("tf", "torch")):
            # Expect that trainer creation causes a num_gpu error.
            self.assertRaisesRegexp(
                RuntimeError,
                "Not enough GPUs found.+for num_gpus=2",
                lambda: PGTrainer(conf, env="Pendulum-v0"),
            )
        ray.shutdown()

    def test_non_existing_gpus_in_local_mode(self):
        # Local mode.
        ray.init(local_mode=True)
        conf = DEFAULT_CONFIG.copy()
        conf["num_workers"] = 2

        # Expect no errors when we run this config (num_gpus>0) w/o a GPU.
        conf["num_gpus"] = 1
        for _ in framework_iterator(conf):
            # Expect that trainer creation causes a num_gpu error.
            trainer = PGTrainer(conf, env="CartPole-v0")
            trainer.stop()
        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
