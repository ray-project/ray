import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.examples.env.random_env import RandomLargeObsSpaceEnv
from ray.rllib.utils.debug.memory import check_memory_leaks
from ray.rllib.utils.test_utils import framework_iterator


class TestMemoryLeaks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dummy_code(self):
        from ray.rllib.utils.debug.memory import _test_some_code_for_memory_leaks

        a = []

        def dumb_code():
            nonlocal a
            a.append(42)  # expect 8 bytes (int64) per detection

        stats = _test_some_code_for_memory_leaks(
            "dumb_code", None, dumb_code, 200, max_num_trials=3
        )
        self.assertTrue(len(stats) == 1)

    def test_ppo_memory_leaks(self):
        """Test whether an APPOTrainer can be built with both frameworks."""
        config = ppo.DEFAULT_CONFIG.copy()
        # Make sure we have an env to test on the local worker.
        # Otherwise, `check_memory_leaks` will complain.
        config["create_env_on_driver"] = True

        for _ in framework_iterator(config):
            _config = config.copy()
            _config["env"] = RandomLargeObsSpaceEnv
            trainer = ppo.PPOTrainer(config=_config)
            check_memory_leaks(trainer)
            trainer.stop()

    def test_appo_memory_leaks(self):
        """Test whether an APPOTrainer can be built with both frameworks."""
        config = ppo.appo.DEFAULT_CONFIG.copy()
        # Make sure we have an env to test on the local worker.
        # Otherwise, `check_memory_leaks` will complain.
        config["create_env_on_driver"] = True

        for _ in framework_iterator(
            config, frameworks="torch"
        ):  # , with_eager_tracing=True):#TODO
            _config = config.copy()
            _config["env"] = RandomLargeObsSpaceEnv
            trainer = ppo.appo.APPOTrainer(config=_config)
            leaks = check_memory_leaks(trainer, to_check="policy")
            if leaks:
                for l in leaks:
                    print(l)
            assert not leaks
            trainer.stop()

    def test_ddppo_memory_leaks(self):
        """Test whether an APPOTrainer can be built with both frameworks."""
        config = ppo.ddppo.DEFAULT_CONFIG.copy()
        # Make sure we have an env to test on the local worker.
        # Otherwise, `check_memory_leaks` will complain.
        config["create_env_on_driver"] = True

        for _ in framework_iterator(config):
            _config = config.copy()
            _config["env"] = RandomLargeObsSpaceEnv
            trainer = ppo.ddppo.DDPPOTrainer(config=_config)
            check_memory_leaks(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
