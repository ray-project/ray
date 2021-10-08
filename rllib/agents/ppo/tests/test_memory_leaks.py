import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.examples.env.random_env import RandomLargeObsSpaceEnv
from ray.rllib.utils.debug.memory import check_memory_leaks
from ray.rllib.utils.test_utils import framework_iterator


class TestMemoryLeaks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

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

        for _ in framework_iterator(config, frameworks="tf2", with_eager_tracing=True):#TODO
            _config = config.copy()
            _config["env"] = RandomLargeObsSpaceEnv
            trainer = ppo.appo.APPOTrainer(config=_config)
            leaks = check_memory_leaks(trainer, to_check="rollout_worker")
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
