import copy
import unittest

import ray
import ray.rllib.agents.a3c as a3c
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestA2C(unittest.TestCase):
    """Sanity tests for A2C exec impl."""

    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_a2c_compilation(self):
        """Test whether an A2CTrainer can be built with both frameworks."""
        config = a3c.DEFAULT_CONFIG.copy()
        config["num_workers"] = 2
        config["num_envs_per_worker"] = 2

        num_iterations = 1

        # Test against all frameworks.
        for _ in framework_iterator(config):
            for env in ["PongDeterministic-v0"]:
                trainer = a3c.A2CTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    print(results)
                check_compute_single_action(trainer)
                trainer.stop()

    def test_a2c_fake_multi_gpu_learning(self):
        """Test whether A2CTrainer can learn CartPole w/ faked multi-GPU."""
        config = copy.deepcopy(a3c.a2c.A2C_DEFAULT_CONFIG)

        # Fake GPU setup.
        config["num_gpus"] = 2
        config["_fake_gpus"] = True

        config["framework"] = "tf"
        # Mimic tuned_example for A2C CartPole.
        config["lr"] = 0.001

        trainer = a3c.A2CTrainer(config=config, env="CartPole-v0")
        num_iterations = 100
        learnt = False
        for i in range(num_iterations):
            results = trainer.train()
            print("reward={}".format(results["episode_reward_mean"]))
            if results["episode_reward_mean"] > 100.0:
                learnt = True
                break
        assert learnt, "A2C multi-GPU (with fake-GPUs) did not learn CartPole!"
        trainer.stop()

    def test_a2c_exec_impl(ray_start_regular):
        config = {"min_iter_time_s": 0}
        for _ in framework_iterator(config):
            trainer = a3c.A2CTrainer(env="CartPole-v0", config=config)
            assert isinstance(trainer.train(), dict)
            check_compute_single_action(trainer)
            trainer.stop()

    def test_a2c_exec_impl_microbatch(ray_start_regular):
        config = {
            "min_iter_time_s": 0,
            "microbatch_size": 10,
        }
        for _ in framework_iterator(config):
            trainer = a3c.A2CTrainer(env="CartPole-v0", config=config)
            assert isinstance(trainer.train(), dict)
            check_compute_single_action(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
