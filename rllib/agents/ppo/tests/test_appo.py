import copy
import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestAPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_appo_compilation(self):
        """Test whether an APPOTrainer can be built with both frameworks."""
        config = ppo.appo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 1
        num_iterations = 2

        for _ in framework_iterator(config):
            print("w/o v-trace")
            _config = config.copy()
            _config["vtrace"] = False
            trainer = ppo.APPOTrainer(config=_config, env="CartPole-v0")
            for i in range(num_iterations):
                print(trainer.train())
            check_compute_single_action(trainer)
            trainer.stop()

            print("w/ v-trace")
            _config = config.copy()
            _config["vtrace"] = True
            trainer = ppo.APPOTrainer(config=_config, env="CartPole-v0")
            for i in range(num_iterations):
                print(trainer.train())
            check_compute_single_action(trainer)
            trainer.stop()

    def test_appo_fake_multi_gpu_learning(self):
        """Test whether APPOTrainer can learn CartPole w/ faked multi-GPU."""
        config = copy.deepcopy(ppo.appo.DEFAULT_CONFIG)
        # Fake GPU setup.
        config["num_gpus"] = 2
        config["_fake_gpus"] = True
        # Mimic tuned_example for PPO CartPole.
        config["num_workers"] = 1
        config["lr"] = 0.0003
        config["observation_filter"] = "MeanStdFilter"
        config["num_sgd_iter"] = 6
        config["vf_loss_coeff"] = 0.01
        config["model"]["fcnet_hiddens"] = [32]
        config["model"]["fcnet_activation"] = "linear"
        config["model"]["vf_share_layers"] = True

        # Test w/ LSTMs.
        config["model"]["use_lstm"] = True

        # Double batch size (2 GPUs).
        config["train_batch_size"] = 1000

        for _ in framework_iterator(config, frameworks=("torch", "tf")):
            trainer = ppo.appo.APPOTrainer(config=config, env="CartPole-v0")
            num_iterations = 200
            learnt = False
            for i in range(num_iterations):
                results = trainer.train()
                print(results)
                if results["episode_reward_mean"] > 65.0:
                    learnt = True
                    break
            assert learnt, \
                "APPO multi-GPU (with fake-GPUs) did not learn CartPole!"
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
