import copy
import unittest

import ray
import ray.rllib.agents.impala as impala
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check, \
    check_compute_single_action, framework_iterator

tf1, tf, tfv = try_import_tf()


class TestIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_compilation(self):
        """Test whether an ImpalaTrainer can be built with both frameworks."""
        config = impala.DEFAULT_CONFIG.copy()
        config["num_gpus"] = 0
        config["model"]["lstm_use_prev_action"] = True
        config["model"]["lstm_use_prev_reward"] = True
        num_iterations = 1
        env = "CartPole-v0"

        for _ in framework_iterator(config):
            local_cfg = config.copy()
            for lstm in [False, True]:
                local_cfg["num_aggregation_workers"] = 0 if not lstm else 1
                local_cfg["model"]["use_lstm"] = lstm
                print("lstm={} aggregation-worker={}".format(
                    lstm, local_cfg["num_aggregation_workers"]))
                # Test with and w/o aggregation workers (this has nothing
                # to do with LSTMs, though).
                trainer = impala.ImpalaTrainer(config=local_cfg, env=env)
                for i in range(num_iterations):
                    print(trainer.train())
                check_compute_single_action(
                    trainer,
                    include_state=lstm,
                    include_prev_action_reward=lstm,
                )
                trainer.stop()

    def test_impala_lr_schedule(self):
        config = impala.DEFAULT_CONFIG.copy()
        config["num_gpus"] = 0
        # Test whether we correctly ignore the "lr" setting.
        # The first lr should be 0.0005.
        config["lr"] = 0.1
        config["lr_schedule"] = [
            [0, 0.0005],
            [10000, 0.000001],
        ]
        config["num_gpus"] = 0  # Do not use any (fake) GPUs.
        config["env"] = "CartPole-v0"

        def get_lr(result):
            return result["info"]["learner"][DEFAULT_POLICY_ID]["cur_lr"]

        for fw in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = impala.ImpalaTrainer(config=config)
            policy = trainer.get_policy()

            try:
                if fw == "tf":
                    check(policy.get_session().run(policy.cur_lr), 0.0005)
                else:
                    check(policy.cur_lr, 0.0005)
                r1 = trainer.train()
                r2 = trainer.train()
                assert get_lr(r2) < get_lr(r1), (r1, r2)
            finally:
                trainer.stop()

    def test_impala_fake_multi_gpu_learning(self):
        """Test whether IMPALATrainer can learn CartPole w/ faked multi-GPU."""
        config = copy.deepcopy(impala.DEFAULT_CONFIG)
        # Fake GPU setup.
        config["_fake_gpus"] = True
        config["num_gpus"] = 2

        config["train_batch_size"] *= 2

        # Test w/ LSTMs.
        config["model"]["use_lstm"] = True

        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = impala.ImpalaTrainer(config=config, env="CartPole-v0")
            num_iterations = 200
            learnt = False
            for i in range(num_iterations):
                results = trainer.train()
                print(results)
                if results["episode_reward_mean"] > 55.0:
                    learnt = True
                    break
            assert learnt, \
                "IMPALA multi-GPU (with fake-GPUs) did not learn CartPole!"
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
