import copy
import numpy as np
import unittest

import ray
import ray.rllib.agents.pg as pg
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import check, check_compute_single_action, fc, \
    framework_iterator


class TestPG(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_pg_compilation(self):
        """Test whether a PGTrainer can be built with both frameworks."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0
        num_iterations = 2

        for fw in framework_iterator(config):
            # For tf, build with fake-GPUs.
            config["_fake_gpus"] = fw == "tf"
            config["num_gpus"] = 2 if fw == "tf" else 0
            trainer = pg.PGTrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                print(trainer.train())
            check_compute_single_action(
                trainer, include_prev_action_reward=True)

    def test_pg_fake_multi_gpu_learning(self):
        """Test whether PGTrainer can learn CartPole w/ faked multi-GPU."""
        config = copy.deepcopy(pg.DEFAULT_CONFIG)

        # Fake GPU setup.
        config["num_gpus"] = 2
        config["_fake_gpus"] = True

        config["framework"] = "tf"
        # Mimic tuned_example for PG CartPole.
        config["model"]["fcnet_hiddens"] = [64]
        config["model"]["fcnet_activation"] = "linear"

        trainer = pg.PGTrainer(config=config, env="CartPole-v0")
        num_iterations = 200
        learnt = False
        for i in range(num_iterations):
            results = trainer.train()
            print("reward={}".format(results["episode_reward_mean"]))
            # Make this test quite short (75.0).
            if results["episode_reward_mean"] > 75.0:
                learnt = True
                break
        assert learnt, "PG multi-GPU (with fake-GPUs) did not learn CartPole!"
        trainer.stop()

    def test_pg_loss_functions(self):
        """Tests the PG loss function math."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["gamma"] = 0.99
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = "linear"

        # Fake CartPole episode of n time steps.
        train_batch = {
            SampleBatch.OBS: np.array([[0.1, 0.2, 0.3,
                                        0.4], [0.5, 0.6, 0.7, 0.8],
                                       [0.9, 1.0, 1.1, 1.2]]),
            SampleBatch.ACTIONS: np.array([0, 1, 1]),
            SampleBatch.REWARDS: np.array([1.0, 1.0, 1.0]),
            SampleBatch.DONES: np.array([False, False, True]),
            SampleBatch.EPS_ID: np.array([1234, 1234, 1234]),
            SampleBatch.AGENT_INDEX: np.array([0, 0, 0]),
        }

        for fw, sess in framework_iterator(config, session=True):
            dist_cls = (Categorical if fw != "torch" else TorchCategorical)
            trainer = pg.PGTrainer(config=config, env="CartPole-v0")
            policy = trainer.get_policy()
            vars = policy.model.trainable_variables()
            if sess:
                vars = policy.get_session().run(vars)

            # Post-process (calculate simple (non-GAE) advantages) and attach
            # to train_batch dict.
            # A = [0.99^2 * 1.0 + 0.99 * 1.0 + 1.0, 0.99 * 1.0 + 1.0, 1.0] =
            # [2.9701, 1.99, 1.0]
            train_batch_ = pg.post_process_advantages(policy,
                                                      train_batch.copy())
            if fw == "torch":
                train_batch_ = policy._lazy_tensor_dict(train_batch_)

            # Check Advantage values.
            check(train_batch_[Postprocessing.ADVANTAGES], [2.9701, 1.99, 1.0])

            # Actual loss results.
            if sess:
                results = policy.get_session().run(
                    policy._loss,
                    feed_dict=policy._get_loss_inputs_dict(
                        train_batch_, shuffle=False))
            else:
                results = (pg.pg_tf_loss
                           if fw in ["tf2", "tfe"] else pg.pg_torch_loss)(
                               policy,
                               policy.model,
                               dist_class=dist_cls,
                               train_batch=train_batch_)

            # Calculate expected results.
            if fw != "torch":
                expected_logits = fc(
                    fc(train_batch_[SampleBatch.OBS],
                       vars[0],
                       vars[1],
                       framework=fw),
                    vars[2],
                    vars[3],
                    framework=fw)
            else:
                expected_logits = fc(
                    fc(train_batch_[SampleBatch.OBS],
                       vars[2],
                       vars[3],
                       framework=fw),
                    vars[0],
                    vars[1],
                    framework=fw)
            expected_logp = dist_cls(expected_logits, policy.model).logp(
                train_batch_[SampleBatch.ACTIONS])
            adv = train_batch_[Postprocessing.ADVANTAGES]
            if sess:
                expected_logp = sess.run(expected_logp)
            elif fw == "torch":
                expected_logp = expected_logp.detach().cpu().numpy()
                adv = adv.detach().cpu().numpy()
            else:
                expected_logp = expected_logp.numpy()
            expected_loss = -np.mean(expected_logp * adv)
            check(results, expected_loss, decimals=4)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
