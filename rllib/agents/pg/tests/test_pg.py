import numpy as np
import unittest

import ray
import ray.rllib.agents.pg as pg
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import check, fc, framework_iterator


class TestPG(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_pg_exec_impl(ray_start_regular):
        trainer = pg.PGTrainer(
            env="CartPole-v0",
            config={
                "min_iter_time_s": 0,
                "use_exec_api": True
            })
        assert isinstance(trainer.train(), dict)

    def test_pg_compilation(self):
        """Test whether a PGTrainer can be built with both frameworks."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        num_iterations = 2

        for _ in framework_iterator(config):
            trainer = pg.PGTrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                trainer.train()

    def test_pg_loss_functions(self):
        """Tests the PG loss function math."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["gamma"] = 0.99
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = "linear"

        # Fake CartPole episode of n time steps.
        train_batch = {
            SampleBatch.CUR_OBS: np.array([[0.1, 0.2, 0.3,
                                            0.4], [0.5, 0.6, 0.7, 0.8],
                                           [0.9, 1.0, 1.1, 1.2]]),
            SampleBatch.ACTIONS: np.array([0, 1, 1]),
            SampleBatch.PREV_ACTIONS: np.array([1, 0, 1]),
            SampleBatch.REWARDS: np.array([1.0, 1.0, 1.0]),
            SampleBatch.PREV_REWARDS: np.array([-1.0, -1.0, -1.0]),
            SampleBatch.DONES: np.array([False, False, True])
        }

        for fw, sess in framework_iterator(config, session=True):
            dist_cls = (Categorical if fw != "torch" else TorchCategorical)
            trainer = pg.PGTrainer(config=config, env="CartPole-v0")
            policy = trainer.get_policy()
            vars = policy.model.trainable_variables()
            if fw == "tf":
                vars = policy.get_session().run(vars)

            # Post-process (calculate simple (non-GAE) advantages) and attach
            # to train_batch dict.
            # A = [0.99^2 * 1.0 + 0.99 * 1.0 + 1.0, 0.99 * 1.0 + 1.0, 1.0] =
            # [2.9701, 1.99, 1.0]
            train_batch = pg.post_process_advantages(policy, train_batch)
            if fw == "torch":
                train_batch = policy._lazy_tensor_dict(train_batch)

            # Check Advantage values.
            check(train_batch[Postprocessing.ADVANTAGES], [2.9701, 1.99, 1.0])

            # Actual loss results.
            if fw == "tf":
                results = policy.get_session().run(
                    policy._loss,
                    feed_dict=policy._get_loss_inputs_dict(
                        train_batch, shuffle=False))
            else:
                results = (pg.pg_tf_loss
                           if fw == "eager" else pg.pg_torch_loss)(
                               policy,
                               policy.model,
                               dist_class=dist_cls,
                               train_batch=train_batch)

            # Calculate expected results.
            if fw != "torch":
                expected_logits = fc(
                    fc(train_batch[SampleBatch.CUR_OBS],
                       vars[0],
                       vars[1],
                       framework=fw),
                    vars[2],
                    vars[3],
                    framework=fw)
            else:
                expected_logits = fc(
                    fc(train_batch[SampleBatch.CUR_OBS],
                       vars[2],
                       vars[3],
                       framework=fw),
                    vars[0],
                    vars[1],
                    framework=fw)
            expected_logp = dist_cls(expected_logits, policy.model).logp(
                train_batch[SampleBatch.ACTIONS])
            if sess:
                expected_logp = sess.run(expected_logp)
            else:
                expected_logp = expected_logp.numpy()
            expected_loss = -np.mean(
                expected_logp *
                (train_batch[Postprocessing.ADVANTAGES] if fw != "torch" else
                 train_batch[Postprocessing.ADVANTAGES].numpy()))
            check(results, expected_loss, decimals=4)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
