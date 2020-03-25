import numpy as np
import unittest

import ray
import ray.rllib.agents.pg as pg
from ray.rllib.agents.pg import PGTrainer
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import check, fc


class TestPG(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_pg_exec_impl(ray_start_regular):
        trainer = PGTrainer(
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

        # tf.
        trainer = pg.PGTrainer(config=config, env="CartPole-v0")

        num_iterations = 2
        for i in range(num_iterations):
            trainer.train()

        # Torch.
        config["use_pytorch"] = True
        trainer = pg.PGTrainer(config=config, env="CartPole-v0")
        for i in range(num_iterations):
            trainer.train()

    def test_pg_loss_functions(self):
        """Tests the PG loss function math."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["eager"] = True
        config["gamma"] = 0.99
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = "linear"

        # Fake CartPole episode of n time steps.
        train_batch = {
            SampleBatch.CUR_OBS: np.array([[0.1, 0.2, 0.3,
                                            0.4], [0.5, 0.6, 0.7, 0.8],
                                           [0.9, 1.0, 1.1, 1.2]]),
            SampleBatch.ACTIONS: np.array([0, 1, 1]),
            SampleBatch.REWARDS: np.array([1.0, 1.0, 1.0]),
            SampleBatch.DONES: np.array([False, False, True])
        }

        # tf.
        trainer = pg.PGTrainer(config=config, env="CartPole-v0")
        policy = trainer.get_policy()
        vars = policy.model.trainable_variables()

        # Post-process (calculate simple (non-GAE) advantages) and attach to
        # train_batch dict.
        # A = [0.99^2 * 1.0 + 0.99 * 1.0 + 1.0, 0.99 * 1.0 + 1.0, 1.0] =
        # [2.9701, 1.99, 1.0]
        train_batch = pg.post_process_advantages(policy, train_batch)
        # Check Advantage values.
        check(train_batch[Postprocessing.ADVANTAGES], [2.9701, 1.99, 1.0])

        # Actual loss results.
        results = pg.pg_tf_loss(
            policy,
            policy.model,
            dist_class=Categorical,
            train_batch=train_batch)

        # Calculate expected results.
        expected_logits = fc(
            fc(train_batch[SampleBatch.CUR_OBS], vars[0].numpy(),
               vars[1].numpy()), vars[2].numpy(), vars[3].numpy())
        expected_logp = Categorical(expected_logits, policy.model).logp(
            train_batch[SampleBatch.ACTIONS])
        expected_loss = -np.mean(
            expected_logp * train_batch[Postprocessing.ADVANTAGES])
        check(results.numpy(), expected_loss, decimals=4)

        # Torch.
        config["use_pytorch"] = True
        trainer = pg.PGTrainer(config=config, env="CartPole-v0")
        policy = trainer.get_policy()
        train_batch = policy._lazy_tensor_dict(train_batch)
        results = pg.pg_torch_loss(
            policy,
            policy.model,
            dist_class=TorchCategorical,
            train_batch=train_batch)
        expected_logits = policy.model.last_output()
        expected_logp = TorchCategorical(expected_logits, policy.model).logp(
            train_batch[SampleBatch.ACTIONS])
        expected_loss = -np.mean(
            expected_logp.detach().numpy() *
            train_batch[Postprocessing.ADVANTAGES].numpy())
        check(results.detach().numpy(), expected_loss, decimals=4)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
