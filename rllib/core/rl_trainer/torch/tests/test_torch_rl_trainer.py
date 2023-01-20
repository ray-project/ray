import gymnasium as gym
import unittest
import torch
import numpy as np

import ray

from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.torch.bc_rl_trainer import BCTorchRLTrainer
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader
from ray.rllib.utils.numpy import convert_to_numpy

from ray.air.config import ScalingConfig


def _get_trainer(scaling_config=None, distributed: bool = False) -> RLTrainer:
    env = gym.make("CartPole-v1")
    scaling_config = scaling_config or ScalingConfig()
    distributed = False

    # TODO: Another way to make RLTrainer would be to construct the module first
    # and then apply trainer to it. We should also allow that. In fact if we figure
    # out the serialization of RLModules we can simply pass the module the trainer
    # and internally it will serialize and deserialize the module for distributed
    # construction.
    trainer = BCTorchRLTrainer(
        module_class=DiscreteBCTorchModule,
        module_kwargs={
            "observation_space": env.observation_space,
            "action_space": env.action_space,
            "model_config": {"hidden_dim": 32},
        },
        scaling_config=scaling_config,
        optimizer_config={"lr": 1e-3},
        distributed=distributed,
    )

    trainer.build()

    return trainer


class TestRLTrainer(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_end_to_end_update(self):

        trainer = _get_trainer(scaling_config=ScalingConfig(num_workers=2))
        reader = get_cartpole_dataset_reader(batch_size=512)

        min_loss = float("inf")
        for iter_i in range(1000):
            batch = reader.next()
            results = trainer.update(batch.as_multi_agent())

            loss = results["loss"]["total_loss"]
            min_loss = min(loss, min_loss)
            print(f"[iter = {iter_i}] Loss: {loss:.3f}, Min Loss: {min_loss:.3f}")
            # The loss is initially around 0.69 (ln2). When it gets to around
            # 0.57 the return of the policy gets to around 100.
            if min_loss < 0.57:
                break
        self.assertLess(min_loss, 0.57)

    def test_compute_gradients(self):
        """Tests the compute_gradients correctness.

        Tests that if we sum all the trainable variables the gradient of output w.r.t.
        the weights is all ones.
        """
        trainer = _get_trainer(scaling_config=ScalingConfig(num_workers=2))

        params = trainer.get_parameters(trainer.module[DEFAULT_POLICY_ID])
        loss = {"total_loss": sum([param.sum() for param in params])}
        gradients = trainer.compute_gradients(loss)

        # type should be a mapping from ParamRefs to gradients
        self.assertIsInstance(gradients, dict)

        for grad in gradients.values():
            check(grad, np.ones(grad.shape))

    def test_apply_gradients(self):
        """Tests the apply_gradients correctness.

        Tests that if we apply gradients of all ones, the new params are equal to the
        standard SGD/Adam update rule.
        """

        trainer = _get_trainer(scaling_config=ScalingConfig(num_workers=2))

        # calculated the expected new params based on gradients of all ones.
        params = trainer.get_parameters(trainer.module[DEFAULT_POLICY_ID])
        n_steps = 100
        expected = [
            convert_to_numpy(param)
            - n_steps * trainer.optimizer_config["lr"] * np.ones(param.shape)
            for param in params
        ]
        for _ in range(n_steps):
            gradients = {trainer.get_param_ref(p): torch.ones_like(p) for p in params}
            trainer.apply_gradients(gradients)

        check(params, expected)

    def test_add_remove_module(self):
        """Tests the compute/apply_gradients with add/remove modules.

        Tests that if we add a module with SGD optimizer with a known lr (different
        from default), and remove the default module, with a loss that is the sum of
        all variables the updated parameters follow the SGD update rule.
        """
        env = gym.make("CartPole-v1")
        trainer = _get_trainer(scaling_config=ScalingConfig(num_workers=2))

        # add a test module with SGD optimizer with a known lr
        lr = 1e-4

        def set_optimizer_fn(module):
            return [(module.parameters(), torch.optim.Adam(module.parameters(), lr=lr))]

        trainer.add_module(
            module_id="test",
            module_cls=DiscreteBCTorchModule,
            module_kwargs={
                "observation_space": env.observation_space,
                "action_space": env.action_space,
                # the hidden size is different than the default module
                "model_config": {"hidden_dim": 16},
            },
            set_optimizer_fn=set_optimizer_fn,
        )

        trainer.remove_module(DEFAULT_POLICY_ID)

        # only test module should be left
        self.assertEqual(set(trainer.module.keys()), {"test"})

        # calculated the expected new params based on gradients of all ones.
        params = trainer.get_parameters(trainer.module["test"])
        n_steps = 100
        expected = [
            convert_to_numpy(param) - n_steps * lr * np.ones(param.shape)
            for param in params
        ]
        for _ in range(n_steps):
            loss = {"total_loss": sum([param.sum() for param in params])}
            gradients = trainer.compute_gradients(loss)
            trainer.apply_gradients(gradients)

        check(params, expected)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
