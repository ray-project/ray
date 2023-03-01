import gymnasium as gym
import unittest
import torch
import numpy as np

import ray

from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.core.testing.utils import get_learner


def _get_learner() -> Learner:
    env = gym.make("CartPole-v1")
    learner = get_learner("torch", env)
    learner.build()

    return learner


class TestLearner(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_end_to_end_update(self):

        learner = _get_learner()
        reader = get_cartpole_dataset_reader(batch_size=512)

        min_loss = float("inf")
        for iter_i in range(1000):
            batch = reader.next()
            results = learner.update(batch.as_multi_agent())

            loss = results[ALL_MODULES]["total_loss"]
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
        learner = _get_learner()

        params = learner.get_parameters(learner.module[DEFAULT_POLICY_ID])
        loss = {"total_loss": sum([param.sum() for param in params])}
        gradients = learner.compute_gradients(loss)

        # type should be a mapping from ParamRefs to gradients
        self.assertIsInstance(gradients, dict)

        for grad in gradients.values():
            check(grad, np.ones(grad.shape))

    def test_apply_gradients(self):
        """Tests the apply_gradients correctness.

        Tests that if we apply gradients of all ones, the new params are equal to the
        standard SGD/Adam update rule.
        """

        learner = _get_learner()

        # calculated the expected new params based on gradients of all ones.
        params = learner.get_parameters(learner.module[DEFAULT_POLICY_ID])
        n_steps = 100
        expected = [
            convert_to_numpy(param)
            - n_steps * learner._optimizer_config["lr"] * np.ones(param.shape)
            for param in params
        ]
        for _ in range(n_steps):
            gradients = {learner.get_param_ref(p): torch.ones_like(p) for p in params}
            learner.apply_gradients(gradients)

        check(params, expected)

    def test_add_remove_module(self):
        """Tests the compute/apply_gradients with add/remove modules.

        Tests that if we add a module with SGD optimizer with a known lr (different
        from default), and remove the default module, with a loss that is the sum of
        all variables the updated parameters follow the SGD update rule.
        """
        env = gym.make("CartPole-v1")
        learner = _get_learner()

        # add a test module with SGD optimizer with a known lr
        lr = 1e-4

        def set_optimizer_fn(module):
            return [(module.parameters(), torch.optim.Adam(module.parameters(), lr=lr))]

        learner.add_module(
            module_id="test",
            module_spec=SingleAgentRLModuleSpec(
                module_class=DiscreteBCTorchModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config={"fcnet_hiddens": [16]},
            ),
            set_optimizer_fn=set_optimizer_fn,
        )

        learner.remove_module(DEFAULT_POLICY_ID)

        # only test module should be left
        self.assertEqual(set(learner.module.keys()), {"test"})

        # calculated the expected new params based on gradients of all ones.
        params = learner.get_parameters(learner.module["test"])
        n_steps = 100
        expected = [
            convert_to_numpy(param) - n_steps * lr * np.ones(param.shape)
            for param in params
        ]
        for _ in range(n_steps):
            loss = {"total_loss": sum([param.sum() for param in params])}
            gradients = learner.compute_gradients(loss)
            learner.apply_gradients(gradients)

        check(params, expected)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
