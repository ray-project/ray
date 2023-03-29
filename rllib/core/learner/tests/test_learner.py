from typing import Iterable, List
import gymnasium as gym
import unittest
import tensorflow as tf
import torch
import numpy as np

import ray

from ray.rllib.core.learner.learner import Learner, LearnerMetrics
from ray.rllib.core.testing.utils import get_learner, get_module_spec
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader
from ray.rllib.utils.typing import TensorType


def _compute_gradients(
    framework: str, learner: Learner, params: Iterable[TensorType]
) -> List[TensorType]:
    """Compute gradients of sum of params w.r.t. params. using learner's api.

    Leaner's compute_gradients() is used to compute the gradients of the sum of
    params w.r.t. params. This is used to test the correctness of learner's
    compute_gradients() api. The expected gradients should be all ones.

    Args:
        framework: The framework to use.
        learner: The learner to use.
        params: The params to compute gradients for.

    Returns:
        The gradients.
    """

    if framework == "tf":
        with tf.GradientTape() as tape:
            loss = {
                LearnerMetrics.TOTAL_LOSS: sum(
                    [tf.reduce_sum(param) for param in params]
                )
            }
            gradients = learner.compute_gradients(loss, tape)
    else:
        loss = {LearnerMetrics.TOTAL_LOSS: sum([param.sum() for param in params])}
        gradients = learner.compute_gradients(loss)

    return gradients


class TestLearner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_end_to_end_update(self):
        env = gym.make("CartPole-v1")

        for framework in ["tf", "torch"]:
            learner = get_learner(framework, env)
            learner.build()
            reader = get_cartpole_dataset_reader(batch_size=512)

            min_loss = float("inf")
            for iter_i in range(1000):
                batch = reader.next()
                results = learner.update(batch.as_multi_agent())

                loss = results[LearnerMetrics.ALL_MODULES][LearnerMetrics.TOTAL_LOSS]
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

        env = gym.make("CartPole-v1")

        for framework in ["tf", "torch"]:
            learner = get_learner(framework, env)
            learner.build()

            params = learner.get_parameters(learner.module[DEFAULT_POLICY_ID])
            gradients = _compute_gradients(framework, learner, params)

            # type should be a mapping from ParamRefs to gradients
            self.assertIsInstance(gradients, dict)

            for grad in gradients.values():
                check(grad, np.ones(grad.shape))

    def test_apply_gradients(self):
        """Tests the apply_gradients correctness.

        Tests that if we apply gradients of all ones, the new params are equal to the
        standard SGD/Adam update rule.
        """

        env = gym.make("CartPole-v1")

        for framework in ["tf", "torch"]:
            learner = get_learner(framework, env)
            learner.build()

            # calculated the expected new params based on gradients of all ones.
            params = learner.get_parameters(learner.module[DEFAULT_POLICY_ID])
            n_steps = 100
            expected = [
                convert_to_numpy(param)
                - n_steps * learner._optimizer_config["lr"] * np.ones(param.shape)
                for param in params
            ]
            for _ in range(n_steps):
                module = tf if framework == "tf" else torch
                gradients = {
                    learner._get_param_ref(p): module.ones_like(p) for p in params
                }
                learner.apply_gradients(gradients)

            check(params, expected)

    def test_add_remove_module(self):
        """Tests the compute/apply_gradients with add/remove modules.

        Tests that if we add a module with SGD optimizer with a known lr (different
        from default), and remove the default module, with a loss that is the sum of
        all variables the updated parameters follow the SGD update rule.
        """
        env = gym.make("CartPole-v1")

        for framework in ["tf", "torch"]:
            learner = get_learner(framework, env)
            learner.build()

            # add a test module with SGD optimizer with a known lr
            lr = 1e-4

            def set_optimizer_fn(module):
                if framework == "torch":
                    return [
                        (
                            module.parameters(),
                            torch.optim.Adam(module.parameters(), lr=lr),
                        )
                    ]
                else:
                    return [
                        (
                            module.trainable_variables,
                            tf.keras.optimizers.SGD(learning_rate=lr),
                        )
                    ]

            learner.add_module(
                module_id="test",
                module_spec=get_module_spec(framework, env),
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
                gradients = _compute_gradients(framework, learner, params)
                learner.apply_gradients(gradients)

            check(params, expected)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
