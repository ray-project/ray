import gymnasium as gym
import numpy as np
import tempfile
import unittest

import ray
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.testing.testing_learner import BaseTestingAlgorithmConfig

from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import (
    check,
    framework_iterator,
    get_cartpole_dataset_reader,
)
from ray.rllib.utils.metrics import ALL_MODULES

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


class TestLearner(unittest.TestCase):

    ENV = gym.make("CartPole-v1")

    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_end_to_end_update(self):

        config = BaseTestingAlgorithmConfig()

        for _ in framework_iterator(config, frameworks=("torch", "tf2")):
            learner = config.build_learner(env=self.ENV)
            reader = get_cartpole_dataset_reader(batch_size=512)

            min_loss = float("inf")
            for iter_i in range(1000):
                batch = reader.next()
                results = learner.update_from_batch(batch=batch.as_multi_agent())

            loss = results[ALL_MODULES][Learner.TOTAL_LOSS_KEY]
            min_loss = min(loss, min_loss)
            print(f"[iter = {iter_i}] Loss: {loss:.3f}, Min Loss: {min_loss:.3f}")
            # The loss is initially around 0.69 (ln2). When it gets to around
            # 0.58 the return of the policy gets to around 100.
            if min_loss < 0.58:
                break
        self.assertLess(min_loss, 0.58)

    def test_compute_gradients(self):
        """Tests the compute_gradients correctness.

        Tests that if we sum all the trainable variables the gradient of output w.r.t.
        the weights is all ones.
        """
        config = BaseTestingAlgorithmConfig()

        for fw in framework_iterator(config, frameworks=("torch", "tf2")):
            learner = config.build_learner(env=self.ENV)

            params = learner.get_parameters(learner.module[DEFAULT_MODULE_ID])

            tape = None
            if fw == "torch":
                loss_per_module = {ALL_MODULES: sum(param.sum() for param in params)}
            else:
                with tf.GradientTape() as tape:
                    loss_per_module = {
                        ALL_MODULES: sum(tf.reduce_sum(param) for param in params)
                    }

            gradients = learner.compute_gradients(loss_per_module, gradient_tape=tape)

            # Type should be a mapping from ParamRefs to gradients.
            self.assertIsInstance(gradients, dict)

            for grad in gradients.values():
                check(grad, np.ones(grad.shape))

    def test_postprocess_gradients(self):
        """Tests the base grad clipping logic in `postprocess_gradients()`."""

        # Clip by value only.
        config = BaseTestingAlgorithmConfig().training(
            lr=0.0003, grad_clip=0.75, grad_clip_by="value"
        )

        for fw in framework_iterator(config, frameworks=("torch", "tf2")):
            learner = config.build_learner(env=self.ENV)
            # Pretend our computed gradients are our weights + 1.0.
            grads = {
                learner.get_param_ref(v): v + 1.0
                for v in learner.get_parameters(learner.module[DEFAULT_MODULE_ID])
            }
            # Call the learner's postprocessing method.
            processed_grads = list(learner.postprocess_gradients(grads).values())
            # Check clipped gradients.
            # No single gradient must be larger than 0.1 or smaller than -0.1:
            self.assertTrue(
                all(
                    np.max(grad) <= config.grad_clip
                    and np.min(grad) >= -config.grad_clip
                    for grad in convert_to_numpy(processed_grads)
                )
            )

            # Clip by norm.
            config.grad_clip = 1.0
            config.grad_clip_by = "norm"
            learner = config.build_learner(env=self.ENV)
            # Pretend our computed gradients are our weights + 1.0.
            grads = {
                learner.get_param_ref(v): v + 1.0
                for v in learner.get_parameters(learner.module[DEFAULT_MODULE_ID])
            }
            # Call the learner's postprocessing method.
            processed_grads = list(learner.postprocess_gradients(grads).values())
            # Check clipped gradients.
            for proc_grad, grad in zip(
                convert_to_numpy(processed_grads),
                convert_to_numpy(list(grads.values())),
            ):
                l2_norm = np.sqrt(np.sum(grad**2.0))
                if l2_norm > config.grad_clip:
                    check(proc_grad, grad * (config.grad_clip / l2_norm))

            # Clip by global norm.
            config.grad_clip = 5.0
            config.grad_clip_by = "global_norm"
            learner = config.build_learner(env=self.ENV)
            # Pretend our computed gradients are our weights + 1.0.
            grads = {
                learner.get_param_ref(v): v + 1.0
                for v in learner.get_parameters(learner.module[DEFAULT_MODULE_ID])
            }
            # Call the learner's postprocessing method.
            processed_grads = list(learner.postprocess_gradients(grads).values())
            # Check clipped gradients.
            global_norm = np.sqrt(
                np.sum(
                    np.sum(grad**2.0)
                    for grad in convert_to_numpy(list(grads.values()))
                )
            )
            if global_norm > config.grad_clip:
                for proc_grad, grad in zip(
                    convert_to_numpy(processed_grads),
                    grads.values(),
                ):
                    check(proc_grad, grad * (config.grad_clip / global_norm))

    def test_apply_gradients(self):
        """Tests the apply_gradients correctness.

        Tests that if we apply gradients of all ones, the new params are equal to the
        standard SGD/Adam update rule.
        """
        config = BaseTestingAlgorithmConfig().training(lr=0.0003)

        for fw in framework_iterator(config, frameworks=("torch", "tf2")):
            learner = config.build_learner(env=self.ENV)

            # calculated the expected new params based on gradients of all ones.
            params = learner.get_parameters(learner.module[DEFAULT_MODULE_ID])
            n_steps = 100
            expected = [
                (
                    convert_to_numpy(param)
                    - n_steps * learner.config.lr * np.ones(param.shape)
                )
                for param in params
            ]
            for _ in range(n_steps):
                if fw == "torch":
                    gradients = {
                        learner.get_param_ref(p): torch.ones_like(p) for p in params
                    }
                else:
                    gradients = {
                        learner.get_param_ref(p): tf.ones_like(p) for p in params
                    }
                learner.apply_gradients(gradients)

            check(params, expected)

    def test_add_remove_module(self):
        """Tests the compute/apply_gradients with add/remove modules.

        Tests that if we add a module with SGD optimizer with a known lr (different
        from default), and remove the default module, with a loss that is the sum of
        all variables the updated parameters follow the SGD update rule.
        """
        config = BaseTestingAlgorithmConfig().training(lr=0.0003)

        for fw in framework_iterator(config, frameworks=("torch", "tf2")):
            learner = config.build_learner(env=self.ENV)
            rl_module_spec = config.get_default_rl_module_spec()
            rl_module_spec.observation_space = self.ENV.observation_space
            rl_module_spec.action_space = self.ENV.action_space
            learner.add_module(
                module_id="test",
                module_spec=rl_module_spec,
            )
            learner.remove_module(DEFAULT_MODULE_ID)

            # only test module should be left
            self.assertEqual(set(learner.module.keys()), {"test"})

            # calculated the expected new params based on gradients of all ones.
            params = learner.get_parameters(learner.module["test"])
            n_steps = 100
            expected = [
                convert_to_numpy(param)
                - n_steps * learner.config.lr * np.ones(param.shape)
                for param in params
            ]
            for _ in range(n_steps):
                tape = None
                if fw == "torch":
                    loss_per_module = {
                        ALL_MODULES: sum(param.sum() for param in params)
                    }
                else:
                    with tf.GradientTape() as tape:
                        loss_per_module = {
                            ALL_MODULES: sum(tf.reduce_sum(param) for param in params)
                        }
                gradients = learner.compute_gradients(
                    loss_per_module, gradient_tape=tape
                )
                learner.apply_gradients(gradients)

            check(params, expected)

    def test_save_load_state(self):
        """Tests, whether a Learner's state is properly saved and restored."""
        config = BaseTestingAlgorithmConfig()

        for fw in framework_iterator(config, frameworks=("torch", "tf2")):
            # Get a Learner instance for the framework and env.
            learner1 = config.build_learner(env=self.ENV)
            with tempfile.TemporaryDirectory() as tmpdir:
                learner1.save_state(tmpdir)

                learner2 = config.build_learner(env=self.ENV)
                learner2.load_state(tmpdir)
                self._check_learner_states(fw, learner1, learner2)

            # Add a module then save/load and check states.
            with tempfile.TemporaryDirectory() as tmpdir:
                rl_module_spec = config.get_default_rl_module_spec()
                rl_module_spec.observation_space = self.ENV.observation_space
                rl_module_spec.action_space = self.ENV.action_space
                learner1.add_module(
                    module_id="test",
                    module_spec=rl_module_spec,
                )
                learner1.save_state(tmpdir)
                learner2.load_state(tmpdir)
                self._check_learner_states(fw, learner1, learner2)

            # Remove a module then save/load and check states.
            with tempfile.TemporaryDirectory() as tmpdir:
                learner1.remove_module(module_id=DEFAULT_MODULE_ID)
                learner1.save_state(tmpdir)
                learner2.load_state(tmpdir)
                self._check_learner_states(fw, learner1, learner2)

    def _check_learner_states(self, framework, learner1, learner2):
        check(learner1.get_module_state(), learner2.get_module_state())

        # Method to call on the local optimizer object to get the optimizer's
        # state.
        method = "get_config" if framework == "tf2" else "state_dict"

        # check all internal optimizer state dictionaries have been updated
        learner_1_optims_serialized = {
            name: getattr(optim, method)()
            for name, optim in learner1._named_optimizers.items()
        }
        learner_2_optims_serialized = {
            name: getattr(optim, method)()
            for name, optim in learner2._named_optimizers.items()
        }
        check(learner_1_optims_serialized, learner_2_optims_serialized)

        learner_1_optims_serialized = [
            getattr(optim, method)() for optim in learner1._optimizer_parameters.keys()
        ]
        learner_2_optims_serialized = [
            getattr(optim, method)() for optim in learner2._optimizer_parameters.keys()
        ]
        check(learner_1_optims_serialized, learner_2_optims_serialized)

        check(learner1._module_optimizers, learner2._module_optimizers)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
