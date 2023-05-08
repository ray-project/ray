import gymnasium as gym
import numpy as np
import tensorflow as tf
import tempfile
import unittest

import ray
from ray.rllib.algorithms.appo.appo import APPOConfig
from ray.rllib.core.learner.learner import Learner, FrameworkHyperparameters
from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_learner import BCTfLearner
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import (
    check,
    framework_iterator,
    get_cartpole_dataset_reader,
)
from ray.rllib.utils.metrics import ALL_MODULES


def get_learner(obs_space, action_space, learning_rate=1e-3) -> Learner:
    learner = BCTfLearner(
        module_spec=SingleAgentRLModuleSpec(
            module_class=DiscreteBCTFModule,
            observation_space=obs_space,
            action_space=action_space,
            model_config_dict={"fcnet_hiddens": [32]},
        ),
        # made this a configurable hparam to avoid information leakage in tests where we
        # need to know what the learning rate is.
        optimizer_config={"lr": learning_rate},
        learner_group_scaling_config=LearnerGroupScalingConfig(),
        framework_hyperparameters=FrameworkHyperparameters(eager_tracing=True),
    )

    learner.build()

    return learner


class TestLearner(unittest.TestCase):

    ENV = gym.make("CartPole-v1")

    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_end_to_end_update(self):

        learner = get_learner(self.ENV.observation_space, self.ENV.action_space)
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
        learner = BCTfLearner(
            module_spec=SingleAgentRLModuleSpec(
                module_class=DiscreteBCTFModule,
                observation_space=self.ENV.observation_space,
                action_space=self.ENV.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            ),
            # made this a configurable hparam to avoid information leakage in tests
            # where we need to know what the learning rate is.
            optimizer_config={"lr": 1e-3},
            learner_group_scaling_config=LearnerGroupScalingConfig(),
            framework_hyperparameters=FrameworkHyperparameters(eager_tracing=True),
        )

        learner.build()

        with tf.GradientTape() as tape:
            params = learner.module[DEFAULT_POLICY_ID].trainable_variables
            loss = {"total_loss": sum(tf.reduce_sum(param) for param in params)}
            gradients = learner.compute_gradients(loss, tape)

        # type should be a mapping from ParamRefs to gradients
        self.assertIsInstance(gradients, dict)

        for grad in gradients.values():
            check(grad, np.ones(grad.shape))

    def test_postprocess_gradients(self):
        """Tests the postprocess_gradients correctness."""
        config = (
            APPOConfig()
            .environment("CartPole-v1")
            .framework(eager_tracing=True)
            .rollouts(rollout_fragment_length=50)
        )

        # TODO (sven): Enable torch once available for APPO.
        for fw in framework_iterator(config, frameworks=("tf2")):
            # Clip by value only.
            config.training(
                grad_clip=0.75,
                grad_clip_by="value",
            )
            # TODO (sven): remove this once validation does NOT cause HPs to be
            #  generated anymore.
            config.validate()
            config.freeze()
            module_spec = config.get_default_rl_module_spec()
            module_spec.model_config_dict = {"fcnet_hiddens": [10]}
            module_spec.observation_space = self.ENV.observation_space
            module_spec.action_space = self.ENV.action_space
            learner_group = (
                config.get_learner_group_config(module_spec=module_spec)
                .learner(learner_class=config.get_default_learner_class())
                .build()
            )
            learner = learner_group._learner
            # Pretend our computed gradients are our weights + 1.0.
            grads = {
                v.ref(): v + 1.0
                for v in learner.module[DEFAULT_POLICY_ID].trainable_variables
            }
            # Call the learner's postprocessing method.
            processed_grads = list(learner.postprocess_gradients(grads).values())
            # Check clipped gradients.
            # No single gradient must be larger than 0.1 or smaller than -0.1:
            self.assertTrue(
                all(
                    np.max(grad) <= 0.75 and np.min(grad) >= -0.75
                    for grad in processed_grads
                )
            )

            # Clip by norm.
            config = config.copy(copy_frozen=False).training(
                grad_clip=1.0,
                grad_clip_by="norm",
            )
            # TODO (sven): remove this once validation does NOT cause HPs to be
            #  generated anymore.
            config.validate()
            config.freeze()
            learner_group = (
                config.get_learner_group_config(module_spec=module_spec)
                .learner(learner_class=config.get_default_learner_class())
                .build()
            )
            learner = learner_group._learner
            # Call the learner's postprocessing method.
            processed_grads = list(learner.postprocess_gradients(grads).values())
            # Check clipped gradients.
            for proc_grad, grad in zip(processed_grads, grads.values()):
                l2_norm = np.sqrt(np.sum(grad**2.0))
                if l2_norm > 1.0:
                    check(proc_grad, grad * (1.0 / l2_norm))

            # Clip by global norm.
            config = config.copy(copy_frozen=False).training(
                grad_clip=5.0,
                grad_clip_by="global_norm",
            )
            # TODO: remove this once validation does NOT cause HPs to be generated
            #  anymore
            config.validate()
            config.freeze()
            learner_group = (
                config.get_learner_group_config(module_spec=module_spec)
                .learner(learner_class=config.get_default_learner_class())
                .build()
            )
            learner = learner_group._learner
            # Call the learner's postprocessing method.
            processed_grads = list(learner.postprocess_gradients(grads).values())
            # Check clipped gradients.
            global_norm = np.sqrt(
                np.sum(np.sum(grad**2.0) for grad in grads.values())
            )
            if global_norm > 5.0:
                for proc_grad, grad in zip(processed_grads, grads.values()):
                    check(proc_grad, grad * (5.0 / global_norm))

    def test_apply_gradients(self):
        """Tests the apply_gradients correctness.

        Tests that if we apply gradients of all ones, the new params are equal to the
        standard SGD/Adam update rule.
        """

        learner = get_learner(self.ENV.observation_space, self.ENV.action_space)

        # calculated the expected new params based on gradients of all ones.
        params = learner.module[DEFAULT_POLICY_ID].trainable_variables
        n_steps = 100
        expected = [
            param - n_steps * learner._optimizer_config["lr"] * np.ones(param.shape)
            for param in params
        ]
        for _ in range(n_steps):
            gradients = {learner.get_param_ref(p): tf.ones_like(p) for p in params}
            learner.apply_gradients(gradients)

        check(params, expected)

    def test_add_remove_module(self):
        """Tests the compute/apply_gradients with add/remove modules.

        Tests that if we add a module with SGD optimizer with a known lr (different
        from default), and remove the default module, with a loss that is the sum of
        all variables the updated parameters follow the SGD update rule.
        """
        lr = 1e-3
        learner = get_learner(self.ENV.observation_space, self.ENV.action_space, lr)

        learner.add_module(
            module_id="test",
            module_spec=SingleAgentRLModuleSpec(
                module_class=DiscreteBCTFModule,
                observation_space=self.ENV.observation_space,
                action_space=self.ENV.action_space,
                model_config_dict={"fcnet_hiddens": [16]},
            ),
        )

        learner.remove_module(DEFAULT_POLICY_ID)

        # only test module should be left
        self.assertEqual(set(learner.module.keys()), {"test"})

        # calculated the expected new params based on gradients of all ones.
        params = learner.module["test"].trainable_variables
        n_steps = 100
        expected = [param - n_steps * lr * np.ones(param.shape) for param in params]
        for _ in range(n_steps):
            with tf.GradientTape() as tape:
                loss = {"total_loss": sum(tf.reduce_sum(param) for param in params)}
                gradients = learner.compute_gradients(loss, tape)
                learner.apply_gradients(gradients)

        check(params, expected)

    def test_save_load_state(self):
        learner1 = BCTfLearner(
            module_spec=SingleAgentRLModuleSpec(
                module_class=DiscreteBCTFModule,
                observation_space=self.ENV.observation_space,
                action_space=self.ENV.action_space,
                model_config_dict={"fcnet_hiddens": [64]},
            ),
            optimizer_config={"lr": 2e-3},
            learner_group_scaling_config=LearnerGroupScalingConfig(),
            framework_hyperparameters=FrameworkHyperparameters(eager_tracing=True),
        )

        learner1.build()
        with tempfile.TemporaryDirectory() as tmpdir:
            learner1.save_state(tmpdir)

            learner2 = BCTfLearner(
                module_spec=SingleAgentRLModuleSpec(
                    module_class=DiscreteBCTFModule,
                    observation_space=self.ENV.observation_space,
                    action_space=self.ENV.action_space,
                    model_config_dict={"fcnet_hiddens": [32]},
                ),
                optimizer_config={"lr": 1e-3},
                learner_group_scaling_config=LearnerGroupScalingConfig(),
                framework_hyperparameters=FrameworkHyperparameters(eager_tracing=True),
            )
            learner2.build()
            learner2.load_state(tmpdir)
            self._check_learner_states(learner1, learner2)

        # add a module then save/load and check states
        with tempfile.TemporaryDirectory() as tmpdir:
            learner1.add_module(
                module_id="test",
                module_spec=SingleAgentRLModuleSpec(
                    module_class=DiscreteBCTFModule,
                    observation_space=self.ENV.observation_space,
                    action_space=self.ENV.action_space,
                    model_config_dict={"fcnet_hiddens": [32]},
                ),
            )
            learner1.save_state(tmpdir)
            learner2.load_state(tmpdir)
            self._check_learner_states(learner1, learner2)

        # remove a module then save/load and check states
        with tempfile.TemporaryDirectory() as tmpdir:
            learner1.remove_module(module_id=DEFAULT_POLICY_ID)
            learner1.save_state(tmpdir)
            learner2.load_state(tmpdir)
            self._check_learner_states(learner1, learner2)

    def _check_learner_states(self, learner1, learner2):
        check(learner1.get_weights(), learner2.get_weights())

        # check all internal optimizer state dictionaries have been updated
        learner_1_optims_serialized = {
            name: optim.get_config()
            for name, optim in learner1._named_optimizers.items()
        }
        learner_2_optims_serialized = {
            name: optim.get_config()
            for name, optim in learner2._named_optimizers.items()
        }
        check(learner_1_optims_serialized, learner_2_optims_serialized)

        learner_1_optims_serialized = [
            optim.get_config() for optim in learner1._optimizer_parameters.keys()
        ]
        learner_2_optims_serialized = [
            optim.get_config() for optim in learner2._optimizer_parameters.keys()
        ]
        check(learner_1_optims_serialized, learner_2_optims_serialized)

        check(learner1._module_optimizers, learner2._module_optimizers)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
