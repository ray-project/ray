import gymnasium as gym
import unittest
import numpy as np
import tempfile
import torch

import ray

from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.core.testing.utils import get_learner
from ray.rllib.core.models.tests.test_base_models import _dynamo_is_available
from ray.rllib.core.testing.utils import get_module_spec
from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig
from ray.rllib.core.learner.learner import FrameworkHyperparameters


def _get_learner(learning_rate: float = 1e-3) -> Learner:
    env = gym.make("CartPole-v1")
    # adding learning rate as a configurable parameter to avoid hardcoding it
    # and information leakage across tests that rely on knowing the LR value
    # that is used in the learner.
    learner = get_learner("torch", env, learning_rate=learning_rate)
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

            loss = results[ALL_MODULES][Learner.TOTAL_LOSS_KEY]
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
        loss = {ALL_MODULES: sum(param.sum() for param in params)}
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
        learning_rate = 1e-3
        learner = _get_learner(learning_rate)

        learner.add_module(
            module_id="test",
            module_spec=SingleAgentRLModuleSpec(
                module_class=DiscreteBCTorchModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [16]},
            ),
        )

        learner.remove_module(DEFAULT_POLICY_ID)

        # only test module should be left
        self.assertEqual(set(learner.module.keys()), {"test"})

        # calculated the expected new params based on gradients of all ones.
        params = learner.get_parameters(learner.module["test"])
        n_steps = 100
        expected = [
            convert_to_numpy(param) - n_steps * learning_rate * np.ones(param.shape)
            for param in params
        ]
        for _ in range(n_steps):
            loss = {ALL_MODULES: sum(param.sum() for param in params)}
            gradients = learner.compute_gradients(loss)
            learner.apply_gradients(gradients)

        check(params, expected)

    def test_save_load_state(self):
        env = gym.make("CartPole-v1")

        learner1 = BCTorchLearner(
            module_spec=SingleAgentRLModuleSpec(
                module_class=DiscreteBCTorchModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [64]},
            ),
            optimizer_config={"lr": 2e-3},
        )

        learner1.build()
        with tempfile.TemporaryDirectory() as tmpdir:
            learner1.save_state(tmpdir)

            learner2 = BCTorchLearner(
                module_spec=SingleAgentRLModuleSpec(
                    module_class=DiscreteBCTorchModule,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict={"fcnet_hiddens": [32]},
                ),
                optimizer_config={"lr": 1e-3},
            )
            learner2.build()
            learner2.load_state(tmpdir)
            self._check_learner_states(learner1, learner2)

        # add a module then save/load and check states
        with tempfile.TemporaryDirectory() as tmpdir:
            learner1.add_module(
                module_id="test",
                module_spec=SingleAgentRLModuleSpec(
                    module_class=DiscreteBCTorchModule,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
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
            name: optim.state_dict()
            for name, optim in learner1._named_optimizers.items()
        }
        learner_2_optims_serialized = {
            name: optim.state_dict()
            for name, optim in learner2._named_optimizers.items()
        }
        check(learner_1_optims_serialized, learner_2_optims_serialized)

        learner_1_optims_serialized = [
            optim.state_dict() for optim in learner1._optimizer_parameters.keys()
        ]
        learner_2_optims_serialized = [
            optim.state_dict() for optim in learner2._optimizer_parameters.keys()
        ]
        check(learner_1_optims_serialized, learner_2_optims_serialized)

        check(learner1._module_optimizers, learner2._module_optimizers)

    @unittest.skipIf(not _dynamo_is_available(), "torch._dynamo not available")
    def test_torch_compile(self):
        """Test if torch.compile() can be applied and used on the learner.

        Also tests if we can update with the compiled update method without errors.
        """

        env = gym.make("CartPole-v1")
        framework_hps = FrameworkHyperparameters(
            torch_compile=True,
            torch_compile_cfg=TorchCompileConfig(),
        )

        for is_multi_agent in [False, True]:
            spec = get_module_spec(
                framework="torch", env=env, is_multi_agent=is_multi_agent
            )
            learner = BCTorchLearner(
                module_spec=spec,
                framework_hyperparameters=framework_hps,
                optimizer_config={"lr": 0.0001},
            )
            learner.build()

            reader = get_cartpole_dataset_reader(batch_size=512)

            for iter_i in range(100):
                batch = reader.next()
                learner.update(batch.as_multi_agent())

    @unittest.skipIf(not _dynamo_is_available(), "torch._dynamo not available")
    def test_torch_compile_no_breaks(self):
        """Tests if torch.compile() does encounter too many breaks.

        torch.compile() should ideally not encounter any breaks when compiling the
        update method of the learner. This method tests if we encounter only a given
        number of breaks.
        """

        env = gym.make("CartPole-v1")
        framework_hps = FrameworkHyperparameters(
            torch_compile=False,
            torch_compile_cfg=TorchCompileConfig(),
        )

        spec = get_module_spec(framework="torch", env=env)
        learner = BCTorchLearner(
            module_spec=spec,
            framework_hyperparameters=framework_hps,
            optimizer_config={"lr": 0.0001},
        )
        learner.build()

        import torch._dynamo as dynamo

        reader = get_cartpole_dataset_reader(batch_size=512)

        batch = reader.next().as_multi_agent()
        batch = learner._convert_batch_type(batch)

        # This is a helper method of dynamo to analyze where breaks occur.
        dynamo_explanation = dynamo.explain(learner._update, batch)
        print(dynamo_explanation[5])

        # There should be only one break reason - `return_value` - since inputs and
        # outputs are not checked
        break_reasons_list = dynamo_explanation[4]

        # TODO(Artur): Attempt bringing breaks down to 1. (This may not be possible)
        self.assertEquals(len(break_reasons_list), 3)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
