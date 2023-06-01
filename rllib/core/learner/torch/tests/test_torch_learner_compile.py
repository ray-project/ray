import itertools
import unittest

import gymnasium as gym

import ray
from ray.rllib.core.learner.learner import FrameworkHyperparameters
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.models.tests.test_base_models import _dynamo_is_available
from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig
from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner
from ray.rllib.core.testing.utils import get_learner
from ray.rllib.core.testing.utils import get_module_spec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.test_utils import get_cartpole_dataset_reader


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

    @unittest.skipIf(not _dynamo_is_available(), "torch._dynamo not available")
    def test_torch_compile(self):
        """Test if torch.compile() can be applied and used on the learner.

        Also tests if we can update with the compiled update method without errors.
        """

        env = gym.make("CartPole-v1")
        is_multi_agents = [False, True]
        what_to_compiles = ["complete_update", "forward_train"]

        for is_multi_agent, what_to_compile in itertools.product(
            is_multi_agents, what_to_compiles
        ):
            framework_hps = FrameworkHyperparameters(
                torch_compile=True,
                torch_compile_cfg=TorchCompileConfig(),
                what_to_compile=what_to_compile,
            )
            spec = get_module_spec(
                framework="torch", env=env, is_multi_agent=is_multi_agent
            )
            learner = BCTorchLearner(
                module_spec=spec,
                framework_hyperparameters=framework_hps,
            )
            learner.build()

            reader = get_cartpole_dataset_reader(batch_size=512)

            for iter_i in range(10):
                batch = reader.next()
                learner.update(batch.as_multi_agent())

            spec = get_module_spec(framework="torch", env=env, is_multi_agent=False)
            learner.add_module(module_id="another_module", module_spec=spec)

            for iter_i in range(10):
                batch = MultiAgentBatch(
                    {"another_module": reader.next(), "default_policy": reader.next()},
                    0,
                )
                learner.update(batch)

            learner.remove_module(module_id="another_module")

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
