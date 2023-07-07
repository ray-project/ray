import itertools
import unittest

import gymnasium as gym

import ray
from ray.rllib.core.learner.learner import (
    FrameworkHyperparameters,
    TorchCompileWhatToCompile,
)
from ray.rllib.utils.torch_utils import _dynamo_is_available
from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig
from ray.rllib.core.testing.utils import get_learner
from ray.rllib.core.testing.utils import get_module_spec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.test_utils import get_cartpole_dataset_reader


class TestLearner(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    # Todo (rllib-team): Fix for torch 2.0+
    @unittest.skip("Failing with torch >= 2.0")
    @unittest.skipIf(not _dynamo_is_available(), "torch._dynamo not available")
    def test_torch_compile(self):
        """Test if torch.compile() can be applied and used on the learner.

        Also tests if we can update with the compiled update method without errors.
        """

        env = gym.make("CartPole-v1")
        is_multi_agents = [False, True]
        what_to_compiles = [
            TorchCompileWhatToCompile.FORWARD_TRAIN,
            TorchCompileWhatToCompile.COMPLETE_UPDATE,
        ]

        for is_multi_agent, what_to_compile in itertools.product(
            is_multi_agents, what_to_compiles
        ):
            print(
                f"Testing is_multi_agent={is_multi_agent},"
                f"what_to_compile={what_to_compile}"
            )
            framework_hps = FrameworkHyperparameters(
                torch_compile=True,
                torch_compile_cfg=TorchCompileConfig(),
                what_to_compile=what_to_compile,
            )
            learner = get_learner(
                framework="torch", env=env, framework_hps=framework_hps
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

    # Todo (rllib-team): Fix for torch 2.0+
    @unittest.skip("Failing with torch >= 2.0")
    @unittest.skipIf(not _dynamo_is_available(), "torch._dynamo not available")
    def test_torch_compile_no_breaks(self):
        """Tests if torch.compile() does encounter too many breaks.

        torch.compile() should ideally not encounter any breaks when compiling the
        update method of the learner. This method tests if we encounter only a given
        number of breaks.
        """

        env = gym.make("CartPole-v1")
        framework_hps = FrameworkHyperparameters(
            torch_compile=True,
            torch_compile_cfg=TorchCompileConfig(),
        )
        learner = get_learner(framework="torch", env=env, framework_hps=framework_hps)
        learner.build()

        import torch._dynamo as dynamo

        reader = get_cartpole_dataset_reader(batch_size=512)

        batch = reader.next().as_multi_agent()
        batch = learner._convert_batch_type(batch)

        # The followingcall to dynamo.explain() breaks depending on the torch version.
        # It works for torch==2.0.0.
        # TODO(Artur): Fit this to to the correct torch version once it is enabled on
        #  CI.
        # This is a helper method of dynamo to analyze where breaks occur.
        (
            explanation,
            out_guards,
            graphs,
            ops_per_graph,
            break_reasons,
            explanation_verbose,
        ) = dynamo.explain(learner._update, batch)

        print(explanation_verbose)

        # There should be only one break reason - `return_value` - since inputs and
        # outputs are not checked
        # TODO(Artur): Attempt bringing breaks down to 1. (This may not be possible)
        # Note: This test is skipped on CI if torch dynamo is available.
        self.assertEquals(len(break_reasons), 3)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
