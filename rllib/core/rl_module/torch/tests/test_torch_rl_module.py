import gc
import tempfile
import unittest

import gymnasium as gym
import torch

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig
from ray.rllib.examples.rl_modules.classes.vpg_torch_rlm import VPGTorchRLModule
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.torch_utils import _dynamo_is_available


class TestRLModule(unittest.TestCase):
    def test_compilation(self):

        env = gym.make("CartPole-v1")
        module = VPGTorchRLModule(
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config={"hidden_dim": 32},
        )

        self.assertIsInstance(module, TorchRLModule)

    def test_forward_train(self):

        bsize = 1024
        env = gym.make("CartPole-v1")
        module = VPGTorchRLModule(
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config={"hidden_dim": 32},
        )

        obs_shape = env.observation_space.shape
        obs = torch.randn((bsize,) + obs_shape)
        actions = torch.stack(
            [torch.tensor(env.action_space.sample()) for _ in range(bsize)]
        )
        output = module.forward_train({"obs": obs})

        self.assertIsInstance(output, dict)
        self.assertIn(Columns.ACTION_DIST_INPUTS, output)

        action_dist_inputs = output[Columns.ACTION_DIST_INPUTS]
        action_dist_class = module.get_train_action_dist_cls()
        action_dist = action_dist_class.from_logits(action_dist_inputs)

        loss = -action_dist.logp(actions.view(-1)).mean()
        loss.backward()

        # check that all neural net parameters have gradients
        for param in module.parameters():
            self.assertIsNotNone(param.grad)

    def test_forward(self):
        """Test forward inference and exploration of"""

        env = gym.make("CartPole-v1")
        module = VPGTorchRLModule(
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config={"hidden_dim": 32},
        )

        obs_shape = env.observation_space.shape
        obs = torch.randn((1,) + obs_shape)

        # just test if the forward pass runs fine
        module.forward_inference({"obs": obs})
        module.forward_exploration({"obs": obs})

    def test_get_set_state(self):

        env = gym.make("CartPole-v1")
        module = VPGTorchRLModule(
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config={"hidden_dim": 32},
        )

        state = module.get_state()
        self.assertIsInstance(state, dict)

        module2 = VPGTorchRLModule(
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config={"hidden_dim": 32},
        )
        state2 = module2.get_state()
        check(state, state2, false=True)

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)

    def test_checkpointing(self):
        env = gym.make("CartPole-v1")
        module = VPGTorchRLModule(
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config={"hidden_dim": 32},
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = "/tmp/rl_module_test"
            module.save_to_path(tmpdir)
            new_module = VPGTorchRLModule.from_checkpoint(tmpdir)

        check(module.get_state(), new_module.get_state())
        self.assertNotEqual(id(module), id(new_module))


class TestRLModuleGPU(unittest.TestCase):
    @unittest.skipIf(not _dynamo_is_available(), "torch._dynamo not available")
    def test_torch_compile_no_memory_leak_gpu(self):
        assert torch.cuda.is_available()

        def get_memory_usage_cuda():
            torch.cuda.empty_cache()
            return torch.cuda.memory_allocated()

        compile_cfg = TorchCompileConfig()

        env = gym.make("CartPole-v1")

        memory_before_create = get_memory_usage_cuda()

        torch_rl_module = VPGTorchRLModule(
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config={"hidden_dim": 32},
        )

        torch_rl_module.cuda()

        torch_rl_module.compile(compile_cfg)

        memory_after_create = get_memory_usage_cuda()
        memory_diff_create = memory_after_create - memory_before_create
        print("memory_diff_create: ", memory_diff_create)
        # Sanity check that we actually allocated memory.
        assert memory_diff_create > 0

        del torch_rl_module
        gc.collect()
        memory_after_delete = get_memory_usage_cuda()
        memory_diff_delete = memory_after_delete - memory_after_create
        print("memory_diff_delete: ", memory_diff_delete)

        # Memory should be released after deleting the module.
        check(memory_before_create, memory_after_delete)


if __name__ == "__main__":
    import sys

    import pytest

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
