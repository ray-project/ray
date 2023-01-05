# TODO (avnishn): Merge with the tensorflow version of this test once the
# RLTrainer has been merged.
import gymnasium as gym
import pytest
import torch
import unittest

import ray

from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.optim.marl_optimizer import MultiAgentRLOptimizer

from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.torch.bc_optimizer import BCTorchOptimizer
from ray.rllib.core.testing.torch.marl_module_obs_encoder import MaModuleWObsEncoder
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader


class TestRLOptimizer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_rl_optimizer_in_behavioral_cloning_torch(self):
        # test to see that general behavior cloning with "multi agents just works"
        torch.manual_seed(1)
        env = gym.make("CartPole-v1")

        config = {
            "modules": {
                "module_0": {
                    "module_class": DiscreteBCTorchModule,
                    "observation_space": env.observation_space,
                    "action_space": env.action_space,
                    "model_config": {"hidden_dim": 32},
                },
                "module_1": {
                    "module_class": DiscreteBCTorchModule,
                    "observation_space": env.observation_space,
                    "action_space": env.action_space,
                    "model_config": {"hidden_dim": 32},
                },
            }
        }

        marl_module = MultiAgentRLModule.from_multi_agent_config(config)
        marl_optimizer = MultiAgentRLOptimizer.from_marl_module(
            marl_module,
            rl_optimizer_classes={
                "module_0": BCTorchOptimizer,
                "module_1": BCTorchOptimizer,
            },
            optim_kwargs={"module_0": {"lr": 0.1}, "module_1": {"lr": 0.1}},
        )

        dataset_reader = get_cartpole_dataset_reader(batch_size=500)
        num_epochs = 500

        for _ in range(num_epochs):
            batch = dataset_reader.next()
            ma_batch = MultiAgentBatch(
                {"module_0": batch, "module_1": batch}, env_steps=batch.count
            )
            ma_batch = convert_to_torch_tensor(ma_batch.policy_batches)
            fwd_out = marl_module.forward_train(ma_batch)
            loss = marl_optimizer.compute_loss(fwd_out, ma_batch)
            loss["total_loss"].backward()
            for rl_optimizer in marl_optimizer.get_rl_optimizers().values():
                for optimizer in rl_optimizer.get_optimizers().values():
                    optimizer.step()
                    optimizer.zero_grad()
            loss = convert_to_numpy(loss)
            if loss["total_loss"] < 1.03:
                break
        # The loss is initially around 1.5. When it gets to around
        # 1.03 the return of each policy gets to around 100.
        self.assertLess(loss["total_loss"], 1.03)

    def test_marl_optimizer_set_state_get_state_torch(self):
        torch.manual_seed(1)
        env = gym.make("CartPole-v1")

        config = {
            "modules": {
                "module_0": {
                    "module_class": DiscreteBCTorchModule,
                    "observation_space": env.observation_space,
                    "action_space": env.action_space,
                    "model_config": {"hidden_dim": 32},
                },
                "module_1": {
                    "module_class": DiscreteBCTorchModule,
                    "observation_space": env.observation_space,
                    "action_space": env.action_space,
                    "model_config": {"hidden_dim": 32},
                },
            }
        }

        marl_module1 = MultiAgentRLModule.from_multi_agent_config(config)
        marl_optimizer1 = MultiAgentRLOptimizer.from_marl_module(
            marl_module1,
            rl_optimizer_classes={
                "module_0": BCTorchOptimizer,
                "module_1": BCTorchOptimizer,
            },
            optim_kwargs={"module_0": {"lr": 0.1}, "module_1": {"lr": 0.1}},
        )

        marl_module2 = MultiAgentRLModule.from_multi_agent_config(config)
        marl_optimizer2 = MultiAgentRLOptimizer.from_marl_module(
            marl_module2,
            rl_optimizer_classes={
                "module_0": BCTorchOptimizer,
                "module_1": BCTorchOptimizer,
            },
            optim_kwargs={"module_0": {"lr": 0.2}, "module_1": {"lr": 0.2}},
        )
        self.assertIsInstance(marl_optimizer1.get_state(), dict)

        # check that the learning rates are different before setting the state to be
        # the same
        check(
            marl_optimizer1.get_state()["module_0"]["module"]["param_groups"][0]["lr"],
            marl_optimizer2.get_state()["module_0"]["module"]["param_groups"][0]["lr"],
            false=True,
        )
        check(
            marl_optimizer1.get_state()["module_1"]["module"]["param_groups"][0]["lr"],
            marl_optimizer2.get_state()["module_1"]["module"]["param_groups"][0]["lr"],
            false=True,
        )

        marl_optimizer2.set_state(marl_optimizer1.get_state())
        # check that the learning rates are the same after setting the state to be
        # the same
        check(
            marl_optimizer1.get_state()["module_0"]["module"]["param_groups"][0]["lr"],
            marl_optimizer2.get_state()["module_0"]["module"]["param_groups"][0]["lr"],
        )
        check(
            marl_optimizer1.get_state()["module_1"]["module"]["param_groups"][0]["lr"],
            marl_optimizer2.get_state()["module_1"]["module"]["param_groups"][0]["lr"],
        )

    def test_marl_optimizer_shared_encoder_torch(self):
        # check that using a shared observation encoder also just works

        env = gym.make("CartPole-v1")
        output_dim_encoder = 3
        config = {
            "encoder": {
                "input_dim_encoder": env.observation_space.shape[0],
                "hidden_dim_encoder": 32,
                "output_dim_encoder": output_dim_encoder,
            },
            "modules": {
                "module_0": {
                    "module_class": DiscreteBCTorchModule,
                    "input_dim": output_dim_encoder,
                    "output_dim": env.action_space.n,
                    "hidden_dim": 32,
                },
                "module_1": {
                    "module_class": DiscreteBCTorchModule,
                    "input_dim": output_dim_encoder,
                    "output_dim": env.action_space.n,
                    "hidden_dim": 32,
                },
            },
        }
        marl_module = MaModuleWObsEncoder.from_multi_agent_config(config)
        dataset_reader = get_cartpole_dataset_reader(batch_size=500)
        batch = dataset_reader.next()
        ma_batch = MultiAgentBatch(
            {"module_0": batch, "module_1": batch}, env_steps=batch.count
        )
        ma_batch = convert_to_torch_tensor(ma_batch.policy_batches)
        fwd_out = marl_module.forward_train(ma_batch)
        print(fwd_out)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
