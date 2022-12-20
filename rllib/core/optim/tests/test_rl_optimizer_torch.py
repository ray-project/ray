# TODO (avnishn): Merge with the tensorflow version of this test once the
# RLTrainer has been merged.
import gymnasium as gym
import pytest
import torch
from typing import Any, Mapping, Union
import unittest

import ray

from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.offline import IOContext
from ray.rllib.offline.dataset_reader import (
    DatasetReader,
    get_dataset_and_shards,
)
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.torch.bc_optimizer import BCTorchOptimizer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import TensorType


def model_norm(model: torch.nn.Module) -> float:
    """Compute the norm of the weights of a model.

    Args:
        model: The model whose weights to compute a norm over.

    Returns:
        the norm of model.
    """
    total_norm = 0
    for p in model.parameters():
        param_norm = p.detach().data.norm(2)
        total_norm += param_norm.item() ** 2
    total_norm = total_norm**0.5
    return total_norm


# TODO (avnishn): This RLTrainer has to be properly implemented once the RLTrainerActor
# is implemented on master.
class BCTorchTrainer:
    """This class is a demonstration on how to use RLOptimizer and RLModule together."""

    def __init__(self, env: gym.Env) -> None:
        optimizer_config = {}
        self._module = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )
        self._rl_optimizer = BCTorchOptimizer(self._module, optimizer_config)

    @staticmethod
    def on_after_compute_gradients(
        gradients_dict: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Called after gradients have been computed.

        Args:
            gradients_dict (Mapping[str, Any]): A dictionary of gradients.

        Note the relative order of operations looks like this:
            fwd_out = forward_train(batch)
            loss = compute_loss(batch, fwd_out)
            gradients = compute_gradients(loss)
            ---> post_processed_gradients = on_after_compute_gradients(gradients)
            apply_gradients(post_processed_gradients)

        Returns:
            Mapping[str, Any]: A dictionary of gradients.
        """
        return gradients_dict

    def compile_results(
        self,
        batch: NestedDict,
        fwd_out: Mapping[str, Any],
        postprocessed_loss: Mapping[str, Any],
        post_processed_gradients: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Compile results from the update.

        Args:
            batch: The batch that was used for the update.
            fwd_out: The output of the forward train pass.
            postprocessed_loss: The loss after postprocessing.
            post_processed_gradients: The gradients after postprocessing.

        Returns:
            A dictionary of results.
        """

        loss_numpy = convert_to_numpy(postprocessed_loss)
        results = {
            "avg_reward": batch["rewards"].mean(),
        }
        results.update(loss_numpy)
        results["module_norm"] = model_norm(self._module)
        return results

    def update(self, batch: SampleBatch) -> Mapping[str, Any]:
        """Perform an update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        torch_batch = convert_to_torch_tensor(batch)
        fwd_out = self._module.forward_train(torch_batch)
        loss = self._rl_optimizer.compute_loss(torch_batch, fwd_out)

        # if loss is a tensor, wrap it in a dict
        if isinstance(loss, torch.Tensor):
            loss = {"total_loss": loss}

        gradients = self.compute_gradients(loss)
        post_processed_gradients = self.on_after_compute_gradients(gradients)
        self.apply_gradients(post_processed_gradients)
        results = self.compile_results(batch, fwd_out, loss, post_processed_gradients)
        return results

    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        """Perform an update on self._module.

        For example compute and apply gradients to self._module if
        necessary.

        Args:
            loss: variable(s) used for optimizing self._module.

        Returns:
            A dictionary of extra information and statistics.
        """

        if isinstance(loss, torch.Tensor):
            loss = {"total_loss": loss}

        if not isinstance(loss, dict):
            raise ValueError("loss must be a dict or torch.Tensor")

        loss["total_loss"].backward()
        grads = {n: p.grad for n, p in self._module.named_parameters()}
        return grads

    def apply_gradients(self, gradients: Mapping[str, Any]) -> None:
        """Perform an update on self._module"""
        for optimizer in self._rl_optimizer.get_optimizers().values():
            optimizer.step()
            optimizer.zero_grad()

    def set_state(self, state: Mapping[str, Any]) -> None:
        """Set the state of the trainer."""
        self._rl_optimizer.set_state(state.get("optimizer_state", {}))
        self._module.set_state(state.get("module_state", {}))

    def get_state(self) -> Mapping[str, Any]:
        """Get the state of the trainer."""
        return {
            "module_state": self._module.get_state(),
            "optimizer_state": self._rl_optimizer.get_state(),
        }


class TestRLOptimizer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_rl_optimizer_in_behavioral_cloning_torch(self):
        torch.manual_seed(1)
        env = gym.make("CartPole-v1")
        module_for_inference = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        trainer = BCTorchTrainer(env)
        trainer.set_state({"module_state": module_for_inference.get_state()})

        # path = "s3://air-example-data/rllib/cartpole/large.json"
        path = "tests/data/cartpole/large.json"
        input_config = {"format": "json", "paths": path}
        dataset, _ = get_dataset_and_shards(
            AlgorithmConfig().offline_data(input_="dataset", input_config=input_config)
        )
        batch_size = 500
        ioctx = IOContext(
            config=(
                AlgorithmConfig()
                .training(train_batch_size=batch_size)
                .offline_data(actions_in_input_normalized=True)
            ),
            worker_index=0,
        )
        reader = DatasetReader(dataset, ioctx)
        num_epochs = 100
        total_timesteps_of_training = 1000000
        inter_steps = total_timesteps_of_training // (num_epochs * batch_size)
        for _ in range(num_epochs):
            for _ in range(inter_steps):
                batch = reader.next()
                results = trainer.update(batch)
                if results["total_loss"] < 0.57:
                    break
        # The loss is initially around 0.68. When it gets to around
        # 0.57 the return of the policy gets to around 100.
        self.assertLess(results["total_loss"], 0.57)

    def test_rl_optimizer_set_state_get_state_torch(self):
        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )
        optim1 = BCTorchOptimizer(module, {"lr": 0.1})
        optim2 = BCTorchOptimizer(module, {"lr": 0.2})

        self.assertIsInstance(optim1.get_state(), dict)
        check(
            optim1.get_state()["module"]["param_groups"][0]["lr"],
            optim2.get_state()["module"]["param_groups"][0]["lr"],
            false=True,
        )
        optim1.set_state(optim2.get_state())
        check(optim1.get_state(), optim2.get_state())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
