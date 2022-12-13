import gymnasium as gym
import pytest
import numpy as np
import torch
from typing import Any, List, Mapping, Union
import unittest

from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.offline import IOContext
from ray.rllib.offline.dataset_reader import (
    DatasetReader,
    get_dataset_and_shards,
)

import ray
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import FCNet, FCConfig
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.torch.tests.test_torch_rl_module import (
    to_numpy,
    to_tensor,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.torch_distributions import (
    TorchDeterministic,
    TorchCategorical,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
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


def do_rollouts(
    env: gym.Env, module_for_inference: RLModule, num_rollouts: int
) -> SampleBatch:
    _returns = []
    _rollouts = []
    for _ in range(num_rollouts):
        obs, info = env.reset()
        _obs, _next_obs, _actions, _rewards, _terminateds, _truncateds = (
            [], [], [], [], [], []
        )
        _return = -0
        for _ in range(env._max_episode_steps):
            _obs.append(obs)
            fwd_out = module_for_inference.forward_inference(
                {"obs": to_tensor(obs)[None]}
            )
            action = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
            next_obs, reward, terminated, truncated, _ = env.step(action)
            _next_obs.append(next_obs)
            _actions.append([action])
            _rewards.append([reward])
            _terminateds.append([terminated])
            _truncateds.append([truncated])
            _return += reward
            if terminated or truncated:
                break
            obs = next_obs
        batch = SampleBatch(
            {
                SampleBatch.OBS: _obs,
                SampleBatch.NEXT_OBS: _next_obs,
                SampleBatch.ACTIONS: _actions,
                SampleBatch.REWARDS: _rewards,
                SampleBatch.TERMINATEDS: _terminateds,
                SampleBatch.TRUNCATEDS: _truncateds,
            }
        )
        _returns.append(_return)
        _rollouts.append(batch)
    return np.mean(_returns), _returns, _rollouts


class BCTorchModule(TorchRLModule):
    def __init__(self, config: FCConfig) -> None:
        super().__init__(config)
        self.policy = FCNet(config)

    @override(RLModule)
    def input_specs_exploration(self) -> ModelSpec:
        return ModelSpec(self._default_inputs())

    @override(RLModule)
    def input_specs_inference(self) -> ModelSpec:
        return ModelSpec(self._default_inputs())

    @override(RLModule)
    def input_specs_train(self) -> ModelSpec:
        return ModelSpec(
            dict(self._default_inputs(), **{"actions": TorchTensorSpec("b")}),
        )

    @override(RLModule)
    def output_specs_exploration(self) -> ModelSpec:
        return ModelSpec(self._default_outputs())

    @override(RLModule)
    def output_specs_inference(self) -> ModelSpec:
        return ModelSpec(self._default_outputs())

    @override(RLModule)
    def output_specs_train(self) -> ModelSpec:
        return ModelSpec(self._default_outputs())

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        obs = batch[SampleBatch.OBS]
        with torch.no_grad():
            action_logits = self.policy(obs)
        action_logits_inference = torch.argmax(action_logits, dim=-1)
        action_dist = TorchDeterministic(action_logits_inference)
        return {"action_dist": action_dist}

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        with torch.no_grad():
            return self._forward_train(batch)

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        obs = batch[SampleBatch.OBS]
        action_logits = self.policy(obs)
        action_dist = TorchCategorical(logits=action_logits)
        return {"action_dist": action_dist}

    @override(TorchRLModule)
    def get_state(self) -> Mapping[str, Any]:
        return {"policy": self.policy.state_dict()}

    @override(TorchRLModule)
    def set_state(self, state: Mapping[str, Any]) -> None:
        self.policy.load_state_dict(state["policy"])

    def _default_inputs(self) -> dict:
        obs_dim = self.config.input_dim
        return {
            "obs": TorchTensorSpec("b, do", do=obs_dim),
        }

    def _default_outputs(self) -> dict:
        return {"action_dist": Distribution}


class BCTorchOptimizer(RLOptimizer):
    def __init__(self, module, config):
        super().__init__(module, config)

    @override(RLOptimizer)
    def compute_loss(
        self, batch: NestedDict, fwd_out: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Compute a loss"""
        action_dist = fwd_out["action_dist"]
        loss = -action_dist.logp(batch[SampleBatch.ACTIONS]).mean()
        loss_dict = {
            "total_loss": loss,
        }
        return loss_dict

    @override(RLOptimizer)
    def _configure_optimizers(self) -> List[torch.optim.Optimizer]:
        return {
            "module": torch.optim.SGD(
                self.module.parameters(), lr=self._config.get("lr", 1e-3)
            )
        }

    @override(RLOptimizer)
    def get_state(self):
        return {key: optim.state_dict() for key, optim in self.get_optimizers().items()}

    @override(RLOptimizer)
    def set_state(self, state: Mapping[Any, Any]) -> None:
        assert set(state.keys()) == set(self.get_state().keys()) or not state
        for key, optim_dict in state.items():
            self.get_optimizers()[key].load_state_dict(optim_dict)


class BCTorchTrainer:
    def __init__(self, config: Mapping[str, any]) -> None:
        module_config = config["module_config"]
        optimizer_config = {}
        self._module = BCTorchModule(module_config)
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
        return {
            "avg_reward": batch["rewards"].mean(),
            "module_norm": model_norm(self._module),
            **loss_numpy,
        }

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
        gradients = self.compute_gradients(loss)
        post_processed_gradients = self.on_after_compute_gradients(gradients)
        self.apply_gradients(post_processed_gradients)
        results = self.compile_results(batch, fwd_out, loss, post_processed_gradients)
        return results

    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        """Perform an update on self._module

            For example compute and apply gradients to self._module if
                necessary.

        Args:
            loss: variable(s) used for optimizing self._module.

        Returns:
            A dictionary of extra information and statistics.
        """
        # for torch
        if isinstance(loss, dict):
            loss = loss["total_loss"]
        loss.backward()
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

    def test_rl_optimizer_example_torch(self):
        torch.manual_seed(1)
        env = gym.make("CartPole-v1")
        module_config = FCConfig(
            input_dim=sum(env.observation_space.shape),
            output_dim=sum(env.action_space.shape or [env.action_space.n]),
            hidden_layers=[32],
        )
        module_for_inference = BCTorchModule(module_config)

        trainer = BCTorchTrainer({"module_config": module_config})
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
                trainer.update(batch)
            module_for_inference.set_state(trainer.get_state()["module_state"])
            avg_return, _, _ = do_rollouts(env, module_for_inference, 10)
            if avg_return > 50:
                break
        assert (
            avg_return > 50
        ), f"Return for training behavior cloning is too low: avg_return={avg_return}!"

    def test_rl_optimizer_set_state_get_state_torch(self):
        env = gym.make("CartPole-v1")
        module_config = FCConfig(
            input_dim=sum(env.observation_space.shape),
            output_dim=sum(env.action_space.shape or [env.action_space.n]),
            hidden_layers=[32],
        )
        module = BCTorchModule(module_config)
        optim1 = BCTorchOptimizer(module, {"lr": 0.1})
        optim2 = BCTorchOptimizer(module, {"lr": 0.2})

        assert isinstance(
            optim1.get_state(), dict
        ), "rl_optimizer.get_state() should return a dict"
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
