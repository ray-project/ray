from dataclasses import dataclass, field
import gym
import pytest
import numpy as np
import tensorflow as tf
from typing import Any, List, Mapping, Union
import unittest

from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.offline import IOContext
from ray.rllib.offline.dataset_reader import (
    DatasetReader,
    get_dataset_and_shards,
)

import ray
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TFRLModule
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.specs.specs_dict import ModelSpec, check_specs
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.models.tf.tf_action_dist import (
    Categorical,
    Deterministic,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.typing import TensorType


def do_rollouts(
    env: gym.Env, module_for_inference: RLModule, num_rollouts: int
) -> SampleBatch:
    _returns = []
    _rollouts = []
    for _ in range(num_rollouts):
        obs = env.reset()
        _obs, _next_obs, _actions, _rewards, _dones = [], [], [], [], []
        _return = -0
        for _ in range(env._max_episode_steps):
            _obs.append(obs)
            fwd_out = module_for_inference.forward_inference(
                {"obs": tf.convert_to_tensor([obs], dtype=tf.float32)}
            )
            action = convert_to_numpy(fwd_out["action_dist"].sample())[0]
            next_obs, reward, done, _ = env.step(action)
            _next_obs.append(next_obs)
            _actions.append([action])
            _rewards.append([reward])
            _dones.append([done])
            _return += reward
            if done:
                break
            obs = next_obs
        batch = SampleBatch(
            {
                "obs": _obs,
                "next_obs": _next_obs,
                "actions": _actions,
                "rewards": _rewards,
                "dones": _dones,
            }
        )
        _returns.append(_return)
        _rollouts.append(batch)
    return np.mean(_returns), _returns, _rollouts


@dataclass
class FCConfig:
    """Configuration for a fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
        hidden_layers: The sizes of the hidden layers.
        activation: The activation function to use after each layer (except for the
            output).
        output_activation: The activation function to use for the output layer.
    """

    input_dim: int = None
    output_dim: int = None
    hidden_layers: List[int] = field(default_factory=lambda: [256, 256])
    activation: tf.keras.layers.Layer = tf.keras.layers.ReLU
    output_activation: str = None


class TFFCNet(tf.keras.Model):
    """A simple fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
        hidden_layers: The sizes of the hidden layers.
        activation: The activation function to use after each layer.
    """

    def __init__(self, config: FCConfig):
        super().__init__()
        self.input_dim = config.input_dim
        self.hidden_layers = config.hidden_layers

        activation_class = config.activation
        self._layers = []
        self._layers.append(tf.keras.Input(shape=(self.input_dim,)))

        for i in range(len(self.hidden_layers)):
            self._layers.append(activation_class())
            self._layers.append(tf.keras.layers.Dense(self.hidden_layers[i]))

        if config.output_dim is not None:
            self._layers.append(activation_class())
            self._layers.append(tf.keras.layers.Dense(config.output_dim))

        if config.output_dim is None:
            self.output_dim = config.hidden_layers[-1]
        else:
            self.output_dim = config.output_dim

        self._layers = tf.keras.Sequential(self._layers)

        self._input_specs = self.input_specs()

    def input_specs(self):
        return TFTensorSpecs("b, h", h=self.input_dim)

    @check_specs(input_spec="_input_specs")
    def call(self, x, training=False):
        return self._layers(x)


class BCTFModule(TFRLModule):
    def __init__(self, config: FCConfig) -> None:
        super().__init__(config)
        self.policy = TFFCNet(config)

    def input_specs(self) -> ModelSpec:
        obs_dim = self.config.input_dim
        return ModelSpec(
            {
                "obs": TFTensorSpecs("b, do", do=obs_dim),
                "actions": TFTensorSpecs("b"),
            }
        )

    def output_specs(self) -> ModelSpec:
        return ModelSpec({"action_dist": ActionDistribution})

    def input_specs_exploration(self) -> ModelSpec:
        obs_dim = self.config.input_dim
        return ModelSpec(
            {
                "obs": TFTensorSpecs("b, do", do=obs_dim),
            }
        )

    def input_specs_inference(self) -> ModelSpec:
        obs_dim = self.config.input_dim
        return ModelSpec(
            {
                "obs": TFTensorSpecs("b, do", do=obs_dim),
            }
        )

    def input_specs_train(self) -> ModelSpec:
        return self.input_specs()

    def output_specs_exploration(self) -> ModelSpec:
        return self.output_specs()

    def output_specs_inference(self) -> ModelSpec:
        return self.output_specs()

    def output_specs_train(self) -> ModelSpec:
        return self.output_specs()

    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        obs = batch[SampleBatch.OBS]
        action_logits = self.policy(obs)
        action_logits_inference = tf.argmax(action_logits, axis=-1)
        action_dist = Deterministic(action_logits_inference, None)
        return {"action_dist": action_dist}

    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        return self._forward_inference(batch)

    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        obs = batch[SampleBatch.OBS]
        action_logits = self.policy(obs)
        action_dist = Categorical(action_logits)
        return {"action_dist": action_dist}

    def is_distributed(self) -> bool:
        return False

    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        pass

    def get_state(self) -> Mapping[str, Any]:
        return {"policy": self.policy.get_weights()}

    def set_state(self, state: Mapping[str, Any]) -> None:
        self.policy.set_weights(state["policy"])

    def trainable_variables(self) -> Mapping[str, Any]:
        return {"policy": self.policy.trainable_variables}


class BCTFOptimizer(RLOptimizer):
    def __init__(self, module, config):
        super().__init__(module, config)

    def compute_loss(
        self, batch: NestedDict, fwd_out: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Compute a loss"""
        action_dist = fwd_out["action_dist"]
        loss = -tf.math.reduce_mean(action_dist.logp(batch[SampleBatch.ACTIONS]))
        loss_dict = {
            "total_loss": loss,
        }
        return loss_dict

    def get_state(self):
        return {
            key: optim.get_weights() for key, optim in self.get_optimizers().items()
        }

    def set_state(self, state: Mapping[str, Any]) -> None:
        assert set(state.keys()) == set(self.get_state().keys()) or not state
        for key, optim_dict in state.items():
            self.get_optimizers()[key].set_weights(optim_dict)

    def _configure_optimizers(self) -> Mapping[str, Any]:
        return {
            "policy": tf.keras.optimizers.Adam(
                learning_rate=self._config.get("lr", 1e-3)
            )
        }


class BCTFTrainer:
    def __init__(self, config: Mapping[str, any]) -> None:
        module_config = config["module_config"]
        optimizer_config = {}
        self._module = BCTFModule(module_config)
        self._rl_optimizer = BCTFOptimizer(self._module, optimizer_config)

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
            "avg_reward": tf.math.reduce_mean(batch["rewards"]).numpy(),
        }
        results.update(loss_numpy)
        return results

    def update(self, batch: SampleBatch) -> Mapping[str, Any]:
        """Perform an update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        for key, value in batch.items():
            batch[key] = tf.convert_to_tensor(value, dtype=tf.float32)
        with tf.GradientTape() as tape:
            fwd_out = self._module.forward_train(batch)
            loss = self._rl_optimizer.compute_loss(batch, fwd_out)
        gradients = self.compute_gradients(loss, tape)
        post_processed_gradients = self.on_after_compute_gradients(gradients)
        self.apply_gradients(post_processed_gradients)
        results = self.compile_results(batch, fwd_out, loss, post_processed_gradients)
        return results

    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]], tape: tf.GradientTape
    ) -> Mapping[str, Any]:
        """Perform an update on self._module

            For example compute and apply gradients to self._module if
                necessary.

        Args:
            loss: variable(s) used for optimizing self._module.

        Returns:
            A dictionary of extra information and statistics.
        """
        grads = tape.gradient(loss["total_loss"], self._module.trainable_variables())
        return grads

    def apply_gradients(self, gradients: Mapping[str, Any]) -> None:
        """Perform an update on self._module"""
        for key, optimizer in self._rl_optimizer.get_optimizers().items():
            optimizer.apply_gradients(
                zip(gradients[key], self._module.trainable_variables()[key])
            )

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


class TestRLOptimizerTF(unittest.TestCase):
    def test_rl_optimizer_example_tf(self):
        ray.init()
        tf.random.set_seed(1)
        env = gym.make("CartPole-v1")
        module_config = FCConfig(
            input_dim=sum(env.observation_space.shape),
            output_dim=sum(env.action_space.shape or [env.action_space.n]),
            hidden_layers=[32],
        )
        module_for_inference = BCTFModule(module_config)

        trainer = BCTFTrainer({"module_config": module_config})
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
        trainer1 = BCTFTrainer({"module_config": module_config})
        trainer2 = BCTFTrainer({"module_config": module_config})

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
        batch = reader.next()

        trainer1.update(batch)
        trainer2.update(batch)

        # the first element of trainer1.get_state()["optimizer_state"]["policy"]
        # is the number of iterations that the optimizer has run, so we ignore it
        # by indexing into trainer.get_state()["optimizer_state"]["policy"][1]
        # The data under that index is optimizer weights
        check(
            trainer1.get_state()["optimizer_state"]["policy"][1][0],
            trainer2.get_state()["optimizer_state"]["policy"][1][0],
            false=True,
        )

        trainer1.set_state(trainer2.get_state())

        # run the an update again then check weights to see if they
        # are the same
        trainer1.update(batch)
        trainer2.update(batch)

        check(
            trainer1.get_state()["optimizer_state"]["policy"][1][0],
            trainer2.get_state()["optimizer_state"]["policy"][1][0],
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
