# TODO (avnishn): Merge with the torch version of this test once the
# RLTrainer has been merged.
import gymnasium as gym
from typing import Any, Mapping, Union
import unittest

import ray
from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.offline import IOContext
from ray.rllib.offline.dataset_reader import (
    DatasetReader,
    get_dataset_and_shards,
)
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_optimizer import BCTFOptimizer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()
tf1.enable_eager_execution()


class BCTFTrainer:
    """This class is a demonstration on how to use RLOptimizer and RLModule together."""

    def __init__(self, env: gym.Env) -> None:
        optimizer_config = {}
        self._module = DiscreteBCTFModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )
        self._rl_optimizer = BCTFOptimizer.from_module(self._module, optimizer_config)

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

    @tf.function
    def _do_update_fn(self, batch: SampleBatch) -> Mapping[str, Any]:
        with tf.GradientTape() as tape:
            fwd_out = self._module.forward_train(batch)
            loss = self._rl_optimizer.compute_loss(batch, fwd_out)
            if isinstance(loss, tf.Tensor):
                loss = {"total_loss": loss}
        gradients = self.compute_gradients(loss, tape)
        self.apply_gradients(gradients)
        return {"loss": loss, "fwd_out": fwd_out, "post_processed_gradients": gradients}

    def update(self, batch: SampleBatch) -> Mapping[str, Any]:
        """Perform an update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        # TODO(sven): This is a hack to get around the fact that
        # SampleBatch.count becomes 0 after decorating the function with
        # tf.function. This messes with input spec checking. Other fields of
        # the sample batch are possibly modified by tf.function which may lead
        # to unwanted consequences. We'll need to further investigate this.
        batch = NestedDict(batch)
        for key, value in batch.items():
            batch[key] = tf.convert_to_tensor(value, dtype=tf.float32)
        infos = self._do_update_fn(batch)
        loss = infos["loss"]
        fwd_out = infos["fwd_out"]
        post_processed_gradients = infos["post_processed_gradients"]
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
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_rl_optimizer_in_behavioral_cloning_tf(self):
        tf.random.set_seed(1)
        env = gym.make("CartPole-v1")
        trainer = BCTFTrainer(env)

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

    def test_rl_optimizer_set_state_get_state_tf(self):
        env = gym.make("CartPole-v1")

        trainer1 = BCTFTrainer(env)
        trainer2 = BCTFTrainer(env)

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
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
