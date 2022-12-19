import tensorflow as tf
from typing import Any, Mapping, Tuple, Union

from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.nested_dict import NestedDict


class TfRLTrainer(RLTrainer):
    """Base class for RLlib TF algorithm trainers."""

    def remove_module(self, module_id: ModuleID):
        self._module.remove_module(module_id)

    def add_module(self, module_id: ModuleID, module: RLModule):
        self._module.add_module(module_id, module)

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
        infos = self.strategy.run(self._do_update_fn, args=(batch,))
        loss = infos["loss"]
        fwd_out = infos["fwd_out"]
        post_processed_gradients = infos["post_processed_gradients"]
        results = self.compile_results(batch, fwd_out, loss, post_processed_gradients)
        # return results

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

    def make_distributed(self):
        self.strategy = tf.distribute.MultiWorkerMirroredStrategy()
        with self.strategy.scope():
            module = self.module_class.from_model_config(**self.module_config)
            optimizer = self.optimizer_class(module, self.optimizer_config)
        self._module = module
        self._rl_optimizer = optimizer
