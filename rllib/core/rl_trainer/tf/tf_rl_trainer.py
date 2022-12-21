import numpy as np
from typing import Any, Mapping, Union
from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy

tf1, tf, tfv = try_import_tf()
tf1.enable_eager_execution()


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
        batch = NestedDict(batch.policy_batches)
        for key, value in batch.items():
            batch[key] = tf.convert_to_tensor(value, dtype=tf.float32)
        if self.distributed:
            infos = self.strategy.run(self._do_update_fn, args=(batch,))
        else:
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
        trainable_variables = {
            module_id: self._module[module_id].trainable_variables()
            for module_id in self._module.keys()
        }
        grads = tape.gradient(loss["total_loss"], trainable_variables)
        return grads

    def apply_gradients(self, gradients: Mapping[str, Any]) -> None:
        """Perform an update on self._module"""
        trainable_variables = {
            module_id: self._module[module_id].trainable_variables()
            for module_id in self._module.keys()
        }
        for module_id, rl_optimizer in self._rl_optimizer.get_optimizers().items():
            for key, optimizer in rl_optimizer.get_optimizers().items():
                optimizer.apply_gradients(
                    zip(gradients[module_id][key], trainable_variables[module_id][key])
                )

    def make_distributed(self):
        self.strategy = tf.distribute.MultiWorkerMirroredStrategy()
        with self.strategy.scope():
            module = self.module_class.from_model_config(
                **self.module_config
            ).as_multi_agent()
            optimizer = self.optimizer_class(
                module, self.optimizer_config
            ).as_multi_agent()
        return module, optimizer

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
        batch = convert_to_numpy(batch)
        post_processed_gradients = convert_to_numpy(dict(post_processed_gradients))
        mean_grads = {}
        for module_id, module_grads in post_processed_gradients.items():
            mean_grads[module_id] = {}
            for network, grads in module_grads.items():
                mean_grads[module_id][network] = []
                for grad in grads:
                    mean_grads[module_id][network].append(grad.mean())
                mean_grads[module_id][network] = np.mean(mean_grads[module_id][network])
        ret = {
            **loss_numpy,
            "mean_gradient": mean_grads,
        }
        module_avg_weights = {}

        if self.debug:
            for module_id in self._module.keys():
                avg_weights = []
                weights = self._module[module_id].get_weights()
                for weight_array in weights:
                    avg_weights.append(weight_array.mean())
                avg_weights = np.mean(avg_weights)
                module_avg_weights[module_id] = avg_weights
            ret["mean_weight"] = module_avg_weights
        return ret
