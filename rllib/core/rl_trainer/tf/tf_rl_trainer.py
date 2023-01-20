import numpy as np
from typing import Any, Mapping, Union, Type
from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
import tree  # pip install dm-tree


tf1, tf, tfv = try_import_tf()
tf1.enable_eager_execution()


class TfRLTrainer(RLTrainer):
    """Base class for RLlib TF algorithm trainers."""

    def _do_update_fn(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        with tf.GradientTape() as tape:
            fwd_out = self._module.forward_train(batch)
            loss = self.compute_loss(fwd_out=fwd_out, batch=batch)
            if isinstance(loss, tf.Tensor):
                loss = {"total_loss": loss}
        gradients = self.compute_gradients(loss, tape)
        gradients = self.on_after_compute_gradients(gradients)
        self.apply_gradients(gradients)
        return {"loss": loss, "fwd_out": fwd_out, "post_processed_gradients": gradients}

    @override(RLTrainer)
    def configure_optimizers(self):
        lr = self.optimizer_config.get("lr", 1e-3)
        return [
            (
                self._module[key].trainable_variables,
                tf.keras.optimizers.Adam(learning_rate=lr),
            )
            for key in self._module.keys()
        ]

    @override(RLTrainer)
    def update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        if not hasattr(self, "traced_update_fn"):
            self.traced_update_fn = tf.function(self._do_update_fn)
        batch = self.convert_batch_to_tf_tensor(batch)
        if self.distributed:
            update_outs = self.do_distributed_update(batch)
        else:
            update_outs = self.traced_update_fn(batch)
        loss = update_outs["loss"]
        fwd_out = update_outs["fwd_out"]
        post_processed_gradients = update_outs["post_processed_gradients"]
        results = self.compile_results(batch, fwd_out, loss, post_processed_gradients)
        return results

    @override(RLTrainer)
    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]], tape: tf.GradientTape
    ) -> Mapping[str, Any]:
        grads = tape.gradient(loss["total_loss"], self._params)
        return grads

    @override(RLTrainer)
    def apply_gradients(self, gradients: Mapping[str, Any]) -> None:
        for optim, param_groups, grad_groups in zip(
            self._optimizers, self._params, gradients
        ):
            optim.apply_gradients(zip(grad_groups, param_groups))

    @override(RLTrainer)
    def _make_distributed(self) -> RLModule:
        # TODO: Does strategy has to be an attribute here? if so it's very hidden to
        # the user of this class that there is such an attribute.

        # TODO (Kourosh, Avnish): The optimizers still need to be created within
        # strategy.scope. Otherwise parameters of optimizers won't be properly
        # synced
        self.strategy = tf.distribute.MultiWorkerMirroredStrategy()
        with self.strategy.scope():
            module = self._make_module()
        return module

    @override(RLTrainer)
    def compile_results(
        self,
        batch: NestedDict,
        fwd_out: Mapping[str, Any],
        postprocessed_loss: Mapping[str, Any],
        post_processed_gradients: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        loss_numpy = convert_to_numpy(postprocessed_loss)
        batch = convert_to_numpy(batch)
        post_processed_gradients = convert_to_numpy(post_processed_gradients)
        mean_grads = [grad.mean() for grad in tree.flatten(post_processed_gradients)]
        ret = {
            "loss": loss_numpy,
            "mean_gradient": np.mean(mean_grads),
        }

        if self.in_test:
            # this is to check if in the multi-gpu case, the weights across workers are
            # the same. It is really only needed during testing.
            mean_ws = {}
            for module_id in self._module.keys():
                m = self._module[module_id]
                mean_ws[module_id] = np.mean([w.mean() for w in m.get_weights()])
            ret["mean_weight"] = mean_ws
        return ret

    @override(RLTrainer)
    def add_module(
        self,
        module_id: ModuleID,
        module_cls: Type[RLModule],
        module_kwargs,
    ) -> None:
        if self.distributed:
            with self.strategy.scope():
                super().add_module(
                    module_id,
                    module_cls,
                    module_kwargs,
                )
        else:
            super().add_module(module_id, module_cls, module_kwargs)
        self.traced_update_fn = tf.function(self._do_update_fn)

    @override(RLTrainer)
    def remove_module(self, module_id: ModuleID) -> None:
        if self.distributed:
            with self.strategy.scope():
                super().remove_module(module_id)
        else:
            super().remove_module(module_id)
        self.traced_update_fn = tf.function(self._do_update_fn)

    @override(RLTrainer)
    def do_distributed_update(self, batch: NestedDict) -> Mapping[str, Any]:
        update_outs = self.strategy.run(self.traced_update_fn, args=(batch,))
        return update_outs

    def convert_batch_to_tf_tensor(self, batch: MultiAgentBatch) -> NestedDict:
        """Convert the arrays of batch to tf.Tensor's.

        Note: This is an in place operation.

        Args:
            batch: The batch to convert.

        Returns:
            The converted batch.

        """
        # TODO(sven): This is a hack to get around the fact that
        # SampleBatch.count becomes 0 after decorating the function with
        # tf.function. This messes with input spec checking. Other fields of
        # the sample batch are possibly modified by tf.function which may lead
        # to unwanted consequences. We'll need to further investigate this.
        batch = NestedDict(batch.policy_batches)
        for key, value in batch.items():
            batch[key] = tf.convert_to_tensor(value, dtype=tf.float32)
        return batch
