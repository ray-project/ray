import numpy as np
from typing import Any, Mapping, Tuple, Union, Type
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.core.optim.marl_optimizer import MultiAgentRLOptimizer
from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy


tf1, tf, tfv = try_import_tf()
tf1.enable_eager_execution()


class TfRLTrainer(RLTrainer):
    """Base class for RLlib TF algorithm trainers.

    Args:
        module_class: The (MA)RLModule class to use.
        module_kwargs: The kwargs for the (MA)RLModule.
        optimizer_class: The optimizer class to use.
        optimizer_kwargs: The kwargs for the optimizer.
        scaling_config: A mapping that holds the world size and rank of this
            trainer. Note this is only used for distributed training.
        distributed: Whether this trainer is distributed or not.

    Abstract Methods:
        compute_gradients: Compute gradients for the module being optimized.
        apply_gradients: Apply gradients to the module being optimized with respect to
            a loss that is computed by the optimizer.

    Example:
        .. code-block:: python

        trainer = MyRLTrainer(module_class, module_kwargs, optimizer_class,
                optimizer_kwargs, scaling_config)
        trainer.init_trainer()
        batch = ...
        results = trainer.update(batch)

        # add a new module, perhaps for league based training or lru caching
        trainer.add_module("new_player", NewPlayerCls, new_player_kwargs,
            NewPlayerOptimCls, new_player_optim_kwargs)

        batch = ...
        results = trainer.update(batch)  # will train previous and new modules.

        # remove a module
        trainer.remove_module("new_player")

        batch = ...
        results = trainer.update(batch)  # will train previous modules only.

        # get the state of the trainer
        state = trainer.get_state()

        # set the state of the trainer
        trainer.set_state(state)

    """

    def __init__(
        self,
        module_class: Union[Type[RLModule], Type[MultiAgentRLModule]],
        module_kwargs: Mapping[str, Any],
        optimizer_class: Union[Type[RLOptimizer], Type[MultiAgentRLOptimizer]],
        optimizer_kwargs: Mapping[str, Any],
        scaling_config: Mapping[str, Any],
        distributed: bool = False,
        use_tf_function: bool = True,
    ):
        super().__init__(
            module_class=module_class,
            module_kwargs=module_kwargs,
            optimizer_class=optimizer_class,
            optimizer_kwargs=optimizer_kwargs,
            scaling_config=scaling_config,
            distributed=distributed,
        )
        self._use_tf_function = use_tf_function
        if self._use_tf_function:
            self._update_fn = tf.function(self._do_update_fn)
        else:
            self._update_fn = self._do_update_fn

    def _do_update_fn(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        with tf.GradientTape() as tape:
            fwd_out = self._module.forward_train(batch)
            loss = self._rl_optimizer.compute_loss(batch=batch, fwd_out=fwd_out)
            if isinstance(loss, tf.Tensor):
                loss = {"total_loss": loss}
        gradients = self.compute_gradients(loss, tape)
        gradients = self.on_after_compute_gradients(gradients)
        self.apply_gradients(gradients)
        return {"loss": loss, "fwd_out": fwd_out, "post_processed_gradients": gradients}

    @override(RLTrainer)
    def update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        batch = self.convert_batch_to_tf_tensor(batch)
        if self.distributed:
            update_outs = self.do_distributed_update(batch)
        else:
            update_outs = self._update_fn(batch)
        loss = update_outs["loss"]
        fwd_out = update_outs["fwd_out"]
        post_processed_gradients = update_outs["post_processed_gradients"]
        results = self.compile_results(batch, fwd_out, loss, post_processed_gradients)
        return results

    @override(RLTrainer)
    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]], tape: tf.GradientTape
    ) -> Mapping[str, Any]:
        trainable_variables = {
            module_id: self._module[module_id].trainable_variables()
            for module_id in self._module.keys()
        }
        grads = tape.gradient(loss["total_loss"], trainable_variables)
        return grads

    @override(RLTrainer)
    def apply_gradients(self, gradients: Mapping[str, Any]) -> None:
        trainable_variables = {
            module_id: self._module[module_id].trainable_variables()
            for module_id in self._module.keys()
        }
        for module_id, rl_optimizer in self._rl_optimizer.get_rl_optimizers().items():
            for key, optimizer in rl_optimizer.get_optimizers().items():
                optimizer.apply_gradients(
                    zip(gradients[module_id][key], trainable_variables[module_id][key])
                )

    @override(RLTrainer)
    def _make_distributed(self) -> Tuple[RLModule, RLOptimizer]:
        self.strategy = tf.distribute.MultiWorkerMirroredStrategy()
        with self.strategy.scope():
            if issubclass(self.module_class, MultiAgentRLModule):
                module = self.module_class.from_multi_agent_config(**self.module_kwargs)
            elif issubclass(self.module_class, RLModule):
                module = self.module_class.from_model_config(
                    **self.module_kwargs
                ).as_multi_agent()
            else:
                raise ValueError(
                    f"Module class {self.module_class} is not a subclass of "
                    f"RLModule or MultiAgentRLModule."
                )
            if issubclass(self.optimizer_class, RLOptimizer):
                optimizer = self.optimizer_class.from_module(
                    module, **self.optimizer_kwargs
                )
                optimizer = optimizer.as_multi_agent()
            elif issubclass(self.optimizer_class, MultiAgentRLOptimizer):
                optimizer = self.optimizer_class.from_marl_module(
                    module, **self.optimizer_kwargs
                )
            else:
                raise ValueError(
                    f"Optimizer class {self.optimizer_class} is not a subclass of "
                    f"RLOptimizer or MultiAgentRLOptimizer."
                )
        return module, optimizer

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
        post_processed_gradients = convert_to_numpy(dict(post_processed_gradients))
        mean_grads = {}
        for module_id, module_grads in post_processed_gradients.items():
            mean_grads[module_id] = {}
            for network, grads in module_grads.items():
                mean_grads[module_id][network] = []
                for grad in grads:
                    mean_grads[module_id][network].append(grad.mean())
                mean_grads[module_id][network] = np.mean(mean_grads[module_id][network])

        module_avg_weights = {}
        for module_id in self._module.keys():
            avg_weights = []
            weights = self._module[module_id].get_weights()
            for weight_array in weights:
                avg_weights.append(weight_array.mean())
            avg_weights = np.mean(avg_weights)
            module_avg_weights[module_id] = avg_weights
        ret = {
            "loss": loss_numpy,
            "mean_gradient": mean_grads,
            "mean_weight": module_avg_weights,
        }
        return ret

    @override(RLTrainer)
    def add_module(
        self,
        module_id: ModuleID,
        module_cls: Type[RLModule],
        module_kwargs,
        optimizer_cls,
        optimizer_kwargs,
    ) -> None:
        if self.distributed:
            with self.strategy.scope():
                super().add_module(
                    module_id,
                    module_cls,
                    module_kwargs,
                    optimizer_cls,
                    optimizer_kwargs,
                )
        else:
            super().add_module(
                module_id, module_cls, module_kwargs, optimizer_cls, optimizer_kwargs
            )
        if self._use_tf_function:
            self._update_fn = tf.function(self._do_update_fn)

    @override(RLTrainer)
    def remove_module(self, module_id: ModuleID) -> None:
        if self.distributed:
            with self.strategy.scope():
                super().remove_module(module_id)
        else:
            super().remove_module(module_id)
        if self._use_tf_function:
            self._update_fn = tf.function(self._do_update_fn)

    @override(RLTrainer)
    def do_distributed_update(self, batch: NestedDict) -> Mapping[str, Any]:
        update_outs = self.strategy.run(self._update_fn, args=(batch,))
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
        batch = batch.policy_batches
        for module_id, sample_batch in batch.items():
            for key, value in sample_batch.items():
                sample_batch[key] = tf.convert_to_tensor(value, dtype=tf.float32)
            batch[module_id] = dict(sample_batch)

        return batch
