import logging
from typing import (
    Any,
    Mapping,
    Union,
    Type,
    Optional,
    Callable,
    Dict,
    Sequence,
    Hashable,
)

from ray.rllib.core.rl_trainer.rl_trainer import (
    RLTrainer,
    MultiAgentRLModule,
    ParamOptimizerPairs,
    ParamRef,
    Optimizer,
    ParamType,
    ParamDictType,
)
from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.nested_dict import NestedDict

from ray.air.config import ScalingConfig

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class TfRLTrainer(RLTrainer):
    """Base class for RLlib TF algorithm trainers

    Args:
        module_class: The (MA)RLModule class to use.
        module_kwargs: The kwargs for the (MA)RLModule.
        optimizer_config: The config for the optimizer.
        distributed: Whether this trainer is distributed or not.
        enable_tf_function: Whether to enable tf.function tracing for the update
            function.

    Abstract Methods:
        compute_gradients: Compute gradients for the module being optimized.
        apply_gradients: Apply gradients to the module being optimized with respect to
            a loss that is computed by the optimizer.

    Example:
        .. code-block:: python

        trainer = MyRLTrainer(module_class, module_kwargs, optimizer_config)
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

    framework: str = "tf"

    def __init__(
        self,
        module_class: Union[Type[RLModule], Type[MultiAgentRLModule]],
        module_kwargs: Mapping[str, Any],
        optimizer_config: Mapping[str, Any],
        distributed: bool = False,
        scaling_config: Optional[ScalingConfig] = None,
        enable_tf_function: bool = True,
    ):
        super().__init__(
            module_class=module_class,
            module_kwargs=module_kwargs,
            optimizer_config=optimizer_config,
            distributed=distributed,
            scaling_config=scaling_config,
        )

        # TODO (Kourosh): This is required to make sure tf computes the values in the
        # end. Two question remains:
        # 1. Why is it not eager by default. Do we do anything in try_import_tf() that
        # changes this default?
        # 2. What is the implication of this on the performance? The tf documentation
        # does not mention this as a requirement?
        tf1.enable_eager_execution()

        self._enable_tf_function = enable_tf_function
        if self._enable_tf_function:
            self._update_fn = tf.function(self._do_update_fn)
        else:
            self._update_fn = self._do_update_fn

    def _do_update_fn(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        with tf.GradientTape() as tape:
            fwd_out = self._module.forward_train(batch)
            loss = self.compute_loss(fwd_out=fwd_out, batch=batch)
            if isinstance(loss, tf.Tensor):
                loss = {"total_loss": loss}
        gradients = self.compute_gradients(loss, tape)
        gradients = self.postprocess_gradients(gradients)
        self.apply_gradients(gradients)
        return {"loss": loss, "fwd_out": fwd_out, "postprocessed_gradients": gradients}

    @override(RLTrainer)
    def configure_optimizers(self) -> ParamOptimizerPairs:
        # TODO (Kourosh): convert optimizer_config to dataclass later.
        lr = self.optimizer_config["lr"]
        return [
            (
                self._module[key].trainable_variables,
                tf.keras.optimizers.Adam(learning_rate=lr),
            )
            for key in self._module.keys()
        ]

    @override(RLTrainer)
    def update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        if set(batch.policy_batches.keys()) != set(self._module.keys()):
            raise ValueError(
                "Batch keys must match module keys. RLTrainer does not "
                "currently support training of only some modules and not others"
            )
        batch = self.convert_batch_to_tf_tensor(batch)
        if self.distributed:
            update_outs = self.do_distributed_update(batch)
        else:
            update_outs = self._update_fn(batch)
        loss = update_outs["loss"]
        fwd_out = update_outs["fwd_out"]
        postprocessed_gradients = update_outs["postprocessed_gradients"]
        results = self.compile_results(batch, fwd_out, loss, postprocessed_gradients)
        return results

    @override(RLTrainer)
    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]], tape: "tf.GradientTape"
    ) -> ParamDictType:
        grads = tape.gradient(loss[self.TOTAL_LOSS_KEY], self._params)
        return grads

    @override(RLTrainer)
    def apply_gradients(self, gradients: Dict[ParamRef, TensorType]) -> None:

        # TODO (Avnishn, kourosh): apply gradients doesn't work in cases where
        # only some agents have a sample batch that is passed but not others.
        # This is probably because of the way that we are iterating over the
        # parameters in the optim_to_param_dictionary
        for optim, param_ref_seq in self._optim_to_param.items():
            variable_list = [self._params[param_ref] for param_ref in param_ref_seq]
            gradient_list = [gradients[param_ref] for param_ref in param_ref_seq]
            optim.apply_gradients(zip(gradient_list, variable_list))

    @override(RLTrainer)
    def _make_distributed_module(self) -> MultiAgentRLModule:
        # TODO (Kourosh): Does strategy has to be an attribute here? if so it's very
        # hidden to the user of this class that there is such an attribute.

        # TODO (Kourosh, Avnish): The optimizers still need to be created within
        # strategy.scope. Otherwise parameters of optimizers won't be properly
        # synced
        self.strategy = tf.distribute.MultiWorkerMirroredStrategy()
        with self.strategy.scope():
            module = self._make_module()
        return module

    @override(RLTrainer)
    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_cls: Type[RLModule],
        module_kwargs: Mapping[str, Any],
        set_optimizer_fn: Optional[Callable[[RLModule], ParamOptimizerPairs]] = None,
        optimizer_cls: Optional[Type[Optimizer]] = None,
    ) -> None:
        if self.distributed:
            with self.strategy.scope():
                super().add_module(
                    module_id=module_id,
                    module_cls=module_cls,
                    module_kwargs=module_kwargs,
                    set_optimizer_fn=set_optimizer_fn,
                    optimizer_cls=optimizer_cls,
                )
        else:
            super().add_module(
                module_id=module_id,
                module_cls=module_cls,
                module_kwargs=module_kwargs,
                set_optimizer_fn=set_optimizer_fn,
                optimizer_cls=optimizer_cls,
            )
        if self._enable_tf_function:
            self._update_fn = tf.function(self._do_update_fn)

    @override(RLTrainer)
    def remove_module(self, module_id: ModuleID) -> None:
        if self.distributed:
            with self.strategy.scope():
                super().remove_module(module_id)
        else:
            super().remove_module(module_id)
        if self._enable_tf_function:
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
        batch = NestedDict(batch.policy_batches)
        for key, value in batch.items():
            batch[key] = tf.convert_to_tensor(value, dtype=tf.float32)
        return batch

    @override(RLTrainer)
    def get_parameters(self, module: RLModule) -> Sequence[ParamType]:
        return module.trainable_variables

    @override(RLTrainer)
    def get_param_ref(self, param: ParamType) -> Hashable:
        return param.ref()

    @override(RLTrainer)
    def get_optimizer_obj(
        self, module: RLModule, optimizer_cls: Type[Optimizer]
    ) -> Optimizer:
        lr = self.optimizer_config.get("lr", 1e-3)
        return optimizer_cls(learning_rate=lr)
