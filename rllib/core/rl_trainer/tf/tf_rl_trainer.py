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
    FrameworkHPs,
    RLTrainer,
    ParamOptimizerPairs,
    ParamRef,
    Optimizer,
    ParamType,
    ParamDictType,
)
from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    ModuleID,
    SingleAgentRLModuleSpec,
)
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType, ResultDict
from ray.rllib.utils.minibatch_utils import (
    MiniBatchDummyIterator,
    MiniBatchCyclicIterator,
)
from ray.rllib.utils.nested_dict import NestedDict


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
        *,
        framework_hyperparameters: Optional[FrameworkHPs] = FrameworkHPs(),
        **kwargs,
    ):
        super().__init__(framework_hyperparameters=framework_hyperparameters, **kwargs)

        tf1.enable_eager_execution()

        self._enable_tf_function = framework_hyperparameters.eager_tracing
        # the default strategy is a no-op that can be used in the local mode
        # cpu only case
        self._strategy = tf.distribute.get_strategy()

    @override(RLTrainer)
    def build(self) -> None:
        if self._distributed:
            self._strategy = tf.distribute.MultiWorkerMirroredStrategy()
        else:
            if self._use_gpu:
                # mirrored strategy is typically used for multi-gpu training
                # on a single machine, however we can use it for single-gpu
                devices = tf.config.list_logical_devices("GPU")
                assert self._local_gpu_idx < len(devices), (
                    f"local_gpu_idx {self._local_gpu_idx} is not a valid GPU id or is "
                    " not available."
                )
                local_gpu = [devices[self._local_gpu_idx].name]
                self._strategy = tf.distribute.MirroredStrategy(devices=local_gpu)
        with self._strategy.scope():
            super().build()

        if self._enable_tf_function:
            self._update_fn = tf.function(self._do_update_fn, reduce_retracing=True)
        else:
            self._update_fn = self._do_update_fn

    def _do_update_fn(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        # TODO (Avnish): Match this base class's implementation.
        def helper(_batch):
            with tf.GradientTape() as tape:
                fwd_out = self._module.forward_train(_batch)
                loss = self.compute_loss(fwd_out=fwd_out, batch=_batch)
                if isinstance(loss, tf.Tensor):
                    loss = {"total_loss": loss}
            gradients = self.compute_gradients(loss, tape)
            gradients = self.postprocess_gradients(gradients)
            self.apply_gradients(gradients)
            return {
                "loss": loss,
                "fwd_out": fwd_out,
                "postprocessed_gradients": gradients,
            }

        return self._strategy.run(helper, args=(batch,))

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
    def update(
        self,
        batch: MultiAgentBatch,
        *,
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        reduce_fn: Callable[[ResultDict], ResultDict] = ...,
    ) -> Mapping[str, Any]:
        if set(batch.policy_batches.keys()) != set(self._module.keys()):
            raise ValueError(
                "Batch keys must match module keys. RLTrainer does not "
                "currently support training of only some modules and not others"
            )

        batch_iter = (
            MiniBatchCyclicIterator
            if minibatch_size is not None
            else MiniBatchDummyIterator
        )

        results = []
        for minibatch in batch_iter(batch, minibatch_size, num_iters):
            # TODO (Avnish): converting to tf tensor and then from nested dict back to
            # dict will most likely hit us in perf. But let's go with this for now.
            minibatch = self.convert_batch_to_tf_tensor(minibatch)
            update_outs = self._update_fn(minibatch.asdict())
            loss = update_outs["loss"]
            fwd_out = update_outs["fwd_out"]
            postprocessed_gradients = update_outs["postprocessed_gradients"]
            result = self.compile_results(batch, fwd_out, loss, postprocessed_gradients)
            results.append(result)

        # Reduce results across all minibatches, if necessary.
        if len(results) == 1:
            return results[0]
        else:
            if reduce_fn is None:
                return results
            return reduce_fn(results)

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
    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
        set_optimizer_fn: Optional[Callable[[RLModule], ParamOptimizerPairs]] = None,
        optimizer_cls: Optional[Type[Optimizer]] = None,
    ) -> None:
        # TODO(Avnishn):
        # WARNING:tensorflow:Using MirroredStrategy eagerly has significant overhead
        # currently. We will be working on improving this in the future, but for now
        # please wrap `call_for_each_replica` or `experimental_run` or `run` inside a
        # tf.function to get the best performance.
        # I get this warning any time I add a new module. I see the warning a few times
        # and then it disappears. I think that I will need to open an issue with the TF
        # team.
        with self._strategy.scope():
            super().add_module(
                module_id=module_id,
                module_spec=module_spec,
                set_optimizer_fn=set_optimizer_fn,
                optimizer_cls=optimizer_cls,
            )
        if self._enable_tf_function:
            self._update_fn = tf.function(self._do_update_fn, reduce_retracing=True)

    @override(RLTrainer)
    def remove_module(self, module_id: ModuleID) -> None:
        with self._strategy.scope():
            super().remove_module(module_id)
        if self._enable_tf_function:
            self._update_fn = tf.function(self._do_update_fn, reduce_retracing=True)

    def convert_batch_to_tf_tensor(self, batch: MultiAgentBatch) -> NestedDict:
        """Convert the arrays of batch to tf.Tensor's.

        Note: This is an in place operation.

        Args:
            batch: The batch to convert.

        Returns:
            The converted batch.

        """
        # TODO(avnishn): This is a hack to get around the fact that
        # SampleBatch.count becomes 0 after decorating the function with
        # tf.function. This messes with input spec checking. Other fields of
        # the sample batch are possibly modified by tf.function which may lead
        # to unwanted consequences. We'll need to further investigate this.
        batch = NestedDict(batch.policy_batches)
        for key, value in batch.items():
            batch[key] = tf.convert_to_tensor(value, dtype=tf.float32)
        return batch

    def get_weights(self) -> Mapping[str, Any]:
        # TODO (Kourosh) Implement this.
        raise NotImplementedError

    def set_weights(self, weights: Mapping[str, Any]) -> None:
        # TODO (Kourosh) Implement this.
        raise NotImplementedError

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
