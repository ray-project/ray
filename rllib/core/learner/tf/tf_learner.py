import logging
from typing import (
    Any,
    Mapping,
    Union,
    Type,
    Optional,
    Callable,
    Sequence,
    Hashable,
    Set,
)

from ray.rllib.core.learner.learner import (
    FrameworkHPs,
    Learner,
    ParamOptimizerPairs,
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


class TfLearner(Learner):

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
        # cpu only case, build will override this if needed.
        self._strategy = tf.distribute.get_strategy()

    @override(Learner)
    def configure_optimizers(self) -> ParamOptimizerPairs:
        """Configures the optimizers for the Learner.

        By default it sets up a single Adam optimizer for each sub-module in module
        accessible via `moduel.keys()`.
        """
        # TODO (Kourosh): convert optimizer_config to dataclass later.
        lr = self._optimizer_config["lr"]
        return [
            (
                self._module[key].trainable_variables,
                tf.keras.optimizers.Adam(learning_rate=lr),
            )
            for key in self._module.keys()
        ]

    @override(Learner)
    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]], tape: "tf.GradientTape"
    ) -> ParamDictType:
        grads = tape.gradient(loss[self.TOTAL_LOSS_KEY], self._params)
        return grads

    @override(Learner)
    def apply_gradients(self, gradients: ParamDictType) -> None:
        # TODO (Avnishn, kourosh): apply gradients doesn't work in cases where
        # only some agents have a sample batch that is passed but not others.
        # This is probably because of the way that we are iterating over the
        # parameters in the optim_to_param_dictionary
        for optim, param_ref_seq in self._optim_to_param.items():
            variable_list = [self._params[param_ref] for param_ref in param_ref_seq]
            gradient_list = [gradients[param_ref] for param_ref in param_ref_seq]
            optim.apply_gradients(zip(gradient_list, variable_list))

    @override(Learner)
    def get_weights(self, module_ids: Optional[Set[str]] = None) -> Mapping[str, Any]:
        """Returns the weights of the underlying MultiAgentRLModule"""
        module_weights = self._module.get_state()
        if module_ids is None:
            return module_weights

        return {k: v for k, v in module_weights.items() if k in module_ids}

    @override(Learner)
    def set_weights(self, weights: Mapping[str, Any]) -> None:
        self._module.set_state(weights)

    @override(Learner)
    def get_param_ref(self, param: ParamType) -> Hashable:
        return param.ref()

    @override(Learner)
    def get_parameters(self, module: RLModule) -> Sequence[ParamType]:
        return list(module.trainable_variables)

    @override(Learner)
    def get_optimizer_obj(
        self, module: RLModule, optimizer_cls: Type[Optimizer]
    ) -> Optimizer:
        lr = self._optimizer_config["lr"]
        return optimizer_cls(learning_rate=lr)

    @override(Learner)
    def _convert_batch_type(self, batch: MultiAgentBatch) -> NestedDict[TensorType]:
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
            batch[key] = tf.cast(tf.convert_to_tensor(value), tf.float32)
        return batch.asdict()

    @override(Learner)
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

    @override(Learner)
    def remove_module(self, module_id: ModuleID) -> None:
        with self._strategy.scope():
            super().remove_module(module_id)
        if self._enable_tf_function:
            self._update_fn = tf.function(self._do_update_fn, reduce_retracing=True)

    @override(Learner)
    def build(self) -> None:
        """Build the TfLearner.

        This method is specific TfLearner. Before running super() it sets the correct
        distributing strategy with the right device, so that computational graph is
        placed on the correct device. After running super(), depending on eager_tracing
        flag it will decide whether to wrap the update function with tf.function or not.
        """
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

    @override(Learner)
    def update(
        self,
        batch: MultiAgentBatch,
        *,
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        reduce_fn: Callable[[ResultDict], ResultDict] = ...,
    ) -> Mapping[str, Any]:
        # TODO (Kourosh): The update of learner is vastly differnet than the base
        # class. So we need to unify them.

        if set(batch.policy_batches.keys()) != set(self._module.keys()):
            raise ValueError(
                "Batch keys must match module keys. Learner does not "
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
            minibatch = self._convert_batch_type(minibatch)
            update_outs = self._update_fn(minibatch)
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

    def _do_update_fn(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        # TODO (Avnish): Match this base class's implementation.
        def helper(_batch):
            # TODO (Kourosh): We need to go back to NestedDict because that's the
            # constraint on forward_train and compute_loss APIs. This seems to be
            # in-efficient. Make it efficient.
            _batch = NestedDict(_batch)
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
