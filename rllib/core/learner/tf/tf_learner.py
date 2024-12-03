import logging
import pathlib
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    RLModuleSpec,
)
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.policy.eager_tf_policy import _convert_to_tf
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import (
    ModuleID,
    Optimizer,
    Param,
    ParamDict,
    StateDict,
    TensorType,
)

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class TfLearner(Learner):

    framework: str = "tf2"

    def __init__(self, **kwargs):
        # by default in rllib we disable tf2 behavior
        # This call re-enables it as it is needed for using
        # this class.
        try:
            tf1.enable_v2_behavior()
        except ValueError:
            # This is a hack to avoid the error that happens when calling
            # enable_v2_behavior after variables have already been created.
            pass

        super().__init__(**kwargs)

        self._enable_tf_function = self.config.eager_tracing

        # This is a placeholder which will be filled by
        # `_make_distributed_strategy_if_necessary`.
        self._strategy: tf.distribute.Strategy = None

    @OverrideToImplementCustomLogic
    @override(Learner)
    def configure_optimizers_for_module(
        self, module_id: ModuleID, config: "AlgorithmConfig" = None
    ) -> None:
        module = self._module[module_id]

        # For this default implementation, the learning rate is handled by the
        # attached lr Scheduler (controlled by self.config.lr, which can be a
        # fixed value or a schedule setting).
        optimizer = tf.keras.optimizers.Adam()
        params = self.get_parameters(module)

        # This isn't strictly necessary, but makes it so that if a checkpoint is
        # computed before training actually starts, then it will be the same in
        # shape / size as a checkpoint after training starts.
        optimizer.build(module.trainable_variables)

        # Register the created optimizer (under the default optimizer name).
        self.register_optimizer(
            module_id=module_id,
            optimizer=optimizer,
            params=params,
            lr_or_lr_schedule=config.lr,
        )

    @override(Learner)
    def compute_gradients(
        self,
        loss_per_module: Dict[str, TensorType],
        gradient_tape: "tf.GradientTape",
        **kwargs,
    ) -> ParamDict:
        total_loss = sum(loss_per_module.values())
        grads = gradient_tape.gradient(total_loss, self._params)
        return grads

    @override(Learner)
    def apply_gradients(self, gradients_dict: ParamDict) -> None:
        # TODO (Avnishn, kourosh): apply gradients doesn't work in cases where
        #  only some agents have a sample batch that is passed but not others.
        #  This is probably because of the way that we are iterating over the
        #  parameters in the optim_to_param_dictionary.
        for optimizer in self._optimizer_parameters:
            optim_grad_dict = self.filter_param_dict_for_optimizer(
                optimizer=optimizer, param_dict=gradients_dict
            )
            variable_list = []
            gradient_list = []
            for param_ref, grad in optim_grad_dict.items():
                if grad is not None:
                    variable_list.append(self._params[param_ref])
                    gradient_list.append(grad)
            optimizer.apply_gradients(zip(gradient_list, variable_list))

    @override(Learner)
    def restore_from_path(self, path: Union[str, pathlib.Path]) -> None:
        # This operation is potentially very costly because a MultiRLModule is created
        # at build time, destroyed, and then a new one is created from a checkpoint.
        # However, it is necessary due to complications with the way that Ray Tune
        # restores failed trials. When Tune restores a failed trial, it reconstructs the
        # entire experiment from the initial config. Therefore, to reflect any changes
        # made to the learner's modules, the module created by Tune is destroyed and
        # then rebuilt from the checkpoint.
        with self._strategy.scope():
            super().restore_from_path(path)

    @override(Learner)
    def _get_optimizer_state(self) -> StateDict:
        optim_state = {}
        with tf.init_scope():
            for name, optim in self._named_optimizers.items():
                optim_state[name] = [var.numpy() for var in optim.variables()]
        return optim_state

    @override(Learner)
    def _set_optimizer_state(self, state: StateDict) -> None:
        for name, state_array in state.items():
            if name not in self._named_optimizers:
                raise ValueError(
                    f"Optimizer {name} in `state` is not known! "
                    f"Known optimizers are {self._named_optimizers.keys()}"
                )
            optim = self._named_optimizers[name]
            optim.set_weights(state_array)

    @override(Learner)
    def get_param_ref(self, param: Param) -> Hashable:
        return param.ref()

    @override(Learner)
    def get_parameters(self, module: RLModule) -> Sequence[Param]:
        return list(module.trainable_variables)

    @override(Learner)
    def _is_module_compatible_with_learner(self, module: RLModule) -> bool:
        return isinstance(module, TfRLModule)

    @override(Learner)
    def _check_registered_optimizer(
        self,
        optimizer: Optimizer,
        params: Sequence[Param],
    ) -> None:
        super()._check_registered_optimizer(optimizer, params)
        if not isinstance(optimizer, tf.keras.optimizers.Optimizer):
            raise ValueError(
                f"The optimizer ({optimizer}) is not a tf keras optimizer! "
                "Only use tf.keras.optimizers.Optimizer subclasses for TfLearner."
            )
        for param in params:
            if not isinstance(param, tf.Variable):
                raise ValueError(
                    f"One of the parameters ({param}) in the registered optimizer "
                    "is not a tf.Variable!"
                )

    @override(Learner)
    def _convert_batch_type(self, batch: MultiAgentBatch) -> MultiAgentBatch:
        batch = _convert_to_tf(batch.policy_batches)
        length = max(len(b) for b in batch.values())
        batch = MultiAgentBatch(batch, env_steps=length)
        return batch

    @override(Learner)
    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: RLModuleSpec,
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
            )
        if self._enable_tf_function:
            self._possibly_traced_update = tf.function(
                self._untraced_update, reduce_retracing=True
            )

    @override(Learner)
    def remove_module(self, module_id: ModuleID, **kwargs) -> MultiRLModuleSpec:
        with self._strategy.scope():
            marl_spec = super().remove_module(module_id, **kwargs)

        if self._enable_tf_function:
            self._possibly_traced_update = tf.function(
                self._untraced_update, reduce_retracing=True
            )

        return marl_spec

    def _make_distributed_strategy_if_necessary(self) -> "tf.distribute.Strategy":
        """Create a distributed strategy for the learner.

        A stratgey is a tensorflow object that is used for distributing training and
        gradient computation across multiple devices. By default, a no-op strategy is
        used that is not distributed.

        Returns:
            A strategy for the learner to use for distributed training.

        """
        if self._distributed:
            strategy = tf.distribute.MultiWorkerMirroredStrategy()
        elif self._use_gpu:
            # mirrored strategy is typically used for multi-gpu training
            # on a single machine, however we can use it for single-gpu
            devices = tf.config.list_logical_devices("GPU")
            assert self._local_gpu_idx < len(devices), (
                f"local_gpu_idx {self._local_gpu_idx} is not a valid GPU id or is "
                "not available."
            )
            local_gpu = [devices[self._local_gpu_idx].name]
            strategy = tf.distribute.MirroredStrategy(devices=local_gpu)
        else:
            # the default strategy is a no-op that can be used in the local mode
            # cpu only case, build will override this if needed.
            strategy = tf.distribute.get_strategy()
        return strategy

    @override(Learner)
    def build(self) -> None:
        """Build the TfLearner.

        This method is specific TfLearner. Before running super() it sets the correct
        distributing strategy with the right device, so that computational graph is
        placed on the correct device. After running super(), depending on eager_tracing
        flag it will decide whether to wrap the update function with tf.function or not.
        """

        # we call build anytime we make a learner, or load a learner from a checkpoint.
        # we can't make a new strategy every time we build, so we only make one the
        # first time build is called.
        if not self._strategy:
            self._strategy = self._make_distributed_strategy_if_necessary()

        with self._strategy.scope():
            super().build()

        if self._enable_tf_function:
            self._possibly_traced_update = tf.function(
                self._untraced_update, reduce_retracing=True
            )
        else:
            self._possibly_traced_update = self._untraced_update

    @override(Learner)
    def _update(self, batch: Dict) -> Tuple[Any, Any, Any]:
        return self._possibly_traced_update(batch)

    def _untraced_update(
        self,
        batch: Dict,
        # TODO: Figure out, why _ray_trace_ctx=None helps to prevent a crash in
        #  eager_tracing=True mode.
        #  It seems there may be a clash between the traced-by-tf function and the
        #  traced-by-ray functions (for making the TfLearner class a ray actor).
        _ray_trace_ctx=None,
    ):
        # Activate tensor-mode on our MetricsLogger.
        self.metrics.activate_tensor_mode()

        def helper(_batch):
            with tf.GradientTape(persistent=True) as tape:
                fwd_out = self._module.forward_train(_batch)
                loss_per_module = self.compute_losses(fwd_out=fwd_out, batch=_batch)
            gradients = self.compute_gradients(loss_per_module, gradient_tape=tape)
            del tape
            postprocessed_gradients = self.postprocess_gradients(gradients)
            self.apply_gradients(postprocessed_gradients)

            # Deactivate tensor-mode on our MetricsLogger and collect the (tensor)
            # results.
            return fwd_out, loss_per_module, self.metrics.deactivate_tensor_mode()

        return self._strategy.run(helper, args=(batch,))

    @override(Learner)
    def _get_tensor_variable(self, value, dtype=None, trainable=False) -> "tf.Tensor":
        return tf.Variable(
            value,
            trainable=trainable,
            dtype=(
                dtype
                or (
                    tf.float32
                    if isinstance(value, float)
                    else tf.int32
                    if isinstance(value, int)
                    else None
                )
            ),
        )

    @staticmethod
    @override(Learner)
    def _get_optimizer_lr(optimizer: "tf.Optimizer") -> float:
        return optimizer.lr

    @staticmethod
    @override(Learner)
    def _set_optimizer_lr(optimizer: "tf.Optimizer", lr: float) -> None:
        # When tf creates the optimizer, it seems to detach the optimizer's lr value
        # from the given tf variable.
        # Thus, updating this variable is NOT sufficient to update the actual
        # optimizer's learning rate, so we have to explicitly set it here inside the
        # optimizer object.
        optimizer.lr = lr

    @staticmethod
    @override(Learner)
    def _get_clip_function() -> Callable:
        from ray.rllib.utils.tf_utils import clip_gradients

        return clip_gradients

    @staticmethod
    @override(Learner)
    def _get_global_norm_function() -> Callable:
        return tf.linalg.global_norm
