import json
import logging
import pathlib
from typing import (
    Any,
    Callable,
    Hashable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from ray.rllib.core.learner.learner import (
    FrameworkHyperparameters,
    Learner,
    LearnerHyperparameters,
)
from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    ModuleID,
    SingleAgentRLModuleSpec,
)
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.policy.eager_tf_policy import _convert_to_tf
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.serialization import convert_numpy_to_python_primitives
from ray.rllib.utils.typing import Optimizer, Param, ParamDict, TensorType


tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class TfLearner(Learner):

    framework: str = "tf2"

    def __init__(
        self,
        *,
        framework_hyperparameters: Optional[FrameworkHyperparameters] = None,
        **kwargs,
    ):

        # by default in rllib we disable tf2 behavior
        # This call re-enables it as it is needed for using
        # this class.
        try:
            tf1.enable_v2_behavior()
        except ValueError:
            # This is a hack to avoid the error that happens when calling
            # enable_v2_behavior after variables have already been created.
            pass

        super().__init__(
            framework_hyperparameters=(
                framework_hyperparameters or FrameworkHyperparameters()
            ),
            **kwargs,
        )

        self._enable_tf_function = self._framework_hyperparameters.eager_tracing

        # This is a placeholder which will be filled by
        # `_make_distributed_strategy_if_necessary`.
        self._strategy: tf.distribute.Strategy = None

    @OverrideToImplementCustomLogic
    @override(Learner)
    def configure_optimizers_for_module(
        self, module_id: ModuleID, hps: LearnerHyperparameters
    ) -> None:
        module = self._module[module_id]

        # For this default implementation, the learning rate is handled by the
        # attached lr Scheduler (controlled by self.hps.learning_rate, which can be a
        # fixed value of a schedule setting).
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
            lr_or_lr_schedule=hps.learning_rate,
        )

    @override(Learner)
    def compute_gradients(
        self,
        loss_per_module: Mapping[str, TensorType],
        gradient_tape: "tf.GradientTape",
        **kwargs,
    ) -> ParamDict:
        grads = gradient_tape.gradient(loss_per_module[ALL_MODULES], self._params)
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
    def load_state(
        self,
        path: Union[str, pathlib.Path],
    ) -> None:
        # This operation is potentially very costly because a MARL Module is created at
        # build time, destroyed, and then a new one is created from a checkpoint.
        # However, it is necessary due to complications with the way that Ray Tune
        # restores failed trials. When Tune restores a failed trial, it reconstructs the
        # entire experiment from the initial config. Therefore, to reflect any changes
        # made to the learner's modules, the module created by Tune is destroyed and
        # then rebuilt from the checkpoint.
        with self._strategy.scope():
            super().load_state(path)

    def _save_optimizer_hparams(
        self,
        path: pathlib.Path,
        optim: "tf.keras.optimizers.Optimizer",
        optim_name: str,
    ) -> None:
        """Save the hyperparameters of optim to path/optim_name_hparams.json.

        Args:
            path: The path to the directory to save the hyperparameters to.
            optim: The optimizer to save the hyperparameters of.
            optim_name: The name of the optimizer.

        """
        hparams = tf.keras.optimizers.serialize(optim)
        hparams = tf.nest.map_structure(convert_numpy_to_python_primitives, hparams)
        with open(path / f"{optim_name}_hparams.json", "w") as f:
            json.dump(hparams, f)

    def _save_optimizer_state(
        self,
        path: pathlib.Path,
        optim: "tf.keras.optimizers.Optimizer",
        optim_name: str,
    ) -> None:
        """Save the state variables of optim to path/optim_name_state.txt.

        Args:
            path: The path to the directory to save the state to.
            optim: The optimizer to save the state of.
            optim_name: The name of the optimizer.

        """
        state = optim.variables()
        serialized_tensors = [tf.io.serialize_tensor(tensor) for tensor in state]
        contents = tf.strings.join(serialized_tensors, separator="tensor: ")
        tf.io.write_file(str(path / f"{optim_name}_state.txt"), contents)

    @override(Learner)
    def _save_optimizers(self, path: Union[str, pathlib.Path]) -> None:
        path = pathlib.Path(path)
        path.mkdir(parents=True, exist_ok=True)
        for name, optim in self._named_optimizers.items():
            self._save_optimizer_hparams(path, optim, name)
            self._save_optimizer_state(path, optim, name)

    def _load_optimizer_from_hparams(
        self, path: pathlib.Path, optim_name: str
    ) -> "tf.keras.optimizers.Optimizer":
        """Load an optimizer from the hyperparameters saved at path/optim_name_hparams.json.

        Args:
            path: The path to the directory to load the hyperparameters from.
            optim_name: The name of the optimizer.

        Returns:
            The optimizer loaded from the hyperparameters.

        """
        with open(path / f"{optim_name}_hparams.json", "r") as f:
            state = json.load(f)
        return tf.keras.optimizers.deserialize(state)

    def _load_optimizer_state(
        self,
        path: pathlib.Path,
        optim: "tf.keras.optimizers.Optimizer",
        optim_name: str,
    ) -> None:
        """Load the state of optim from the state saved at path/optim_name_state.txt.

        Args:
            path: The path to the directory to load the state from.
            optim: The optimizer to load the state into.
            optim_name: The name of the optimizer.

        """
        contents = tf.io.read_file(str(path / f"{optim_name}_state.txt"))
        serialized_tensors = tf.strings.split(contents, sep="tensor: ")
        unserialized_optim_state = []
        for serialized_tensor, optim_tensor in zip(
            serialized_tensors, optim.variables()
        ):
            unserialized_optim_state.append(
                tf.io.parse_tensor(serialized_tensor, optim_tensor.dtype)
            )

        # set the state of the optimizer to the state that was saved
        optim.set_weights(unserialized_optim_state)

    @override(Learner)
    def _load_optimizers(self, path: Union[str, pathlib.Path]) -> None:
        path = pathlib.Path(path)
        for name in self._named_optimizers.keys():
            new_optim = self._load_optimizer_from_hparams(path, name)
            old_optim = self._named_optimizers[name]

            # assign replace the old optim with the new optim in the learner's state
            self._named_optimizers[name] = new_optim
            param_seq = self._optimizer_parameters.pop(old_optim)
            self._optimizer_parameters[new_optim] = []
            for param_ref in param_seq:
                self._optimizer_parameters[new_optim].append(param_ref)

            # delete the old optimizer / free its memory
            del old_optim
            # these are the variables that the optimizer is supposed to optimize over
            variable_list = [
                self._params[param_ref]
                for param_ref in self._optimizer_parameters[new_optim]
            ]
            # initialize the optimizer with the variables that it is supposed to
            # optimize over
            new_optim.build(variable_list)

            # This loads in the actual state of the optimizer.
            self._load_optimizer_state(path, new_optim, name)

    @override(Learner)
    def set_module_state(self, state: Mapping[str, Any]) -> None:
        self._module.set_state(state)

    @override(Learner)
    def get_optimizer_state(self) -> Mapping[str, Any]:
        optim_state = {}
        with tf.init_scope():
            for name, optim in self._named_optimizers.items():
                optim_state[name] = [var.numpy() for var in optim.variables()]
        return optim_state

    @override(Learner)
    def set_optimizer_state(self, state: Mapping[str, Any]) -> None:
        for name, state_array in state.items():
            if name not in self._named_optimizers:
                raise ValueError(
                    f"Optimizer {name} in weights is not known."
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
        module_spec: SingleAgentRLModuleSpec,
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
    def remove_module(self, module_id: ModuleID) -> None:
        with self._strategy.scope():
            super().remove_module(module_id)

        if self._enable_tf_function:
            self._possibly_traced_update = tf.function(
                self._untraced_update, reduce_retracing=True
            )

    def _make_distributed_strategy_if_necessary(self) -> "tf.distribute.Strategy":
        """Create a distributed strategy for the learner.

        A stratgey is a tensorflow object that is used for distributing training and
        gradient computation across multiple devices. By default a no-op strategy is
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
    def _update(self, batch: NestedDict) -> Tuple[Any, Any, Any]:
        return self._possibly_traced_update(batch)

    def _untraced_update(
        self,
        batch: NestedDict,
        # TODO: Figure out, why _ray_trace_ctx=None helps to prevent a crash in
        #  eager_tracing=True mode.
        #  It seems there may be a clash between the traced-by-tf function and the
        #  traced-by-ray functions (for making the TfLearner class a ray actor).
        _ray_trace_ctx=None,
    ):
        def helper(_batch):
            # TODO (Kourosh, Sven): We need to go back to NestedDict because that's the
            #  constraint on forward_train and compute_loss APIs. This seems to be
            #  in-efficient. However, for tf>=2.12, it works also w/o this conversion
            #  so remove this after we upgrade officially to tf==2.12.
            _batch = NestedDict(_batch)
            with tf.GradientTape(persistent=True) as tape:
                fwd_out = self._module.forward_train(_batch)
                loss_per_module = self.compute_loss(fwd_out=fwd_out, batch=_batch)
            gradients = self.compute_gradients(loss_per_module, gradient_tape=tape)
            del tape
            postprocessed_gradients = self.postprocess_gradients(gradients)
            self.apply_gradients(postprocessed_gradients)

            return fwd_out, loss_per_module, dict(self._metrics)

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
