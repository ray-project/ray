import json
import logging
import numpy as np
import pathlib
import tree  # pip install dm-tree
from typing import (
    Any,
    Callable,
    Hashable,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from ray.rllib.core.learner.learner import (
    FrameworkHyperparameters,
    Learner,
    ParamOptimizerPair,
    NamedParamOptimizerPairs,
    ParamType,
    ParamDictType,
)
from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    ModuleID,
    SingleAgentRLModuleSpec,
)
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import clip_gradients
from ray.rllib.utils.typing import TensorType, ResultDict
from ray.rllib.utils.minibatch_utils import (
    MiniBatchDummyIterator,
    MiniBatchCyclicIterator,
)
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.serialization import convert_numpy_to_python_primitives


tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class TfLearner(Learner):

    framework: str = "tf"

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

    @override(Learner)
    def configure_optimizer_per_module(
        self, module_id: ModuleID
    ) -> Union[ParamOptimizerPair, NamedParamOptimizerPairs]:
        module = self._module[module_id]
        lr = self._optimizer_config["lr"]
        optim = tf.keras.optimizers.Adam(learning_rate=lr)
        pair: ParamOptimizerPair = (
            self.get_parameters(module),
            optim,
        )
        # this isn't strictly necessary, but makes it so that if a checkpoint is
        # computed before training actually starts, then it will be the same in
        # shape / size as a checkpoint after training starts.
        optim.build(module.trainable_variables)
        return pair

    @override(Learner)
    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]], tape: "tf.GradientTape"
    ) -> ParamDictType:
        grads = tape.gradient(loss[self.TOTAL_LOSS_KEY], self._params)
        return grads

    @override(Learner)
    def postprocess_gradients(
        self,
        gradients_dict: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Postprocesses gradients depending on the optimizer config."""

        # Perform gradient clipping, if necessary.
        clip_gradients(
            gradients_dict,
            grad_clip=self._optimizer_config.get("grad_clip"),
            grad_clip_by=self._optimizer_config.get("grad_clip_by"),
        )

        return gradients_dict

    @override(Learner)
    def apply_gradients(self, gradients: ParamDictType) -> None:
        # TODO (Avnishn, kourosh): apply gradients doesn't work in cases where
        #  only some agents have a sample batch that is passed but not others.
        #  This is probably because of the way that we are iterating over the
        #  parameters in the optim_to_param_dictionary.
        for optim, param_ref_seq in self._optimizer_parameters.items():
            variable_list = [
                self._params[param_ref]
                for param_ref in param_ref_seq
                if gradients[param_ref] is not None
            ]
            gradient_list = [
                gradients[param_ref]
                for param_ref in param_ref_seq
                if gradients[param_ref] is not None
            ]
            optim.apply_gradients(zip(gradient_list, variable_list))

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
    def set_weights(self, weights: Mapping[str, Any]) -> None:
        self._module.set_state(weights)

    @override(Learner)
    def get_optimizer_weights(self) -> Mapping[str, Any]:
        optim_weights = {}
        with tf.init_scope():
            for name, optim in self._named_optimizers.items():
                optim_weights[name] = [var.numpy() for var in optim.variables()]
        return optim_weights

    @override(Learner)
    def set_optimizer_weights(self, weights: Mapping[str, Any]) -> None:
        for name, weight_array in weights.items():
            if name not in self._named_optimizers:
                raise ValueError(
                    f"Optimizer {name} in weights is not known."
                    f"Known optimizers are {self._named_optimizers.keys()}"
                )
            optim = self._named_optimizers[name]
            optim.set_weights(weight_array)

    @override(Learner)
    def get_param_ref(self, param: ParamType) -> Hashable:
        return param.ref()

    @override(Learner)
    def get_parameters(self, module: RLModule) -> Sequence[ParamType]:
        return list(module.trainable_variables)

    def _is_module_compatible_with_learner(self, module: RLModule) -> bool:
        return isinstance(module, TfRLModule)

    @override(Learner)
    def _check_structure_param_optim_pair(self, param_optim_pair: Any) -> None:
        super()._check_structure_param_optim_pair(param_optim_pair)
        params, optim = param_optim_pair
        if not isinstance(optim, tf.keras.optimizers.Optimizer):
            raise ValueError(
                f"The optimizer in {param_optim_pair} is not a tf keras optimizer. "
                "Please use a tf.keras.optimizers.Optimizer for TfLearner."
            )
        for param in params:
            if not isinstance(param, tf.Variable):
                raise ValueError(
                    f"One of the parameters {param} in this ParamOptimizerPair "
                    f"{param_optim_pair} is not a tf.Variable. Please use a "
                    "tf.Variable for TfLearner. You can retrieve the param with a call "
                    "to learner.get_param_ref(tensor)."
                )

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
        ma_batch = NestedDict(batch.policy_batches)
        for key, value in ma_batch.items():
            if isinstance(value, np.ndarray):
                ma_batch[key] = tf.convert_to_tensor(value, dtype=tf.float32)
        return ma_batch

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
            self._update_fn = tf.function(self._do_update_fn, reduce_retracing=True)

    @override(Learner)
    def remove_module(self, module_id: ModuleID) -> None:
        with self._strategy.scope():
            super().remove_module(module_id)
        if self._enable_tf_function:
            self._update_fn = tf.function(self._do_update_fn, reduce_retracing=True)

    def _make_distributed_strategy_if_necessary(self) -> "tf.distribute.Strategy":
        """Create a distributed strategy for the learner.

        A stratgey is a tensorflow object that is used for distributing training and
        gradient computation across multiple devices. By default a no-op strategy is
        that is not distributed is used.

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
                " not available."
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
        #  class. So we need to unify them.
        missing_module_ids = set(batch.policy_batches.keys()) - set(self._module.keys())
        if len(missing_module_ids) > 0:
            raise ValueError(
                "Batch contains module ids that are not in the learner: "
                f"{missing_module_ids}"
            )

        batch_iter = (
            MiniBatchCyclicIterator
            if minibatch_size is not None
            else MiniBatchDummyIterator
        )

        results = []
        for minibatch in batch_iter(batch, minibatch_size, num_iters):
            # TODO (Avnish): converting to tf tensor and then from nested dict back to
            #  dict will most likely hit us in perf. But let's go with this for now.
            tensorbatch = self._convert_batch_type(minibatch)
            update_outs = self._update_fn(tensorbatch)
            loss = update_outs["loss"]
            fwd_out = update_outs["fwd_out"]
            postprocessed_gradients = update_outs["postprocessed_gradients"]
            result = self.compile_results(batch, fwd_out, loss, postprocessed_gradients)
            self._check_result(result)
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
            #  constraint on forward_train and compute_loss APIs. This seems to be
            #  in-efficient. Make it efficient.
            _batch = NestedDict(_batch)
            with tf.GradientTape() as tape:
                fwd_out = self._module.forward_train(_batch)
                loss = self.compute_loss(fwd_out=fwd_out, batch=_batch)
                if isinstance(loss, tf.Tensor):
                    loss = {"total_loss": loss}
            gradients = self.compute_gradients(loss, tape)
            gradients = self.postprocess_gradients(gradients)
            self.apply_gradients(gradients)

            # NOTE (Kourosh) The reason for returning fwd_out is that it is optionally
            # needed for compiling the results in a later step (e.g. in
            # compile_results), but it should not contain anything but tensors, None or
            # ExtensionTypes, otherwise the tf.function will yell at us because it
            # won't be able to convert the returned objects to a tensor representation
            # (for internal reasons). So, in here, we remove anything from fwd_out that
            # is not a tensor, None or ExtensionType.
            def filter_fwd_out(x):
                if isinstance(
                    x, (tf.Tensor, type(None), tf.experimental.ExtensionType)
                ):
                    return x
                return None

            return {
                "loss": loss,
                "fwd_out": tree.map_structure(filter_fwd_out, fwd_out),
                "postprocessed_gradients": gradients,
            }

        return self._strategy.run(helper, args=(batch,))
