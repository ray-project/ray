import contextlib
import logging
from typing import Any, Dict, Optional, Tuple

from ray.rllib.algorithms.algorithm_config import (
    TorchCompileWhatToCompile,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.differentiable_learner import DifferentiableLearner
from ray.rllib.core.rl_module.torch.torch_rl_module import (
    TorchCompileConfig,
)
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import DeviceType, ModuleID, NamedParamDict, TensorType

logger = logging.getLogger("__name__")

torch, nn = try_import_torch()


# TODO (simon): Maybe create again a base class `TorchLearnable`.
class TorchDifferentiableLearner(DifferentiableLearner):
    """A `DifferentiableLearner` class leveraging PyTorch for functional updates.

    This class utilizes PyTorch 2.0's `func` module to perform functional
    updates on the provided parameters.
    """

    # Set the framework to `"torch"`.
    framework: str = "torch"

    def __init__(self, **kwargs):

        # First initialize the `DifferentiableLearner` base class to set
        # the configurations and `MultiRLModule`.
        super().__init__(**kwargs)

        # Whether to compile the RL Module of this learner. This implies that the.
        # forward_train method of the RL Module will be compiled. Further more,
        # other forward methods of the RL Module will be compiled on demand.
        # This is assumed to not happen, since other forwrad methods are not expected
        # to be used during training.
        self._torch_compile_forward_train = False
        self._torch_compile_cfg = None
        # Whether to compile the `_uncompiled_update` method of this learner. This
        # implies that everything within `_uncompiled_update` will be compiled,
        # not only the forward_train method of the RL Module.
        # Note that this is experimental.
        # Note that this requires recompiling the forward methods once we add/remove
        # RL Modules.
        self._torch_compile_complete_update = False
        if self.config.torch_compile_learner:
            if (
                self.config.torch_compile_learner_what_to_compile
                == TorchCompileWhatToCompile.COMPLETE_UPDATE
            ):
                self._torch_compile_complete_update = True
                self._compiled_update_initialized = False
            else:
                self._torch_compile_forward_train = True

            self._torch_compile_cfg = TorchCompileConfig(
                torch_dynamo_backend=self.config.torch_compile_learner_dynamo_backend,
                torch_dynamo_mode=self.config.torch_compile_learner_dynamo_mode,
            )

        # TODO (simon): See, if we can include these without a torch optimizer.
        # self._lr_schedulers = {}
        # self._lr_scheduler_classes = None
        # if self.config._torch_lr_scheduler_classes:
        #     self._lr_scheduler_classes = self.config._torch_lr_scheduler_classes

    def _uncompiled_update(
        self,
        batch: Dict,
        params: Dict[ModuleID, NamedParamDict],
        **kwargs,
    ) -> Tuple[Any, Any, Dict[ModuleID, NamedParamDict], Any]:
        """Performs a single functional update using a batch of data.

        This update operates on parameters passed via a functional call to the
        `MultiRLModule` and leverages PyTorch 2.0's `autograd` module. Parameters
        are not modified in-place within `self._module`; instead, updates are
        applied to the cloned parameters provided.

        Args:
            batch: A dictionary (or `MultiAgentBatch`) containing training data for
                all modules in the `MultiRLModule` (that should be trained).
            params: A dictionary of named parameters for each module id.

        Returns:
            A tuple consisting of:
                1) the output of a functional forward call to the RLModule using
                    `params`,
                2) the `loss_per_module` dictionary mapping module IDs to individual
                    loss tensors,
                3) the functionally updated parameters in the (dict) format passed in,
                4) a metrics dict mapping module IDs to metrics key/value pairs.

        """
        # TODO (sven): Causes weird cuda error when WandB is used.
        #  Diagnosis thus far:
        #  - All peek values during metrics.reduce are non-tensors.
        #  - However, in impala.py::training_step(), a tensor does arrive after learner
        #    group.update(), so somehow, there is still a race condition
        #    possible (learner, which performs the reduce() and learner thread, which
        #    performs the logging of tensors into metrics logger).
        self._compute_off_policyness(batch)

        # Make a functional forward pass with the provided parameters.
        fwd_out = self._make_functional_call(params, batch)
        loss_per_module = self.compute_losses(fwd_out=fwd_out, batch=batch)
        # Compute gradients for the provided parameters.
        gradients = self.compute_gradients(loss_per_module, params)

        with contextlib.ExitStack() as stack:
            if self.config.num_learners > 1:
                for mod in self.module.values():
                    # Skip non-torch modules, b/c they may not have the `no_sync` API.
                    if isinstance(mod, torch.nn.Module):
                        stack.enter_context(mod.no_sync())
            # TODO (simon): See, if we need here postprocessing of gradients.
            # postprocessed_gradients = self.postprocess_gradients(gradients)
            # Make a stateless (of `params`) update of the `RLModule` parameters.
            params = self.apply_gradients(gradients, params)

        # Deactivate tensor-mode on our MetricsLogger and collect the (tensor)
        # results.
        return fwd_out, loss_per_module, params, {}

    # TODO (simon): Maybe make type for gradients.
    @override(DifferentiableLearner)
    def compute_gradients(
        self,
        loss_per_module: Dict[ModuleID, TensorType],
        params: Dict[ModuleID, NamedParamDict],
        **kwargs,
    ) -> Dict[ModuleID, NamedParamDict]:
        """Computes functionally gradients based on the given losses.

        This method uses `torch.autograd.grad` to make the backward pass on the
        `MultiRLModule` which enables a functional backward pass. If a PyTorch
        optimizer is needed a differentiable one must be used (e.g. `torchopt`).

        Args:
            loss_per_module: Dict mapping module IDs to their individual total loss
                terms, computed by the individual `compute_loss_for_module()` calls.
                The overall total loss (sum of loss terms over all modules) is stored
                under `loss_per_module[ALL_MODULES]`
            params: A dictionary containing named parameters for each module id.
            **kwargs: Forward compatibility kwargs.

        Returns:
            The (named) gradients in the same (dict) format as `params`.
        """

        # TODO (simon): Add grad scalers later.
        total_loss = sum(loss_per_module.values())

        # Use `torch`'s `autograd` to compute gradients and create a graph, so we can
        # compute higher order gradients. Allow specified inputs not being used in outputs
        # as probably not all modules/parameters of a `MultiRLModule` are used in the loss.
        # Note, parameters are named parameters as this is needed by the
        # `torch.func.functional_call`
        # TODO (simon): Make sure this works for `MultiRLModule`s. This here can have
        # all parameter tensors in a list. But the `functional_call` above needs named
        # parameters for each module. Implement this via `foreach_module`.
        grads = torch.autograd.grad(
            total_loss,
            sum((list(param.values()) for mid, param in params.items()), []),
            create_graph=True,
            retain_graph=True,
            allow_unused=True,
        )

        # Map all gradients to their keys.
        named_grads = {
            module_id: {
                name: grad for (name, _), grad in zip(module_params.items(), grads)
            }
            for module_id, module_params in params.items()
        }

        return named_grads

    @override(DifferentiableLearner)
    def apply_gradients(
        self,
        gradients: Dict[ModuleID, NamedParamDict],
        params: Dict[ModuleID, NamedParamDict],
    ) -> Dict[ModuleID, NamedParamDict]:
        """Applies the given gradients in a functional manner.

        This method requires functional parameter updates, meaning modifications
        must not be performed in-place (e.g., using an optimizer or directly within
        the `MultiRLModule`).

        Args:
            gradients: A dictionary containing named gradients for each module id.
            params: A dictionary containing named parameters for each module id.

        Returns:
            The updated parameters in the same (dict) format as `params`.
        """
        policies_to_update = self.learner_config.policies_to_update or list(
            gradients.keys()
        )
        # Note, because this is a functional update we cannot apply in-place
        # modifications of parameters.
        updated_params = {}
        for module_id, module_grads in gradients.items():
            if module_id not in policies_to_update:
                updated_params[module_id] = params[module_id]
                continue
            updated_params[module_id] = {}
            for name, grad in module_grads.items():
                # If updates should not be skipped turn `nan` and `inf` gradients to zero.
                if (
                    not self.config.torch_skip_nan_gradients
                    and not torch.isfinite(grad).all()
                ):
                    # Warn the user about `nan` gradients.
                    logger.warning(f"Gradients {name} contain `nan/inf` values.")
                    # If updates should be skipped, do not step the optimizer and return.
                    if not self.config.torch_skip_nan_gradients:
                        logger.warning(
                            "Setting `nan/inf` gradients to zero. If updates with "
                            "`nan/inf` gradients should not be set to zero and instead "
                            "the update be skipped entirely set `torch_skip_nan_gradients` "
                            "to `True`."
                        )
                    # If necessary turn `nan` gradients to zero. Note, this can corrupt the
                    # internal state of the optimizer, if many `nan` gradients occur.
                    grad = torch.nan_to_num(grad)

                if self.config.torch_skip_nan_gradients or torch.isfinite(grad).all():
                    # Update each parameter, by a simple gradient descent step.
                    updated_params[module_id][name] = (
                        params[module_id][name] - self.learner_config.lr * grad
                    )
                elif grad is None or not torch.isfinite(grad).all():
                    logger.warning(
                        "Skipping this update. If updates with `nan/inf` gradients "
                        "should not be skipped entirely and instead `nan/inf` "
                        "gradients set to `zero` set `torch_skip_nan_gradients` to "
                        "`False`."
                    )
        return updated_params

    def _make_functional_call(
        self, params: Dict[ModuleID, NamedParamDict], batch: MultiAgentBatch
    ) -> Dict[ModuleID, NamedParamDict]:
        """Makes a functional call for each module in the `MultiRLModule`."""
        return self._module.foreach_module(
            lambda mid, m: torch.func.functional_call(m, params[mid], batch[mid]),
            return_dict=True,
        )

    @override(DifferentiableLearner)
    def _get_tensor_variable(
        self, value, dtype=None, trainable=False
    ) -> "torch.Tensor":
        tensor = torch.tensor(
            value,
            requires_grad=trainable,
            # TODO (simon): Make GPU-trainable.
            # device=self._device,
            dtype=(
                dtype
                or (
                    torch.float32
                    if isinstance(value, float)
                    else torch.int32
                    if isinstance(value, int)
                    else None
                )
            ),
        )
        return nn.Parameter(tensor) if trainable else tensor

    def _convert_batch_type(
        self,
        batch: MultiAgentBatch,
        to_device: bool = True,
        pin_memory: bool = False,
        use_stream: bool = False,
    ) -> MultiAgentBatch:
        batch = convert_to_torch_tensor(
            batch.policy_batches,
            device=self._device if to_device else None,
            pin_memory=pin_memory,
            use_stream=use_stream,
        )
        # TODO (sven): This computation of `env_steps` is not accurate!
        length = max(len(b) for b in batch.values())
        batch = MultiAgentBatch(batch, env_steps=length)
        return batch

    def _compute_off_policyness(self, batch):
        # Log off-policy'ness of this batch wrt the current weights.
        off_policyness = {
            (mid, DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY): (
                (self._weights_seq_no - module_batch[WEIGHTS_SEQ_NO]).float()
            )
            for mid, module_batch in batch.items()
            if WEIGHTS_SEQ_NO in module_batch
        }
        for key in off_policyness.keys():
            mid = key[0]
            if Columns.LOSS_MASK not in batch[mid]:
                off_policyness[key] = torch.mean(off_policyness[key])
            else:
                mask = batch[mid][Columns.LOSS_MASK]
                num_valid = torch.sum(mask)
                off_policyness[key] = torch.sum(off_policyness[key][mask]) / num_valid
        self.metrics.log_dict(off_policyness, window=1)

    @override(DifferentiableLearner)
    def build(self, device: Optional[DeviceType] = None) -> None:
        """Builds the TorchDifferentiableLearner.

        This method is specific to TorchDifferentiableLearner. Before running super() it will
        initialize the device properly based on `self.config`, so that `_make_module()`
        can place the created module on the correct device. After running super() it
        wraps the module in a TorchDDPRLModule if `config.num_learners > 0`.
        Note, in inherited classes it is advisable to call the parent's `build()`
        after setting up all variables because `configure_optimizer_for_module` is
        called in this `Learner.build()`.
        """
        # TODO (simon): Allow different `DifferentiableLearner` instances in a
        # `MetaLearner` to run on different GPUs.

        super().build(device=device)

        if self._torch_compile_complete_update:
            torch._dynamo.reset()
            self._compiled_update_initialized = False
            self._possibly_compiled_update = torch.compile(
                self._uncompiled_update,
                backend=self._torch_compile_cfg.torch_dynamo_backend,
                mode=self._torch_compile_cfg.torch_dynamo_mode,
                **self._torch_compile_cfg.kwargs,
            )
        # Otherwise, we use the possibly compiled `forward_train` in
        # the module, compiled in the `MetaLearner`.
        else:
            # Nothing, to do.
            self._possibly_compiled_update = self._uncompiled_update

    @override(DifferentiableLearner)
    def _update(
        self, batch: Dict[str, Any], params: Dict[ModuleID, NamedParamDict]
    ) -> Tuple[Any, Dict[ModuleID, NamedParamDict], Any, Any]:
        # The first time we call _update after building the learner or
        # adding/removing models, we update with the uncompiled update method.
        # This makes it so that any variables that may be created during the first
        # update step are already there when compiling. More specifically,
        # this avoids errors that occur around using defaultdicts with
        # torch.compile().
        if (
            self._torch_compile_complete_update
            and not self._compiled_update_initialized
        ):
            self._compiled_update_initialized = True
            return self._uncompiled_update(batch, params)
        else:
            return self._possibly_compiled_update(batch, params)
