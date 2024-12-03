from collections import defaultdict
import logging
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Optional,
    Sequence,
    Tuple,
)

from ray.rllib.algorithms.algorithm_config import (
    AlgorithmConfig,
    TorchCompileWhatToCompile,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import Learner, LR_KEY
from ray.rllib.core.rl_module.multi_rl_module import (
    MultiRLModule,
    MultiRLModuleSpec,
)
from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    RLModuleSpec,
)
from ray.rllib.core.rl_module.torch.torch_rl_module import (
    TorchCompileConfig,
    TorchDDPRLModule,
    TorchRLModule,
)
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY,
    NUM_TRAINABLE_PARAMETERS,
    NUM_NON_TRAINABLE_PARAMETERS,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor, copy_torch_tensors
from ray.rllib.utils.typing import (
    ModuleID,
    Optimizer,
    Param,
    ParamDict,
    ShouldModuleBeUpdatedFn,
    StateDict,
    TensorType,
)

torch, nn = try_import_torch()

if torch:
    from ray.air._internal.torch_utils import get_devices


logger = logging.getLogger(__name__)


class TorchLearner(Learner):

    framework: str = "torch"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Will be set during build.
        self._device = None

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

        # Loss scalers for mixed precision training. Map optimizer names to
        # associated torch GradScaler objects.
        self._grad_scalers = None
        if self.config._torch_grad_scaler_class:
            self._grad_scalers = defaultdict(
                lambda: self.config._torch_grad_scaler_class()
            )
        self._lr_schedulers = {}
        self._lr_scheduler_classes = None
        if self.config._torch_lr_scheduler_classes:
            self._lr_scheduler_classes = self.config._torch_lr_scheduler_classes

    @OverrideToImplementCustomLogic
    @override(Learner)
    def configure_optimizers_for_module(
        self,
        module_id: ModuleID,
        config: "AlgorithmConfig" = None,
    ) -> None:
        module = self._module[module_id]

        # For this default implementation, the learning rate is handled by the
        # attached lr Scheduler (controlled by self.config.lr, which can be a
        # fixed value or a schedule setting).
        params = self.get_parameters(module)
        optimizer = torch.optim.Adam(params)

        # Register the created optimizer (under the default optimizer name).
        self.register_optimizer(
            module_id=module_id,
            optimizer=optimizer,
            params=params,
            lr_or_lr_schedule=config.lr,
        )

    def _uncompiled_update(
        self,
        batch: Dict,
        **kwargs,
    ):
        """Performs a single update given a batch of data."""
        # Activate tensor-mode on our MetricsLogger.
        self.metrics.activate_tensor_mode()

        # TODO (sven): Causes weird cuda error when WandB is used.
        #  Diagnosis thus far:
        #  - All peek values during metrics.reduce are non-tensors.
        #  - However, in impala.py::training_step(), a tensor does arrive after learner
        #    group.update_from_episodes(), so somehow, there is still a race condition
        #    possible (learner, which performs the reduce() and learner thread, which
        #    performs the logging of tensors into metrics logger).
        self._compute_off_policyness(batch)

        fwd_out = self.module.forward_train(batch)
        loss_per_module = self.compute_losses(fwd_out=fwd_out, batch=batch)

        gradients = self.compute_gradients(loss_per_module)
        postprocessed_gradients = self.postprocess_gradients(gradients)
        self.apply_gradients(postprocessed_gradients)

        # Deactivate tensor-mode on our MetricsLogger and collect the (tensor)
        # results.
        return fwd_out, loss_per_module, self.metrics.deactivate_tensor_mode()

    @override(Learner)
    def compute_gradients(
        self, loss_per_module: Dict[ModuleID, TensorType], **kwargs
    ) -> ParamDict:
        for optim in self._optimizer_parameters:
            # `set_to_none=True` is a faster way to zero out the gradients.
            optim.zero_grad(set_to_none=True)

        if self._grad_scalers is not None:
            total_loss = sum(
                self._grad_scalers[mid].scale(loss)
                for mid, loss in loss_per_module.items()
            )
        else:
            total_loss = sum(loss_per_module.values())

        total_loss.backward()
        grads = {pid: p.grad for pid, p in self._params.items()}

        return grads

    @override(Learner)
    def apply_gradients(self, gradients_dict: ParamDict) -> None:
        # Set the gradient of the parameters.
        for pid, grad in gradients_dict.items():
            # If updates should not be skipped turn `nan` and `inf` gradients to zero.
            if (
                not torch.isfinite(grad).all()
                and not self.config.torch_skip_nan_gradients
            ):
                # Warn the user about `nan` gradients.
                logger.warning(f"Gradients {pid} contain `nan/inf` values.")
                # If updates should be skipped, do not step the optimizer and return.
                if not self.config.torch_skip_nan_gradients:
                    logger.warning(
                        "Setting `nan/inf` gradients to zero. If updates with "
                        "`nan/inf` gradients should not be set to zero and instead "
                        "the update be skipped entirely set `torch_skip_nan_gradients` "
                        "to `True`."
                    )
                # If necessary turn `nan` gradients to zero. Note this can corrupt the
                # internal state of the optimizer, if many `nan` gradients occur.
                self._params[pid].grad = torch.nan_to_num(grad)
            # Otherwise, use the gradient as is.
            else:
                self._params[pid].grad = grad

        # For each optimizer call its step function.
        for module_id, optimizer_names in self._module_optimizers.items():
            for optimizer_name in optimizer_names:
                optim = self.get_optimizer(module_id, optimizer_name)
                # If we have learning rate schedulers for a module add them, if
                # necessary.
                if self._lr_scheduler_classes is not None:
                    if (
                        module_id not in self._lr_schedulers
                        or optimizer_name not in self._lr_schedulers[module_id]
                    ):
                        # Set for each module and optimizer a scheduler.
                        self._lr_schedulers[module_id] = {optimizer_name: []}
                        # If the classes are in a dictionary each module might have
                        # a different set of schedulers.
                        if isinstance(self._lr_scheduler_classes, dict):
                            scheduler_classes = self._lr_scheduler_classes[module_id]
                        # Else, each module has the same learning rate schedulers.
                        else:
                            scheduler_classes = self._lr_scheduler_classes
                        # Initialize and add the schedulers.
                        for scheduler_class in scheduler_classes:
                            self._lr_schedulers[module_id][optimizer_name].append(
                                scheduler_class(optim)
                            )

                # Step through the scaler (unscales gradients, if applicable).
                if self._grad_scalers is not None:
                    scaler = self._grad_scalers[module_id]
                    scaler.step(optim)
                    self.metrics.log_value(
                        (module_id, "_torch_grad_scaler_current_scale"),
                        scaler.get_scale(),
                        window=1,  # snapshot in time, no EMA/mean.
                    )
                    # Update the scaler.
                    scaler.update()
                # `step` the optimizer (default), but only if all gradients are finite.
                elif all(
                    param.grad is None or torch.isfinite(param.grad).all()
                    for group in optim.param_groups
                    for param in group["params"]
                ):
                    optim.step()
                # If gradients are not all finite warn the user that the update will be
                # skipped.
                elif not all(
                    torch.isfinite(param.grad).all()
                    for group in optim.param_groups
                    for param in group["params"]
                ):
                    logger.warning(
                        "Skipping this update. If updates with `nan/inf` gradients "
                        "should not be skipped entirely and instead `nan/inf` "
                        "gradients set to `zero` set `torch_skip_nan_gradients` to "
                        "`False`."
                    )

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(Learner)
    def after_gradient_based_update(self, *, timesteps: Dict[str, Any]) -> None:
        """Called after gradient-based updates are completed.

        Should be overridden to implement custom cleanup-, logging-, or non-gradient-
        based Learner/RLModule update logic after(!) gradient-based updates have been
        completed.

        Note, for `framework="torch"` users can register
        `torch.optim.lr_scheduler.LRScheduler` via
        `AlgorithmConfig._torch_lr_scheduler_classes`. These schedulers need to be
        stepped here after gradient updates and reported.

        Args:
            timesteps: Timesteps dict, which must have the key
                `NUM_ENV_STEPS_SAMPLED_LIFETIME`.
                # TODO (sven): Make this a more formal structure with its own type.
        """

        # If we have no `torch.optim.lr_scheduler.LRScheduler` registered call the
        # `super()`'s method to update RLlib's learning rate schedules.
        if not self._lr_schedulers:
            return super().after_gradient_based_update(timesteps=timesteps)

        # Only update this optimizer's lr, if a scheduler has been registered
        # along with it.
        for module_id, optimizer_names in self._module_optimizers.items():
            for optimizer_name in optimizer_names:
                # If learning rate schedulers are provided step them here. Note,
                # stepping them in `TorchLearner.apply_gradients` updates the
                # learning rates during minibatch updates; we want to update
                # between whole batch updates.
                if (
                    module_id in self._lr_schedulers
                    and optimizer_name in self._lr_schedulers[module_id]
                ):
                    for scheduler in self._lr_schedulers[module_id][optimizer_name]:
                        scheduler.step()
                optimizer = self.get_optimizer(module_id, optimizer_name)
                self.metrics.log_value(
                    # Cut out the module ID from the beginning since it's already
                    # part of the key sequence: (ModuleID, "[optim name]_lr").
                    key=(
                        module_id,
                        f"{optimizer_name[len(module_id) + 1:]}_{LR_KEY}",
                    ),
                    value=convert_to_numpy(self._get_optimizer_lr(optimizer)),
                    window=1,
                )

    @override(Learner)
    def _get_optimizer_state(self) -> StateDict:
        return {
            name: copy_torch_tensors(optim.state_dict(), device="cpu")
            for name, optim in self._named_optimizers.items()
        }

    @override(Learner)
    def _set_optimizer_state(self, state: StateDict) -> None:
        for name, state_dict in state.items():
            if name not in self._named_optimizers:
                raise ValueError(
                    f"Optimizer {name} in `state` is not known."
                    f"Known optimizers are {self._named_optimizers.keys()}"
                )
            self._named_optimizers[name].load_state_dict(
                copy_torch_tensors(state_dict, device=self._device)
            )

    @override(Learner)
    def get_param_ref(self, param: Param) -> Hashable:
        return param

    @override(Learner)
    def get_parameters(self, module: RLModule) -> Sequence[Param]:
        return list(module.parameters())

    @override(Learner)
    def _convert_batch_type(self, batch: MultiAgentBatch) -> MultiAgentBatch:
        batch = convert_to_torch_tensor(batch.policy_batches, device=self._device)
        # TODO (sven): This computation of `env_steps` is not accurate!
        length = max(len(b) for b in batch.values())
        batch = MultiAgentBatch(batch, env_steps=length)
        return batch

    @override(Learner)
    def add_module(
        self,
        *,
        module_id: ModuleID,
        # TODO (sven): Rename to `rl_module_spec`.
        module_spec: RLModuleSpec,
        config_overrides: Optional[Dict] = None,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
    ) -> MultiRLModuleSpec:
        # Call super's add_module method.
        marl_spec = super().add_module(
            module_id=module_id,
            module_spec=module_spec,
            config_overrides=config_overrides,
            new_should_module_be_updated=new_should_module_be_updated,
        )

        # we need to ddpify the module that was just added to the pool
        module = self._module[module_id]

        if self._torch_compile_forward_train:
            module.compile(self._torch_compile_cfg)
        elif self._torch_compile_complete_update:
            # When compiling the update, we need to reset and recompile
            # _uncompiled_update every time we add/remove a module anew.
            torch._dynamo.reset()
            self._compiled_update_initialized = False
            self._possibly_compiled_update = torch.compile(
                self._uncompiled_update,
                backend=self._torch_compile_cfg.torch_dynamo_backend,
                mode=self._torch_compile_cfg.torch_dynamo_mode,
                **self._torch_compile_cfg.kwargs,
            )

        if isinstance(module, TorchRLModule):
            self._module[module_id].to(self._device)
            if self.distributed:
                if (
                    self._torch_compile_complete_update
                    or self._torch_compile_forward_train
                ):
                    raise ValueError(
                        "Using torch distributed and torch compile "
                        "together tested for now. Please disable "
                        "torch compile."
                    )
                self._module.add_module(
                    module_id,
                    TorchDDPRLModule(module, **self.config.torch_ddp_kwargs),
                    override=True,
                )

        return marl_spec

    @override(Learner)
    def remove_module(self, module_id: ModuleID, **kwargs) -> MultiRLModuleSpec:
        marl_spec = super().remove_module(module_id, **kwargs)

        if self._torch_compile_complete_update:
            # When compiling the update, we need to reset and recompile
            # _uncompiled_update every time we add/remove a module anew.
            torch._dynamo.reset()
            self._compiled_update_initialized = False
            self._possibly_compiled_update = torch.compile(
                self._uncompiled_update,
                backend=self._torch_compile_cfg.torch_dynamo_backend,
                mode=self._torch_compile_cfg.torch_dynamo_mode,
                **self._torch_compile_cfg.kwargs,
            )

        return marl_spec

    @override(Learner)
    def build(self) -> None:
        """Builds the TorchLearner.

        This method is specific to TorchLearner. Before running super() it will
        initialze the device properly based on the `_use_gpu` and `_distributed`
        flags, so that `_make_module()` can place the created module on the correct
        device. After running super() it will wrap the module in a TorchDDPRLModule
        if `_distributed` is True.
        Note, in inherited classes it is advisable to call the parent's `build()`
        after setting up all variables because `configure_optimizer_for_module` is
        called in this `Learner.build()`.
        """
        # TODO (Kourosh): How do we handle model parallelism?
        # TODO (Kourosh): Instead of using _TorchAccelerator, we should use the public
        #  API in ray.train but allow for session to be None without any errors raised.
        if self._use_gpu:
            # get_devices() returns a list that contains the 0th device if
            # it is called from outside a Ray Train session. It's necessary to give
            # the user the option to run on the gpu of their choice, so we enable that
            # option here through the local gpu id scaling config parameter.
            if self._distributed:
                devices = get_devices()
                assert len(devices) == 1, (
                    "`get_devices()` should only return one cuda device, "
                    f"but {devices} was returned instead."
                )
                self._device = devices[0]
            else:
                assert self._local_gpu_idx < torch.cuda.device_count(), (
                    f"local_gpu_idx {self._local_gpu_idx} is not a valid GPU id or is "
                    " not available."
                )
                # this is an index into the available cuda devices. For example if
                # os.environ["CUDA_VISIBLE_DEVICES"] = "1" then
                # torch.cuda.device_count() = 1 and torch.device(0) will actuall map to
                # the gpu with id 1 on the node.
                self._device = torch.device(self._local_gpu_idx)
        else:
            self._device = torch.device("cpu")

        super().build()

        if self._torch_compile_complete_update:
            torch._dynamo.reset()
            self._compiled_update_initialized = False
            self._possibly_compiled_update = torch.compile(
                self._uncompiled_update,
                backend=self._torch_compile_cfg.torch_dynamo_backend,
                mode=self._torch_compile_cfg.torch_dynamo_mode,
                **self._torch_compile_cfg.kwargs,
            )
        else:
            if self._torch_compile_forward_train:
                if isinstance(self._module, TorchRLModule):
                    self._module.compile(self._torch_compile_cfg)
                elif isinstance(self._module, MultiRLModule):
                    for module in self._module._rl_modules.values():
                        # Compile only TorchRLModules, e.g. we don't want to compile
                        # a RandomRLModule.
                        if isinstance(self._module, TorchRLModule):
                            module.compile(self._torch_compile_cfg)
                else:
                    raise ValueError(
                        "Torch compile is only supported for TorchRLModule and "
                        "MultiRLModule."
                    )

            self._possibly_compiled_update = self._uncompiled_update

        # Log number of non-trainable and trainable parameters of our RLModule.
        num_trainable_params = {
            (mid, NUM_TRAINABLE_PARAMETERS): sum(
                p.numel() for p in rlm.parameters() if p.requires_grad
            )
            for mid, rlm in self.module._rl_modules.items()
            if isinstance(rlm, TorchRLModule)
        }
        num_non_trainable_params = {
            (mid, NUM_NON_TRAINABLE_PARAMETERS): sum(
                p.numel() for p in rlm.parameters() if not p.requires_grad
            )
            for mid, rlm in self.module._rl_modules.items()
            if isinstance(rlm, TorchRLModule)
        }

        self.metrics.log_dict(
            {
                **{
                    (ALL_MODULES, NUM_TRAINABLE_PARAMETERS): sum(
                        num_trainable_params.values()
                    ),
                    (ALL_MODULES, NUM_NON_TRAINABLE_PARAMETERS): sum(
                        num_non_trainable_params.values()
                    ),
                },
                **num_trainable_params,
                **num_non_trainable_params,
            }
        )

        self._make_modules_ddp_if_necessary()

    @override(Learner)
    def _update(self, batch: Dict[str, Any]) -> Tuple[Any, Any, Any]:
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
            return self._uncompiled_update(batch)
        else:
            return self._possibly_compiled_update(batch)

    @OverrideToImplementCustomLogic
    def _make_modules_ddp_if_necessary(self) -> None:
        """Default logic for (maybe) making all Modules within self._module DDP."""

        # If the module is a MultiRLModule and nn.Module we can simply assume
        # all the submodules are registered. Otherwise, we need to loop through
        # each submodule and move it to the correct device.
        # TODO (Kourosh): This can result in missing modules if the user does not
        #  register them in the MultiRLModule. We should find a better way to
        #  handle this.
        if self._distributed:
            # Single agent module: Convert to `TorchDDPRLModule`.
            if isinstance(self._module, TorchRLModule):
                self._module = TorchDDPRLModule(
                    self._module, **self.config.torch_ddp_kwargs
                )
            # Multi agent module: Convert each submodule to `TorchDDPRLModule`.
            else:
                assert isinstance(self._module, MultiRLModule)
                for key in self._module.keys():
                    sub_module = self._module[key]
                    if isinstance(sub_module, TorchRLModule):
                        # Wrap and override the module ID key in self._module.
                        self._module.add_module(
                            key,
                            TorchDDPRLModule(
                                sub_module, **self.config.torch_ddp_kwargs
                            ),
                            override=True,
                        )

    def _is_module_compatible_with_learner(self, module: RLModule) -> bool:
        return isinstance(module, nn.Module)

    @override(Learner)
    def _check_registered_optimizer(
        self,
        optimizer: Optimizer,
        params: Sequence[Param],
    ) -> None:
        super()._check_registered_optimizer(optimizer, params)
        if not isinstance(optimizer, torch.optim.Optimizer):
            raise ValueError(
                f"The optimizer ({optimizer}) is not a torch.optim.Optimizer! "
                "Only use torch.optim.Optimizer subclasses for TorchLearner."
            )
        for param in params:
            if not isinstance(param, torch.Tensor):
                raise ValueError(
                    f"One of the parameters ({param}) in the registered optimizer "
                    "is not a torch.Tensor!"
                )

    @override(Learner)
    def _make_module(self) -> MultiRLModule:
        module = super()._make_module()
        self._map_module_to_device(module)
        return module

    def _map_module_to_device(self, module: MultiRLModule) -> None:
        """Moves the module to the correct device."""
        if isinstance(module, torch.nn.Module):
            module.to(self._device)
        else:
            for key in module.keys():
                if isinstance(module[key], torch.nn.Module):
                    module[key].to(self._device)

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

    @override(Learner)
    def _get_tensor_variable(
        self, value, dtype=None, trainable=False
    ) -> "torch.Tensor":
        tensor = torch.tensor(
            value,
            requires_grad=trainable,
            device=self._device,
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

    @staticmethod
    @override(Learner)
    def _get_optimizer_lr(optimizer: "torch.optim.Optimizer") -> float:
        for g in optimizer.param_groups:
            return g["lr"]

    @staticmethod
    @override(Learner)
    def _set_optimizer_lr(optimizer: "torch.optim.Optimizer", lr: float) -> None:
        for g in optimizer.param_groups:
            g["lr"] = lr

    @staticmethod
    @override(Learner)
    def _get_clip_function() -> Callable:
        from ray.rllib.utils.torch_utils import clip_gradients

        return clip_gradients

    @staticmethod
    @override(Learner)
    def _get_global_norm_function() -> Callable:
        from ray.rllib.utils.torch_utils import compute_global_norm

        return compute_global_norm
