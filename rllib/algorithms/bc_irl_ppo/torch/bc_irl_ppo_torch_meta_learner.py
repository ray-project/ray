import contextlib
from typing import Dict, List, TYPE_CHECKING

from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.learner.torch.torch_meta_learner import TorchMetaLearner
from ray.rllib.core.rl_module.apis import SelfSupervisedLossAPI
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.annotations import override, OverrideToImplementCustomLogic
from ray.rllib.utils.typing import ModuleID, NamedParamDict, ParamDict, TensorType

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

torch, nn = try_import_torch()

REWARD_MODULE = "reward_module"


class BCIRLPPOTorchMetaLearner(TorchMetaLearner):
    @OverrideToImplementCustomLogic
    @override(TorchLearner)
    def configure_optimizers_for_module(
        self,
        module_id: ModuleID,
        config: "AlgorithmConfig" = None,
    ) -> None:

        # Only register an optimizer for the reward module.
        if module_id == REWARD_MODULE:
            # Get the module for the ID.
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

    def compute_losses(self, *, fwd_out, batch, others_loss_per_module=None, **kwargs):
        loss_per_module = {}
        for module_id in fwd_out:
            module_batch = batch[module_id]
            module_fwd_out = fwd_out[module_id]

            module = self.module[module_id].unwrapped()
            if isinstance(module, SelfSupervisedLossAPI):
                loss = module.compute_self_supervised_loss(
                    learner=self,
                    module_id=module_id,
                    config=self.config.get_config_for_module(module_id),
                    batch=module_batch,
                    fwd_out=module_fwd_out,
                    others_loss_per_module=others_loss_per_module,
                )
            elif module_id != REWARD_MODULE:
                loss = self.compute_loss_for_module(
                    module_id=module_id,
                    config=self.config.get_config_for_module(module_id),
                    batch=module_batch,
                    fwd_out=module_fwd_out,
                    others_loss_per_module=others_loss_per_module,
                )
            loss_per_module[module_id] = loss

        return loss_per_module

    def compute_loss_for_module(
        self, *, module_id, config, batch, fwd_out, others_loss_per_module=None
    ):
        """Computes the BC (meta) loss."""
        module = self.module[module_id].unwrapped()

        if Columns.LOSS_MASK in batch:
            num_valid = torch.sum(batch[Columns.LOSS_MASK])

            def possibly_masked_mean(data_):
                return torch.sum(data_[batch[Columns.LOSS_MASK]]) / num_valid

        else:
            possibly_masked_mean = torch.mean

        # Compute the action log-probabilities.
        action_dist_class_train = module.get_train_action_dist_cls()
        curr_action_dist = action_dist_class_train.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )

        log_probs = curr_action_dist.logp(batch[Columns.ACTIONS])

        total_loss = -possibly_masked_mean(log_probs)

        self.metrics.log_value((module_id, POLICY_LOSS_KEY), total_loss, window=1)

        return total_loss

    def _uncompiled_update(
        self,
        batch: Dict,
        params: NamedParamDict,
        others_loss_per_module: List[Dict[ModuleID, TensorType]] = None,
        **kwargs,
    ):

        # TODO (sven): Causes weird cuda error when WandB is used.
        #  Diagnosis thus far:
        #  - All peek values during metrics.reduce are non-tensors.
        #  - However, in impala.py::training_step(), a tensor does arrive after learner
        #    group.update(), so somehow, there is still a race condition
        #    possible (learner, which performs the reduce() and learner thread, which
        #    performs the logging of tensors into metrics logger).
        self._compute_off_policyness(batch)

        # TODO (simon): For this to work, the `RLModule.forward` must run the
        # the `forward_train`. Passing in arguments which makes the `forward` modular
        # could be a workaround.
        # Make a functional forward call to include higher-order gradients in the meta
        # update.
        # TODO (simon): Make the functional call only on the policy.
        fwd_out = self._make_functional_call(params, batch)
        loss_per_module = self.compute_losses(
            fwd_out=fwd_out, batch=batch, others_loss_per_module=others_loss_per_module
        )
        gradients = self.compute_gradients(loss_per_module)

        # Update the reward model only every `reward_update_freq` times.
        if (
            self.config.reward_update_freq
            and self._weights_seq_no % self.config.reward_update_freq == 0
        ):
            with contextlib.ExitStack() as stack:
                if self.config.num_learners > 1:
                    for mod in self.module.values():
                        # Skip non-torch modules, b/c they may not have the `no_sync` API.
                        if isinstance(mod, torch.nn.Module):
                            stack.enter_context(mod.no_sync())
                postprocessed_gradients = self.postprocess_gradients(gradients)
                self.apply_gradients(postprocessed_gradients)

        # Deactivate tensor-mode on our MetricsLogger and collect the (tensor)
        # results.
        return fwd_out, loss_per_module, {}

    # TODO (simon): Maybe add some more noise to the
    @override(TorchLearner)
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

        # If we don't have any loss computations, `sum` returns 0.
        if isinstance(total_loss, int):
            assert total_loss == 0
            return {}

        total_loss.backward(retain_graph=True)
        grads = {pid: p.grad for pid, p in self._params.items()}

        return grads

    @override(TorchMetaLearner)
    def _make_functional_call(
        self, params: Dict[ModuleID, NamedParamDict], batch: MultiAgentBatch
    ) -> Dict[ModuleID, NamedParamDict]:
        """Make a functional forward call to all modules in the `MultiRLModule`."""

        fwd_out = self._module.foreach_module(
            lambda mid, m: torch.func.functional_call(m, params[mid], batch[mid])
            if mid != REWARD_MODULE
            else None,
            return_dict=True,
        )
        del fwd_out[REWARD_MODULE]
        return fwd_out
