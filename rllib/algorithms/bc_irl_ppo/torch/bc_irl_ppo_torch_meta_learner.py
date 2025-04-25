import contextlib
from typing import Dict, List, TYPE_CHECKING

from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.learner.torch.torch_meta_learner import TorchMetaLearner
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.annotations import override, OverrideToImplementCustomLogic
from ray.rllib.utils.typing import ModuleID, NamedParamDict, TensorType

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
            # Get the reward module.
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

    def compute_loss_for_module(
        self, *, module_id, config, batch, fwd_out, others_loss_per_module=None
    ):
        """Computes the BC (meta) loss."""
        module = self.module[module_id].unwrapped()

        # Possibly apply masking to some sub loss terms and to the total loss term
        # at the end. Masking could be used for RNN-based model (zero padded `batch`)
        # and for PPO's batched value function (and bootstrap value) computations,
        # for which we add an additional (artificial) timestep to each episode to
        # simplify the actual computation.
        if Columns.LOSS_MASK in batch:
            num_valid = torch.sum(batch[Columns.LOSS_MASK])

            def possibly_masked_mean(data_):
                return torch.sum(data_[batch[Columns.LOSS_MASK]]) / num_valid

        else:
            possibly_masked_mean = torch.mean

        # Compute the action log-probabilities.
        # TODO (simon): Implement for the future a MARWIL-similar loss.
        action_dist_class_train = module.get_train_action_dist_cls()
        curr_action_dist = action_dist_class_train.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )

        log_probs = curr_action_dist.logp(batch[Columns.ACTIONS])

        # Total loss is the log-likelihood loss.
        # TODO (simon): Implement also a MSE loss alternative.
        total_loss = -possibly_masked_mean(log_probs)

        # Log the metrics and return.
        self.metrics.log_value((module_id, POLICY_LOSS_KEY), total_loss, window=1)

        return total_loss

    def _uncompiled_update(
        self,
        batch: Dict,
        params: NamedParamDict,
        others_loss_per_module: List[Dict[ModuleID, TensorType]] = None,
        **kwargs,
    ):

        # TODO (simon): For this to work, the `RLModule.forward` must run the
        # the `forward_train`. Passing in arguments which makes the `forward` modular
        # could be a workaround.
        # Make a functional forward call to include higher-order gradients in the meta
        # update.
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

        # Update the policy network(s).
        self._make_functional_policy_update(params)

        # Deactivate tensor-mode on our MetricsLogger and collect the (tensor)
        # results.
        return fwd_out, loss_per_module, {}

    @override(TorchMetaLearner)
    def _make_functional_call(
        self, params: Dict[ModuleID, NamedParamDict], batch: MultiAgentBatch
    ) -> Dict[ModuleID, NamedParamDict]:
        """Make a functional forward call to all modules in the `MultiRLModule`."""

        # Make a functional call to the module. Use only the policy sub-module(s).
        fwd_out = self._module.foreach_module(
            lambda mid, m: torch.func.functional_call(m, params[mid], batch[mid])
            if mid != REWARD_MODULE
            else None,
            return_dict=True,
        )
        # Remove the `None` entry for the reward model.
        fwd_out.pop(REWARD_MODULE)
        return fwd_out

    def _make_functional_policy_update(
        self, params: Dict[ModuleID, NamedParamDict]
    ) -> None:
        """Make a functional update to the policy networks(s) in the `MultiRLModule."""

        # Copy the updated policy weights over from the `DifferentiableLearner`.
        with torch.no_grad():
            for name, param in self.module[DEFAULT_MODULE_ID].named_parameters():
                param.copy_(params[DEFAULT_MODULE_ID][name])
