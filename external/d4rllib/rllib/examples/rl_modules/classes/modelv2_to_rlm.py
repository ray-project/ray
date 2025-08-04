import pathlib
from typing import Any, Dict, Optional

import tree
from ray.rllib.core import Columns, DEFAULT_POLICY_ID
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDiagGaussian,
    TorchMultiCategorical,
    TorchMultiDistribution,
    TorchSquashedGaussian,
)
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical as OldTorchCategorical,
    TorchDiagGaussian as OldTorchDiagGaussian,
    TorchMultiActionDistribution as OldTorchMultiActionDistribution,
    TorchMultiCategorical as OldTorchMultiCategorical,
    TorchSquashedGaussian as OldTorchSquashedGaussian,
)
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class ModelV2ToRLModule(TorchRLModule, ValueFunctionAPI):
    """An RLModule containing a (old stack) ModelV2.

    The `ModelV2` may be define either through
    - an existing Policy checkpoint
    - an existing Algorithm checkpoint (and a policy ID or "default_policy")
    - or through an AlgorithmConfig object

    The ModelV2 is created in the `setup` and contines to live through the lifetime
    of the RLModule.
    """

    @override(TorchRLModule)
    def setup(self):
        # Try extracting the policy ID from this RLModule's config dict.
        policy_id = self.model_config.get("policy_id", DEFAULT_POLICY_ID)

        # Try getting the algorithm checkpoint from the `model_config`.
        algo_checkpoint_dir = self.model_config.get("algo_checkpoint_dir")
        if algo_checkpoint_dir:
            algo_checkpoint_dir = pathlib.Path(algo_checkpoint_dir)
            if not algo_checkpoint_dir.is_dir():
                raise ValueError(
                    "The `model_config` of your RLModule must contain a "
                    "`algo_checkpoint_dir` key pointing to the algo checkpoint "
                    "directory! You can find this dir inside the results dir of your "
                    "experiment. You can then add this path "
                    "through `config.rl_module(model_config={"
                    "'algo_checkpoint_dir': [your algo checkpoint dir]})`."
                )
            policy_checkpoint_dir = algo_checkpoint_dir / "policies" / policy_id
        # Try getting the policy checkpoint from the `model_config`.
        else:
            policy_checkpoint_dir = self.model_config.get("policy_checkpoint_dir")

        # Create the ModelV2 from the Policy.
        if policy_checkpoint_dir:
            policy_checkpoint_dir = pathlib.Path(policy_checkpoint_dir)
            if not policy_checkpoint_dir.is_dir():
                raise ValueError(
                    "The `model_config` of your RLModule must contain a "
                    "`policy_checkpoint_dir` key pointing to the policy checkpoint "
                    "directory! You can find this dir under the Algorithm's checkpoint "
                    "dir in subdirectory: [algo checkpoint dir]/policies/[policy ID "
                    "ex. `default_policy`]. You can then add this path through `config"
                    ".rl_module(model_config={'policy_checkpoint_dir': "
                    "[your policy checkpoint dir]})`."
                )
            # Create a temporary policy object.
            policy = TorchPolicyV2.from_checkpoint(policy_checkpoint_dir)
        # Create the ModelV2 from scratch using the config.
        else:
            config = self.model_config.get("old_api_stack_algo_config")
            if not config:
                raise ValueError(
                    "The `model_config` of your RLModule must contain a "
                    "`algo_config` key with a AlgorithmConfig object in it that "
                    "contains all the settings that would be necessary to construct a "
                    "old API stack Algorithm/Policy/ModelV2! You can add this setting "
                    "through `config.rl_module(model_config={'algo_config': "
                    "[your old config]})`."
                )
            # Get the multi-agent policies dict.
            policy_dict, _ = config.get_multi_agent_setup(
                spaces={
                    policy_id: (self.observation_space, self.action_space),
                },
                default_policy_class=config.algo_class.get_default_policy_class(config),
            )
            config = config.to_dict()
            config["__policy_id"] = policy_id
            policy = policy_dict[policy_id].policy_class(
                self.observation_space,
                self.action_space,
                config,
            )

        self._model_v2 = policy.model

        # Translate the action dist classes from the old API stack to the new.
        self.action_dist_class = self._translate_dist_class(policy.dist_class)

        # Erase the torch policy from memory, so it can be garbage collected.
        del policy

    @override(TorchRLModule)
    def _forward_inference(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return self._forward_pass(batch, inference=True)

    @override(TorchRLModule)
    def _forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return self._forward_inference(batch, **kwargs)

    @override(TorchRLModule)
    def _forward_train(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        out = self._forward_pass(batch, inference=False)
        out[Columns.ACTION_LOGP] = self.get_train_action_dist_cls()(
            out[Columns.ACTION_DIST_INPUTS]
        ).logp(batch[Columns.ACTIONS])
        out[Columns.VF_PREDS] = self._model_v2.value_function()
        if Columns.STATE_IN in batch and Columns.SEQ_LENS in batch:
            out[Columns.VF_PREDS] = torch.reshape(
                out[Columns.VF_PREDS], [len(batch[Columns.SEQ_LENS]), -1]
            )
        return out

    def _forward_pass(self, batch, inference=True):
        # Translate states and seq_lens into old API stack formats.
        batch = batch.copy()
        state_in = batch.pop(Columns.STATE_IN, {})
        state_in = [s for i, s in sorted(state_in.items())]
        seq_lens = batch.pop(Columns.SEQ_LENS, None)

        if state_in:
            if inference and seq_lens is None:
                seq_lens = torch.tensor(
                    [1.0] * state_in[0].shape[0], device=state_in[0].device
                )
            elif not inference:
                assert seq_lens is not None
            # Perform the actual ModelV2 forward pass.
            # A recurrent ModelV2 adds and removes the time-rank itself (whereas in the
            # new API stack, the connector pipelines are responsible for doing this) ->
            # We have to remove, then re-add the time rank here to make ModelV2 work.
            batch = tree.map_structure(
                lambda s: torch.reshape(s, [-1] + list(s.shape[2:])), batch
            )
        nn_output, state_out = self._model_v2(batch, state_in, seq_lens)
        # Put back 1ts time rank into nn-output (inference).
        if state_in:
            if inference:
                nn_output = tree.map_structure(
                    lambda s: torch.unsqueeze(s, axis=1), nn_output
                )
            else:
                nn_output = tree.map_structure(
                    lambda s: torch.reshape(s, [len(seq_lens), -1] + list(s.shape[1:])),
                    nn_output,
                )
        # Interpret the NN output as action logits.
        output = {Columns.ACTION_DIST_INPUTS: nn_output}
        # Add the `state_out` to the `output`, new API stack style.
        if state_out:
            output[Columns.STATE_OUT] = {}
        for i, o in enumerate(state_out):
            output[Columns.STATE_OUT][i] = o

        return output

    @override(ValueFunctionAPI)
    def compute_values(self, batch: Dict[str, Any], embeddings: Optional[Any] = None):
        self._forward_pass(batch, inference=False)
        v_preds = self._model_v2.value_function()
        if Columns.STATE_IN in batch and Columns.SEQ_LENS in batch:
            v_preds = torch.reshape(v_preds, [len(batch[Columns.SEQ_LENS]), -1])
        return v_preds

    @override(TorchRLModule)
    def get_initial_state(self):
        """Converts the initial state list of ModelV2 into a dict (new API stack)."""
        init_state_list = self._model_v2.get_initial_state()
        return dict(enumerate(init_state_list))

    def _translate_dist_class(self, old_dist_class):
        map_ = {
            OldTorchCategorical: TorchCategorical,
            OldTorchDiagGaussian: TorchDiagGaussian,
            OldTorchMultiActionDistribution: TorchMultiDistribution,
            OldTorchMultiCategorical: TorchMultiCategorical,
            OldTorchSquashedGaussian: TorchSquashedGaussian,
        }
        if old_dist_class not in map_:
            raise ValueError(
                f"ModelV2ToRLModule does NOT support {old_dist_class} action "
                f"distributions yet!"
            )

        return map_[old_dist_class]
