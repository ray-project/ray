import gymnasium as gym
from gymnasium.spaces import Discrete, Box
import numpy as np
from typing import Dict, List

from ray.rllib import SampleBatch
from ray.rllib.models import ModelV2
from ray.rllib.models.torch.mingpt import (
    GPTConfig,
    GPT,
)
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import (
    ModelConfigDict,
    TensorType,
)

torch, nn = try_import_torch()


class DTTorchModel(TorchModelV2, nn.Module):
    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
    ):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)

        self.obs_dim = num_outputs

        if isinstance(action_space, Discrete):
            self.action_dim = action_space.n
        elif isinstance(action_space, Box):
            self.action_dim = np.product(action_space.shape)
        else:
            raise NotImplementedError

        # Common model parameters
        self.embed_dim = self.model_config["embed_dim"]
        self.max_seq_len = self.model_config["max_seq_len"]
        self.max_ep_len = self.model_config["max_ep_len"]
        self.block_size = self.model_config["max_seq_len"] * 3

        # Build all the nn modules
        self.transformer = self.build_transformer()
        self.position_encoder = self.build_position_encoder()
        self.action_encoder = self.build_action_encoder()
        self.obs_encoder = self.build_obs_encoder()
        self.return_encoder = self.build_return_encoder()
        self.action_head = self.build_action_head()
        self.obs_head = self.build_obs_head()
        self.return_head = self.build_return_head()

        # Update view requirement
        # NOTE: See DTTorchPolicy.action_distribution_fn for an explanation of
        # why the ViewRequirements are like this
        self.view_requirements = {
            SampleBatch.OBS: ViewRequirement(
                space=obs_space, shift=f"-{self.max_seq_len-1}:0"
            ),
            SampleBatch.ACTIONS: ViewRequirement(
                space=action_space, shift=f"-{self.max_seq_len-1}:-1"
            ),
            SampleBatch.REWARDS: ViewRequirement(shift=-1),
            SampleBatch.T: ViewRequirement(shift=f"-{self.max_seq_len-2}:0"),
            SampleBatch.RETURNS_TO_GO: ViewRequirement(
                shift=f"-{self.max_seq_len-1}:-1"
            ),
        }

    def build_transformer(self):
        # build the model
        gpt_config = GPTConfig(
            block_size=self.block_size,
            n_layer=self.model_config["num_layers"],
            n_head=self.model_config["num_heads"],
            n_embed=self.embed_dim,
            embed_pdrop=self.model_config["embed_pdrop"],
            resid_pdrop=self.model_config["resid_pdrop"],
            attn_pdrop=self.model_config["attn_pdrop"],
        )
        gpt = GPT(gpt_config)
        return gpt

    def build_position_encoder(self):
        return nn.Embedding(self.max_ep_len, self.embed_dim)

    def build_action_encoder(self):
        if isinstance(self.action_space, Discrete):
            return nn.Embedding(self.action_dim, self.embed_dim)
        elif isinstance(self.action_space, Box):
            return nn.Linear(self.action_dim, self.embed_dim)
        else:
            raise NotImplementedError

    def build_obs_encoder(self):
        return nn.Linear(self.obs_dim, self.embed_dim)

    def build_return_encoder(self):
        return nn.Linear(1, self.embed_dim)

    def build_action_head(self):
        return nn.Linear(self.embed_dim, self.action_dim)

    def build_obs_head(self):
        if not self.model_config["use_obs_output"]:
            return None
        return nn.Linear(self.embed_dim, self.obs_dim)

    def build_return_head(self):
        if not self.model_config["use_return_output"]:
            return None
        return nn.Linear(self.embed_dim, 1)

    @override(ModelV2)
    def forward(
        self,
        input_dict: Dict[str, TensorType],
        state: List[TensorType],
        seq_lens: TensorType,
    ) -> (TensorType, List[TensorType]):
        # True No-op forward method.
        # TODO: Support image observation inputs
        return input_dict["obs"], state

    def get_prediction(
        self,
        model_out: TensorType,
        input_dict: SampleBatch,
        return_attentions: bool = False,
    ) -> Dict[str, TensorType]:
        """Computes the output of a forward pass of the decision transformer.

        Args:
            model_out: output observation tensor from the base model, [B, T, obs_dim].
            input_dict: a SampleBatch containing
                RETURNS_TO_GO: [B, T (or T + 1), 1] of returns to go values.
                ACTIONS: [B, T, action_dim] of actions.
                T: [B, T] of timesteps.
                ATTENTION_MASKS: [B, T] of attention masks.
            return_attentions: Whether to return the attention tensors from the
                transformer or not.

        Returns:
            A dictionary with keys and values:
                ACTIONS: [B, T, action_dim] of predicted actions.
                if return_attentions:
                    "attentions": List of attentions tensors from the transformer.
                if model_config["use_obs_output"].
                    OBS: [B, T, obs_dim] of predicted observations.
                if model_config["use_return_output"].
                    RETURNS_to_GO: [B, T, 1] of predicted returns to go.
        """
        B, T, *_ = model_out.shape

        obs_embeds = self.obs_encoder(model_out)
        actions_embeds = self.action_encoder(input_dict[SampleBatch.ACTIONS])
        # Note: rtg might have an extra element at the end for targets
        # During training rtg will have T + 1 for its time dimension to get the
        # rtg regression target. During evaluation/inference rtg will have T for
        # its time dimension as we don't need to call get_targets.
        returns_embeds = self.return_encoder(
            input_dict[SampleBatch.RETURNS_TO_GO][:, :T, :]
        )
        timestep_embeds = self.position_encoder(input_dict[SampleBatch.T])

        obs_embeds = obs_embeds + timestep_embeds
        actions_embeds = actions_embeds + timestep_embeds
        returns_embeds = returns_embeds + timestep_embeds

        # This makes the sequence look like (R_1, s_1, a_1, R_2, s_2, a_2, ...)
        stacked_inputs = torch.stack(
            (returns_embeds, obs_embeds, actions_embeds), dim=2
        ).reshape(B, 3 * T, self.embed_dim)

        attention_masks = input_dict[SampleBatch.ATTENTION_MASKS]
        stacked_attention_masks = torch.stack(
            (attention_masks, attention_masks, attention_masks), dim=2
        ).reshape(B, 3 * T)

        # forward the transformer model
        output_embeds = self.transformer(
            stacked_inputs,
            attention_masks=stacked_attention_masks,
            return_attentions=return_attentions,
        )

        outputs = {}
        if return_attentions:
            output_embeds, attentions = output_embeds
            outputs["attentions"] = attentions

        # compute output heads
        outputs[SampleBatch.ACTIONS] = self.action_head(output_embeds[:, 1::3, :])
        if self.model_config["use_obs_output"]:
            outputs[SampleBatch.OBS] = self.obs_head(output_embeds[:, 0::3, :])
        if self.model_config["use_return_output"]:
            outputs[SampleBatch.RETURNS_TO_GO] = self.return_head(
                output_embeds[:, 2::3, :]
            )

        return outputs

    def get_targets(
        self, model_out: TensorType, input_dict: SampleBatch
    ) -> Dict[str, TensorType]:
        """Compute the target predictions for a given input_dict.

        Args:
            model_out: output observation tensor from the base model, [B, T, obs_dim].
            input_dict: a SampleBatch containing
                RETURNS_TO_GO: [B, T + 1, 1] of returns to go values.
                ACTIONS: [B, T, action_dim] of actions.
                T: [B, T] of timesteps.
                ATTENTION_MASKS: [B, T] of attention masks.

        Returns:
            A dictionary with keys and values:
                ACTIONS: [B, T, action_dim] of target actions.
                if model_config["use_obs_output"]
                    OBS: [B, T, obs_dim] of target observations.
                if model_config["use_return_output"]
                    RETURNS_to_GO: [B, T, 1] of target returns to go.
        """
        targets = {SampleBatch.ACTIONS: input_dict[SampleBatch.ACTIONS].detach()}
        if self.model_config["use_obs_output"]:
            targets[SampleBatch.OBS] = model_out.detach()
        if self.model_config["use_return_output"]:
            targets[SampleBatch.RETURNS_TO_GO] = input_dict[SampleBatch.RETURNS_TO_GO][
                :, 1:, :
            ].detach()

        return targets
