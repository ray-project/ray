
from typing import Optional
import gymnasium as gym

import torch
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.examples.rlhf.ppo_ft.utils import masked_mean
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule

import transformers


# TODO (Kourosh): Can we achieve the same thing by overriding the catalog, it seems so complicated to do this. Is that true? 

class Critic(torch.nn.Module):
    def __init__(self, model_base: str):
        super().__init__()

        self.base = transformers.AutoModel.from_pretrained(model_base)
        self.trunk = torch.nn.Linear(self.base.config.hidden_size, 1)
    
    def forward(
        self,
        input_ids: torch.LongTensor,
        attention_mask: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        outputs = self.base(input_ids, attention_mask=attention_mask)
        last_hidden_states = outputs['last_hidden_state']

        # only use the hidden state on the last layer for the the last token as the value
        values = self.trunk(last_hidden_states[:, -1]).squeeze(-1)
        assert values.ndim == 1, "values should be a 1D tensor with batch size"
        return values

class RLHFPPOTorchRLModule(TorchRLModule):

    def __init__(self, config: RLModuleConfig):
        super().__init__(config)

    # Override the default to customize
    def setup(self):

        # TODO (Kourosh): Passing arbitrary custom configs to use in RLModules doesn't 
        # quite work yet. This pretends that it works. 
        model_config = self.config.model_config_dict
        actor_base_model = model_config.get("actor_base_model", "gpt2")
        critic_base_model = model_config.get("critic_base_model", "gpt2")
        self.actor = transformers.AutoModelForCausalLM.from_pretrained(actor_base_model)
        self.critic = Critic(model_base=critic_base_model)

    def input_specs_exploration(self):
        return []
    
    def input_specs_inference(self):
        return []

    def _forward_exploration(self, batch):
        # we skip the default sampler's procedure for inference and exploration 
        pass
        
    def _forward_inference(self, batch):
        # we skip the default sampler's procedure for inference and exploration
        pass
    
    def _forward_train(self, batch):
        output = {}

        vf_out = self.critic(
            input_ids=batch[SampleBatch.ACTIONS]["sequence"],
            attention_mask=batch[SampleBatch.ACTIONS]["attention_mask"],
        )

        output[SampleBatch.VF_PREDS] = vf_out # (batch_size,)

        actor_out = self.actor(
            input_ids=batch[SampleBatch.ACTIONS]["sequence"],
            attention_mask=batch[SampleBatch.ACTIONS]["attention_mask"],
        )
        actor_logits = actor_out.logits # (batch_size, seq_len, vocab_size)
        actor_dist = TorchCategorical(logits=actor_logits)

        output[SampleBatch.ACTION_DIST_INPUTS] = actor_logits
        output[SampleBatch.ACTION_DIST] = actor_dist

        return output



        
        
