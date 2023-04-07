
from typing import Optional
import gymnasium as gym

import torch
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.examples.rlhf.ppo_ft.utils import masked_mean

import transformers


# TODO (Kourosh): Can we achieve the same thing by overriding the catalog, it seems so complicated to do this. Is that true? 

class Critic(torch.nn.Module):
    def __init__(self, model_base: str):
        super().__init__()

        self.base = transformers.AutoModel.from_pretrained(model_base)
        self.trunk = torch.nn.Linear(self.base.config.hidden_size, 1)
    
    # borrowed from colossalai 
    # https://github.com/hpcaitech/ColossalAI/blob/main/applications/Chat/coati/models/
    # base/critic.py
    def forward(
        self,
        sequences: torch.LongTensor,
        action_mask: Optional[torch.Tensor] = None,
        attention_mask: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        outputs = self.base(sequences, attention_mask=attention_mask)
        last_hidden_states = outputs['last_hidden_state']

        values = self.trunk(last_hidden_states).squeeze(-1)

        if action_mask is not None and self.use_action_mask:
            num_actions = action_mask.size(1)
            prompt_mask = attention_mask[:, :-num_actions]
            values = values[:, :-num_actions]
            value = masked_mean(values, prompt_mask, dim=1)
            return value

        values = values[:, :-1]
        value = values.mean(dim=1)
        return value

class RLHFPPOTorchRLModule(PPOTorchRLModule):

    def __init__(self, config: RLModuleConfig):
        super().__init__(config)

    # Override the default to customize
    def build(self):

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
    
    def _fwd(self, batch):
        breakpoint()

    def _forward_exploration(self, batch):
        print("in forward_exploration")
        return self._fwd(batch)
        
    def _forward_inference(self, batch):
        print("in forward_inference")
        return self._fwd(batch)
    
    def _forward_train(self, batch):
        print("in forward_train")
        breakpoint()
        
