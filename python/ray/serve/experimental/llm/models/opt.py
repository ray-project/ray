import torch

from typing import List, Tuple

from ray.serve.experimental.llm.models.casual_lm import CausalLM


class OPT(CausalLM):
    def forward(
        self, input_ids, attention_mask, position_ids, past_key_values=None
    ) -> Tuple[torch.Tensor, List[Tuple[torch.Tensor, torch.Tensor]]]:
        """Overwrite forward to ignore position_ids"""

        # Model Forward
        outputs = self.model.forward(
            input_ids=input_ids,
            attention_mask=attention_mask,
            past_key_values=past_key_values,
            use_cache=True,
        )
        return outputs.logits, outputs.past_key_values
