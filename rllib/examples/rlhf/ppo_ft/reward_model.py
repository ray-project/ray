from typing import Any, Dict, List, Optional, Tuple, Union

import torch
import torch.nn as nn
from transformers import AutoConfig, Trainer
from transformers.models.auto import MODEL_MAPPING


def get_reward_model(model_dir):
    """Load reward model from model_dir."""

    config = AutoConfig.from_pretrained(model_dir)
    model_cls = MODEL_MAPPING[type(config)]

    class RewardModel(model_cls):
        """
        Reward model base class.

        Args:
            model (nn.Module): Base model.
        """

        def __init__(self, config, *args, **kwargs):
            super().__init__(config, *args, **kwargs)

            # The additional value head.
            self.value_head = nn.Linear(self.config.n_embd, 1)
            
            self.post_init()

        # For training purpose, forward computes the values for both prompts.
        def forward(
            self,
            chosen_input_ids,
            chosen_attention_mask,
            rejected_input_ids,
            rejected_attention_mask,
            **kwargs
        ) -> torch.Tensor:
            chosen_value = self.value(chosen_input_ids, chosen_attention_mask)
            rejected_value = self.value(rejected_input_ids, rejected_attention_mask)
            return torch.stack([chosen_value, rejected_value], dim=1)

        def value(self, input_ids, attention_mask) -> torch.Tensor:
            """Forward function predicts whether chosen response has a higher reward.
            """
            # Force inputs to be torch tensors.
            if not isinstance(input_ids, torch.Tensor):
                input_ids = torch.tensor(input_ids).to(self.device)
            if not isinstance(attention_mask, torch.Tensor):
                attention_mask = torch.tensor(attention_mask).to(self.device)

            last_hidden_state = super().forward(
                input_ids=input_ids,
                attention_mask=attention_mask,
                output_hidden_states=True,
            )['last_hidden_state']

            values = self.value_head(last_hidden_state)
            # Remove the last dimension, since there is only a single value per token.
            value = values.mean(dim=1).squeeze(-1)

            return value

        def generate(self, *kwargs):
            raise NotImplementedError("Reward model does not generate token.")

    # Load model weights into the customized RewardModel.
    # Note that for training, it is normal to see a warning message like:
    #   Some weights of RewardModel were not initialized ...
    # This is because we are bootstraping the RewardModel from the base model
    # checkpoint.
    model = RewardModel.from_pretrained(model_dir)
    
    return model


class RewardModelTrainer(Trainer):
    """Custom transformers Trainer that uses a reward model loss function.
    """
    def compute_loss(self, model, inputs, return_outputs=False):
        chosen_ids = inputs.pop("chosen_input_ids").to(model.device)
        chosen_mask = inputs.pop("chosen_attention_mask").to(model.device)
        rejected_ids = inputs.pop("rejected_input_ids").to(model.device)
        rejected_mask = inputs.pop("rejected_attention_mask").to(model.device)

        # B x 2 x SEQ_LEN
        outputs = model(chosen_ids, chosen_mask, rejected_ids, rejected_mask)
        
        # Squeeze out the last value dimension, since there is only a single
        # reward per token.
        chosen_rewards = outputs[:, 0, :]    # B x SEQ_LEN
        rejected_rewards = outputs[:, 1, :]  # B x SEQ_LEN

        batch_size = chosen_ids.shape[0]

        # Note(jungong): different models may have different padding scheme,
        # following logics work for GPT models.
        # TODO(jungong): update if necessary.
        loss = 0
        for i in range(batch_size):
            # Masked tokens that are different between the chosen and rejected prompt.
            # Losses for these tokens should get back-propagated.
            divergence = (
                chosen_ids[i] * chosen_mask[i] != rejected_ids[i] * rejected_mask[i]
            ).squeeze().nonzero(as_tuple=True)[0]

            if len(divergence) <= 0:
                # Chosen and rejected prompts are identical.
                # Loss will be 0 anyways.
                continue

            start_index = divergence[0].item()
            end_index = divergence[-1].item()

            # Loss is the negative log probability loss between the chosen and rejected prompt.
            selected_chosen_rewards = chosen_rewards[i][start_index:end_index + 1]
            selected_rejected_rewards = rejected_rewards[i][start_index:end_index + 1]

            loss += -torch.log(
                torch.sigmoid(selected_chosen_rewards - selected_rejected_rewards)
            ).mean()

        loss /= batch_size

        return (loss, outputs) if return_outputs else loss

    def prediction_step(
        self,
        model: nn.Module,
        inputs: Dict[str, Union[torch.Tensor, Any]],
        prediction_loss_only: bool,
        ignore_keys: Optional[List[str]] = None,
    ) -> Tuple[Optional[torch.Tensor], Optional[torch.Tensor], Optional[torch.Tensor]]:
        """Override prediction step to return the predicted value.

        Note(jungong) : we have to do this to workaround the default behavior of
        droping the first output for evaluation, since Trainer is meant for
        sequence prediction tasks.
        """
        with torch.no_grad():
            with self.compute_loss_context_manager():
                chosen_mask = inputs.get("chosen_attention_mask").to(model.device)
                rejected_mask = inputs.get("rejected_attention_mask").to(model.device)

                loss, outputs = self.compute_loss(model, inputs, return_outputs=True)
                loss = loss.detach()

                logits = []
                for i in range(chosen_mask.shape[0]):
                    # For evaluation, we simply calculate the mean reward for both prompts.
                    chosen_rewards = outputs[i, 0, :] * chosen_mask[i]
                    rejected_rewards = outputs[i, 1, :] * rejected_mask[i]

                    logits.append([
                        chosen_rewards.sum() / chosen_mask[i].sum(),
                        rejected_rewards.sum() / rejected_mask[i].sum(),
                    ])

                logits = torch.tensor(logits).to(model.device)
                labels = inputs.get("labels").to(model.device)

                return (loss, logits, labels)
