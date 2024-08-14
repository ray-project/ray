import numpy as np
import torch
import torch.nn.functional as F
from optimum.bettertransformer import BetterTransformer
from transformers import AutoModel, AutoTokenizer


class ComputeEmbeddings:
    """Main class used to compute embeddings."""

    def __init__(self, text_column_name, model_name, device, chunk_size):
        self.model_name = model_name
        self.chunk_size = chunk_size
        self.text_column_name = text_column_name
        self.device = device

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)

        self.model = AutoModel.from_pretrained(model_name).to(self.device)
        try:
            self.model = BetterTransformer.transform(self.model)
        except NotImplementedError as e:
            # Not all models are supported for BetterTransformer out of the
            # box. Skip using the optimization in that case.
            if "not yet supported to be used with BetterTransformer" not in str(e):
                raise e
            print(
                "This model is not supported for BetterTransformer, "
                "skipping optimization."
            )

    def _average_pool(self, last_hidden_states, attention_mask):
        last_hidden = last_hidden_states.masked_fill(
            ~attention_mask[..., None].bool(), 0.0
        )
        return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]

    def __call__(self, batch):
        batch_text = batch[self.text_column_name].tolist()
        # Directly return torch tensors.
        tokenize_results = self.tokenizer(
            batch_text,
            max_length=self.chunk_size,
            padding=True,
            truncation=True,
            return_tensors="pt",
        )

        # can use either torch.no_grad() or torch.inference_mode() here, not
        # much difference in this case. This significantly reduces GRAM usage
        # and prevents OOMing during embed model call.
        with torch.no_grad():
            if "token_type_ids" in tokenize_results:
                # Only keep necessary fields.
                model_input = {
                    "input_ids": tokenize_results["input_ids"],
                    "token_type_ids": tokenize_results["token_type_ids"],
                    "attention_mask": tokenize_results["attention_mask"],
                }
            else:
                model_input = tokenize_results
            model_input = {k: v.to(self.device) for k, v in model_input.items()}

            outputs = self.model(**model_input)
            embeddings = self._average_pool(
                outputs.last_hidden_state, model_input["attention_mask"]
            )
            embeddings = F.normalize(embeddings)

            batch["values"] = embeddings.detach().cpu().numpy().astype(np.float32)
            return batch
