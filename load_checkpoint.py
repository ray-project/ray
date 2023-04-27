
import ray
import torch
import pytorch_lightning as pl
import torch.nn.functional as F
from torch.utils.data import DataLoader, random_split
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from datasets import load_dataset, load_metric
import numpy as np
import transformers

class SentimentModel(pl.LightningModule):
    def __init__(self, lr=2e-5, eps=1e-8):
        super().__init__()
        self.lr = lr
        self.eps = eps
        self.num_classes = 2
        transformers.utils.logging.set_verbosity_error()
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "bert-base-cased", num_labels=self.num_classes
        )
        self.metric = load_metric("glue", "cola")
        self.predictions = []
        self.references = []

    def forward(self, batch):
        input_ids, attention_mask = batch["input_ids"], batch["attention_mask"]
        outputs = self.model(input_ids, attention_mask=attention_mask)
        logits = outputs.logits
        return logits

    def training_step(self, batch, batch_idx):
        labels = batch["label"]
        logits = self.forward(batch)
        loss = F.cross_entropy(logits.view(-1, self.num_classes), labels)
        self.log("train_loss", loss)
        return loss

    def validation_step(self, batch, batch_idx):
        labels = batch["label"]
        logits = self.forward(batch)
        preds = torch.argmax(logits, dim=1)
        self.predictions.append(preds)
        self.references.append(labels)

    def on_validation_epoch_end(self):
        predictions = torch.concat(self.predictions).view(-1)
        references = torch.concat(self.references).view(-1)
        matthews_correlation = self.metric.compute(
            predictions=predictions, references=references
        )

        # self.metric.compute() returns a dictionary:
        # e.g. {"matthews_correlation": 0.53}
        self.log_dict(matthews_correlation, sync_dist=True)
        self.predictions.clear()
        self.references.clear()

    def configure_optimizers(self):
        return torch.optim.AdamW(self.trainer.model.parameters(), lr=self.lr, eps=self.eps)

model = SentimentModel.load_from_checkpoint("/home/ray/ray_results/ptl-sent-classification/LightningTrainer_745c1_00000_0_2023-04-26_16-02-44/checkpoint_000001/model")