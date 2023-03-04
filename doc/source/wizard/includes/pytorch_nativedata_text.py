import torch
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, AutoModelForSequenceClassification, AdamW

import ray.train as train
from ray.air import session
from ray.tune.syncer import SyncConfig
from ray.air.config import ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer

# --------------------------------------
# Data Ingestion with native dataloader
# --------------------------------------

with open("train.text", "r") as fin:
    train_texts = [line.strip() for line in fin.readlines()]

with open("train.labels", "r") as fin:
    train_labels = [line.strip() for line in fin.readlines()]

with open("eval.text", "r") as fin:
    eval_texts = [line.strip() for line in fin.readlines()]

with open("eval.labels", "r") as fin:
    eval_labels = [line.strip() for line in fin.readlines()]

# Define dataset
class SentimentDataset(Dataset):
    def __init__(self, texts, labels, tokenizer):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = self.texts[idx]
        label = self.labels[idx]
        encoding = self.tokenizer(
            text,
            truncation=True,
            padding="max_length",
            max_length=128,
            return_tensors="np",
        )
        input_ids = encoding["input_ids"].squeeze()
        attention_mask = encoding["attention_mask"].squeeze()
        return input_ids, attention_mask, label


# ------------------------
# Training Loop
# ------------------------


def train_loop_per_worker(config):
    # Define data loader
    tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")
    train_dataset = SentimentDataset(train_texts, train_labels, tokenizer)
    eval_dataset = SentimentDataset(eval_texts, eval_labels, tokenizer)

    worker_batch_size = config["batch_size"] // session.get_world_size()
    train_loader = DataLoader(train_dataset, batch_size=worker_batch_size, shuffle=True)
    eval_loader = DataLoader(eval_dataset, batch_size=worker_batch_size, shuffle=False)

    train_loader = train.torch.prepare_data_loader(train_loader)
    eval_loader = train.torch.prepare_data_loader(eval_loader)

    # Define model
    model = AutoModelForSequenceClassification.from_pretrained(
        "distilbert-base-uncased", num_labels=2
    )
    model = train.torch.prepare_model(model)

    # Define optimizer and learning rate scheduler
    optimizer = AdamW(model.parameters(), lr=config["lr"])
    scheduler = torch.optim.lr_scheduler.StepLR(
        optimizer, step_size=1, gamma=config["gamma"]
    )

    # Train model
    model.train()
    for epoch in range(config["epochs"]):
        model.train()
        for batch in train_loader:
            input_ids, attention_mask, labels = batch
            optimizer.zero_grad()
            outputs = model(
                input_ids=input_ids, attention_mask=attention_mask, labels=labels
            )
            loss = outputs.loss
            loss.backward()
            optimizer.step()
        scheduler.step()

        # Evaluate model
        model.eval()

        total_correct = 0
        total_samples = 0
        with torch.no_grad():
            for batch in eval_loader:
                input_ids, attention_mask, labels = batch
                outputs = model(input_ids=input_ids, attention_mask=attention_mask)
                logits = outputs.logits
                predictions = torch.argmax(logits, dim=1)
                total_correct += torch.sum(predictions == labels)
                total_samples += len(labels)
        accuracy = float(total_correct) / total_samples

        if session.get_world_rank() == 0:
            print("Accuracy:", accuracy)


# ------------------------
# Setup TorchTrainer
# ------------------------


num_workers = 4

# Scale out model training across 4 workers, each assigned 1 CPU and 1 GPU.
scaling_config = ScalingConfig(
    num_workers=num_workers, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
)

# Save only the latest checkpoint
checkpoint_config = CheckpointConfig(num_to_keep=1)

# Set experiment name and checkpoint configs
run_config = RunConfig(
    name="distillbert-hackathon",
    sync_config=SyncConfig(syncer=None),
    checkpoint_config=checkpoint_config,
)

train_loop_config = {
    "batch_size": 128,  # Batch size for training
    "num_epochs": 10,  # Number of epochs to train for
    "lr": 0.001,  # Learning Rate
    "gamma": 0.1,  # Adam parameter
}

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config=train_loop_config,
    scaling_config=scaling_config,
    run_config=run_config,
)

result = trainer.fit()

# Save checkpoint to a well-known location
result.checkpoint.to_directory("/tmp/pytorch-text.checkpoint")
