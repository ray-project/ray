import torch
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, AutoModelForSequenceClassification, AdamW

import ray
import ray.train as train
from ray.air import session
from ray.tune.syncer import SyncConfig
from ray.air.config import ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer

# ----------------------------
# Data Ingestion with Ray Data
# ----------------------------

def get_ray_datasets():
    train_text_ds = ray.data.read_text("train.text")
    eval_text_ds = ray.data.read_text("test.text")
    train_label_ds = ray.data.read_text("train.label")
    eval_label_ds = ray.data.read_text("test.label")

    train_dataset = train_text_ds.zip(train_label_ds)
    eval_dataset = eval_text_ds.zip(eval_label_ds)

    label_to_idx_map = {"positive": 0, "negative": 1}

    tokenizer = AutoTokenizer.from_pretrained('distilbert-base-uncased')

    # Transform input text into token ids


    def build_input_tensors(batch):
        batch["input_ids"] = []
        batch["attention_mask"] = []
        batch["labels"] = [label_to_idx_map[label] for label in batch["text_1"]]

        for text in batch["text"]:
            encoding = tokenizer(
                text, truncation=True, padding='max_length', max_length=128, return_tensors='pt')
            batch["input_ids"].append(encoding['input_ids'].squeeze())
            batch["attention_mask"] = encoding['attention_mask'].squeeze()
        return batch


    train_dataset = train_dataset.map_batches(build_input_tensors)
    eval_dataset = eval_dataset.map_batches(build_input_tensors)
    return {"train": train_dataset, "eval": eval_dataset}

# ------------------------
# Training Loop
# ------------------------

def train_loop_per_worker(config):
    # Define data loader
    train_dataset = session.get_dataset_shard("train")
    eval_dataset = session.get_dataset_shard("eval")

    worker_batch_size = config["batch_size"] // session.get_world_size()

    # Define model
    model = AutoModelForSequenceClassification.from_pretrained(
        'distilbert-base-uncased', num_labels=2)
    model = train.torch.prepare_model(model)

    # Define optimizer and learning rate scheduler
    optimizer = AdamW(model.parameters(), lr=config["lr"])
    scheduler = torch.optim.lr_scheduler.StepLR(
        optimizer, step_size=1, gamma=config["gamma"])

    device = train.torch.get_device()

    # Train model
    model.train()
    for epoch in range(config["epochs"]):
        model.train()
        train_loader = train_dataset.iter_torch_batches(
            batch_size=worker_batch_size, device=device)
        eval_loader = eval_dataset.iter_torch_batches(
            batch_size=worker_batch_size, device=device)

        for batch in train_loader:
            labels = batch["labels"]
            input_ids = batch["input_ids"]
            attention_mask = batch["attention_mask"]

            optimizer.zero_grad()
            outputs = model(input_ids=input_ids,
                            attention_mask=attention_mask, labels=labels)
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
                labels = batch["labels"]
                input_ids = batch["input_ids"]
                attention_mask = batch["attention_mask"]

                outputs = model(input_ids=input_ids,
                                attention_mask=attention_mask)
                logits = outputs.logits
                predictions = torch.argmax(logits, dim=1)
                total_correct += torch.sum(predictions == labels)
                total_samples += len(labels)
        accuracy = float(total_correct) / total_samples

        if session.get_world_rank() == 0:
            print('Accuracy:', accuracy)


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
    "num_epochs": 10,   # Number of epochs to train for
    "lr": 0.001,        # Learning Rate
    "gamma": 0.1,       # Adam parameter
}

ray_datasets = get_ray_datasets()

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config=train_loop_config,
    scaling_config=scaling_config,
    run_config=run_config,
    datasets=ray_datasets
)

result = trainer.fit()
