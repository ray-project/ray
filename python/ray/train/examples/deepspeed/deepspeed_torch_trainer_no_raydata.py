# __deepspeed_torch_basic_example_no_raydata_start__
"""
Minimal Ray Train + DeepSpeed example adapted from
https://github.com/huggingface/accelerate/blob/main/examples/nlp_example.py

Fine-tune a BERT model with DeepSpeed ZeRO-3 and Ray Train
"""

import os
import deepspeed
import torch

from datasets import load_dataset
from deepspeed.accelerator import get_accelerator
from tempfile import TemporaryDirectory
from torch.utils.data import DataLoader
from torchmetrics.classification import BinaryAccuracy, BinaryF1Score
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    set_seed,
)

import ray
import ray.train

# TODO(ml-team): Change this to ray.train.Checkpoint
from ray.train._checkpoint import Checkpoint
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

# TODO(ml-team): Remove this:
os.environ["RAY_AIR_NEW_PERSISTENCE_MODE"] = "1"
ray.init(runtime_env={"env_vars": {"RAY_AIR_NEW_PERSISTENCE_MODE": "1"}})


def train_func(config):
    """Your training function that will be launched on each worker."""

    # Unpack training configs
    set_seed(config["seed"])
    num_epochs = config["num_epochs"]
    eval_batch_size = config["eval_batch_size"]

    # Instantiate the Model
    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-cased", return_dict=True
    )

    # Prepare PyTorch Data Loaders
    # ====================================================
    hf_datasets = load_dataset("glue", "mrpc")
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    def collate_fn(batch):
        outputs = tokenizer(
            [sample["sentence1"] for sample in batch],
            [sample["sentence2"] for sample in batch],
            truncation=True,
            padding="longest",
            return_tensors="pt",
        )
        outputs["labels"] = torch.LongTensor([sample["label"] for sample in batch])
        return outputs

    # Instantiate dataloaders.
    # The train_dataloader already created by `deepspeed.initialize`
    eval_dataloader = DataLoader(
        hf_datasets["validation"],
        shuffle=False,
        collate_fn=collate_fn,
        batch_size=eval_batch_size,
        drop_last=True,
    )
    # ====================================================

    # Initialize DeepSpeed Engine
    model, optimizer, train_dataloader, lr_scheduler = deepspeed.initialize(
        model=model,
        model_parameters=model.parameters(),
        training_data=hf_datasets["train"],
        collate_fn=collate_fn,
        config=deepspeed_config,
    )
    device = get_accelerator().device_name(model.local_rank)

    # Initialize Evaluation Metrics
    f1 = BinaryF1Score().to(device)
    accuracy = BinaryAccuracy().to(device)

    for epoch in range(num_epochs):
        # Training
        model.train()
        for batch in train_dataloader:
            batch = {k: v.to(device) for k, v in batch.items()}
            outputs = model(**batch)
            loss = outputs.loss
            model.backward(loss)
            optimizer.step()
            lr_scheduler.step()
            optimizer.zero_grad()

        # Evaluation
        model.eval()
        for batch in eval_dataloader:
            batch = {k: v.to(device) for k, v in batch.items()}
            with torch.no_grad():
                outputs = model(**batch)
            predictions = outputs.logits.argmax(dim=-1)

            f1.update(predictions, batch["labels"])
            accuracy.update(predictions, batch["labels"])

        # torchmetrics will sync the results across all workers
        eval_metric = {
            "f1": f1.compute().item(),
            "accuracy": accuracy.compute().item(),
        }
        f1.reset()
        accuracy.reset()

        if model.global_rank == 0:
            print(f"epoch {epoch}:", eval_metric)

        # Report checkpoint and metrics to Ray Train
        # ==============================================================
        with TemporaryDirectory() as tmpdir:
            model.save_checkpoint(tmpdir)

            # Ensure all workers finished saving their checkpoint shard
            torch.distributed.barrier()

            # Report all shards on a node from local rank 0
            if model.local_rank == 0:
                checkpoint = Checkpoint.from_directory(tmpdir)
            else:
                checkpoint = None
            ray.train.report(metrics=eval_metric, checkpoint=checkpoint)
        # ==============================================================


if __name__ == "__main__":
    deepspeed_config = {
        "optimizer": {
            "type": "AdamW",
            "params": {
                "lr": 2e-5,
            },
        },
        "scheduler": {"type": "WarmupLR", "params": {"warmup_num_steps": 100}},
        "fp16": {"enabled": True},
        "bf16": {"enabled": False},  # Turn this on if using AMPERE GPUs.
        "zero_optimization": {
            "stage": 3,
            "offload_optimizer": {
                "device": "none",
            },
            "offload_param": {
                "device": "none",
            },
        },
        "gradient_accumulation_steps": 1,
        "gradient_clipping": True,
        "steps_per_print": 10,
        "train_micro_batch_size_per_gpu": 16,
        "wall_clock_breakdown": False,
    }

    training_config = {
        "seed": 42,
        "num_epochs": 3,
        "eval_batch_size": 32,
        "deepspeed_config": deepspeed_config,
    }

    trainer = TorchTrainer(
        train_func,
        train_loop_config=training_config,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
    )

    result = trainer.fit()

    # Retrieve the best checkponints from results
    result.best_checkpoints

# __deepspeed_torch_basic_example_no_raydata_end__
