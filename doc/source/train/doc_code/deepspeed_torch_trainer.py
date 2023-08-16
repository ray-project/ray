# Minimal Ray Train + DeepSpeed example adapted from
# https://github.com/huggingface/accelerate/blob/main/examples/nlp_example.py

import deepspeed
import evaluate
import torch
from datasets import load_dataset
from deepspeed.accelerator import get_accelerator
from torch.utils.data import DataLoader
from tqdm import tqdm
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    set_seed,
)

from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def get_datasets(tokenizer):
    """Creates and preprocess the GLUE MRPC dataset."""

    datasets = load_dataset("glue", "mrpc")

    def tokenize_function(examples):
        outputs = tokenizer(
            examples["sentence1"],
            examples["sentence2"],
            truncation=True,
            max_length=None,
        )
        return outputs

    tokenized_datasets = datasets.map(
        tokenize_function,
        batched=True,
        remove_columns=["idx", "sentence1", "sentence2"],
    )
    tokenized_datasets = tokenized_datasets.rename_column("label", "labels")

    return tokenized_datasets


def train_func(config):
    # Set global random seed
    set_seed(config["seed"])

    # Load datasets and metrics
    metric = evaluate.load("glue", "mrpc")
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
    tokenized_datasets = get_datasets(tokenizer)

    # Create evaluation dataloader
    # The training dataloader will be created by `deepspeed.initialize` later
    def collate_fn(examples):
        return tokenizer.pad(
            examples,
            padding="longest",
            return_tensors="pt",
        )

    eval_dataloader = DataLoader(
        tokenized_datasets["validation"],
        batch_size=config["eval_batch_size"],
        collate_fn=collate_fn,
        shuffle=False,
    )

    # Instantiate the model
    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-cased", return_dict=True
    )

    # Initialize DeepSpeed Engine
    model, optimizer, train_dataloader, lr_scheduler = deepspeed.initialize(
        model=model,
        model_parameters=model.parameters(),
        training_data=tokenized_datasets["train"],
        collate_fn=collate_fn,
        config=deepspeed_config,
    )

    # Define the GPU device for the current worker
    device = get_accelerator().device_name(model.local_rank)

    # Now we train the model
    for epoch in range(config["num_epochs"]):
        model.train()
        for batch in tqdm(
            train_dataloader,
            desc=f"Training epoch {epoch}",
            disable=(model.global_rank != 0),
        ):
            batch = {k: v.to(device) for k, v in batch.items()}
            outputs = model(**batch)
            loss = outputs.loss
            model.backward(loss)
            optimizer.step()
            lr_scheduler.step()
            optimizer.zero_grad()

        model.eval()
        for batch in tqdm(
            eval_dataloader,
            desc=f"Evaluation epoch {epoch}",
            disable=(model.global_rank != 0),
        ):
            batch = {k: v.to(device) for k, v in batch.items()}
            with torch.no_grad():
                outputs = model(**batch)
            predictions = outputs.logits.argmax(dim=-1)

            metric.add_batch(
                predictions=predictions,
                references=batch["labels"],
            )
        eval_metric = metric.compute()

        if model.global_rank == 0:
            print(f"epoch {epoch}:", eval_metric)


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

    trainer.fit()
