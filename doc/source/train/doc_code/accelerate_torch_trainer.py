# __accelerate_torch_basic_example_start__

# Minimal Ray Train + Accelerate example adapted from
# https://github.com/huggingface/accelerate/blob/main/examples/nlp_example.py

import evaluate
import torch

from accelerate import Accelerator
from datasets import load_dataset
from torch.optim import AdamW
from torch.utils.data import DataLoader
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    get_linear_schedule_with_warmup,
    set_seed,
)
from tempfile import TemporaryDirectory
from tqdm import tqdm

import ray.train
from ray.train import ScalingConfig, Checkpoint
from ray.train.torch import TorchTrainer


def get_datasets(tokenizer):
    """Creates a set of `DataLoader`s for the `glue` dataset."""
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

    return tokenized_datasets.rename_column("label", "labels")


def train_func(config):
    """Your training function that will be launched on each worker. """

    # Unpack training configs
    lr = config["lr"]
    seed = config["seed"]
    num_epochs = config["num_epochs"]
    train_batch_size = config["train_batch_size"]
    eval_batch_size = config["eval_batch_size"]

    # Set global random seed
    set_seed(seed)

    # Initialize accelerator
    accelerator = Accelerator()

    # Load datasets and metrics
    metric = evaluate.load("glue", "mrpc")
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
    tokenized_datasets = get_datasets(tokenizer)

    # Create training and evaluation dataloader
    def collate_fn(examples):
        return tokenizer.pad(
            examples,
            padding="longest",
            return_tensors="pt",
        )

    # Instantiate dataloaders.
    train_dataloader = DataLoader(
        tokenized_datasets["train"],
        shuffle=True,
        collate_fn=collate_fn,
        batch_size=train_batch_size,
        drop_last=True,
    )
    eval_dataloader = DataLoader(
        tokenized_datasets["validation"],
        shuffle=False,
        collate_fn=collate_fn,
        batch_size=eval_batch_size,
        drop_last=True,
    )

    # Instantiate the model
    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-cased", return_dict=True
    )

    # Instantiate optimizer
    optimizer = AdamW(params=model.parameters(), lr=lr)

    # Instantiate scheduler
    lr_scheduler = get_linear_schedule_with_warmup(
        optimizer=optimizer,
        num_warmup_steps=100,
        num_training_steps=(len(train_dataloader) * num_epochs),
    )

    # Prepare everything
    (
        model,
        optimizer,
        train_dataloader,
        eval_dataloader,
        lr_scheduler,
    ) = accelerator.prepare(
        model, optimizer, train_dataloader, eval_dataloader, lr_scheduler
    )

    # Now we train the model
    for epoch in range(num_epochs):
        model.train()
        for batch in tqdm(
            train_dataloader,
            desc=f"epoch {epoch}",
            disable=not accelerator.is_main_process,
        ):
            outputs = model(**batch)
            loss = outputs.loss
            accelerator.backward(loss)
            optimizer.step()
            lr_scheduler.step()
            optimizer.zero_grad()

        model.eval()
        for batch in tqdm(
            eval_dataloader,
            desc=f"epoch {epoch}",
            disable=not accelerator.is_main_process,
        ):
            with torch.no_grad():
                outputs = model(**batch)
            predictions = outputs.logits.argmax(dim=-1)

            # Distributed Evaluation
            predictions, references = accelerator.gather_for_metrics(
                (predictions, batch["labels"])
            )
            metric.add_batch(
                predictions=predictions,
                references=references,
            )

        eval_metric = metric.compute()
        accelerator.print(f"epoch {epoch}:", eval_metric)

        # Report Checkpoint and metrics to Ray Train
        # ==========================================
        with TemporaryDirectory() as tmpdir:
            unwrapped_model = accelerator.unwrap_model(model)
            accelerator.save(unwrapped_model, f"{tmpdir}/ckpt_{epoch}.bin")
            checkpoint = Checkpoint.from_directory(tmpdir)
            ray.train.report(metrics=eval_metric, checkpoint=checkpoint)


if __name__ == "__main__":
    training_config = {
        "lr": 2e-5,
        "num_epochs": 3,
        "seed": 42,
        "train_batch_size": 16,
        "eval_batch_size": 32,
    }

    trainer = TorchTrainer(
        train_func,
        train_loop_config=training_config,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
    )

    result = trainer.fit()

# __accelerate_torch_basic_example_end__
