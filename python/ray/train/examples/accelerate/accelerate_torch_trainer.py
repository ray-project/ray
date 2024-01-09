# __accelerate_torch_basic_example_start__
"""
Minimal Ray Train and Accelerate example adapted from
https://github.com/huggingface/accelerate/blob/main/examples/nlp_example.py

Fine-tune a BERT model with Hugging Face Accelerate and Ray Train and Ray Data
"""

from tempfile import TemporaryDirectory

import evaluate
import torch
from accelerate import Accelerator
from datasets import load_dataset
from torch.optim import AdamW
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    get_linear_schedule_with_warmup,
    set_seed,
)

import ray
import ray.train
from ray.train import Checkpoint, DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):
    """Your training function that launches on each worker."""

    # Unpack training configs
    lr = config["lr"]
    seed = config["seed"]
    num_epochs = config["num_epochs"]
    train_batch_size = config["train_batch_size"]
    eval_batch_size = config["eval_batch_size"]
    train_ds_size = config["train_dataset_size"]

    set_seed(seed)

    # Initialize accelerator
    accelerator = Accelerator()

    # Load datasets and metrics
    metric = evaluate.load("glue", "mrpc")

    # Prepare Ray Data loaders
    # ====================================================
    train_ds = ray.train.get_dataset_shard("train")
    eval_ds = ray.train.get_dataset_shard("validation")

    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    def collate_fn(batch):
        outputs = tokenizer(
            list(batch["sentence1"]),
            list(batch["sentence2"]),
            truncation=True,
            padding="longest",
            return_tensors="pt",
        )
        outputs["labels"] = torch.LongTensor(batch["label"])
        outputs = {k: v.to(accelerator.device) for k, v in outputs.items()}
        return outputs

    train_dataloader = train_ds.iter_torch_batches(
        batch_size=train_batch_size, collate_fn=collate_fn
    )
    eval_dataloader = eval_ds.iter_torch_batches(
        batch_size=eval_batch_size, collate_fn=collate_fn
    )
    # ====================================================

    # Instantiate the model, optimizer, lr_scheduler
    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-cased", return_dict=True
    )

    optimizer = AdamW(params=model.parameters(), lr=lr)

    steps_per_epoch = train_ds_size // (accelerator.num_processes * train_batch_size)
    lr_scheduler = get_linear_schedule_with_warmup(
        optimizer=optimizer,
        num_warmup_steps=100,
        num_training_steps=(steps_per_epoch * num_epochs),
    )

    # Prepare everything with accelerator
    model, optimizer, lr_scheduler = accelerator.prepare(model, optimizer, lr_scheduler)

    for epoch in range(num_epochs):
        # Training
        model.train()
        for batch in train_dataloader:
            outputs = model(**batch)
            loss = outputs.loss
            accelerator.backward(loss)
            optimizer.step()
            lr_scheduler.step()
            optimizer.zero_grad()

        # Evaluation
        model.eval()
        for batch in eval_dataloader:
            with torch.no_grad():
                outputs = model(**batch)
            predictions = outputs.logits.argmax(dim=-1)

            predictions, references = accelerator.gather_for_metrics(
                (predictions, batch["labels"])
            )
            metric.add_batch(
                predictions=predictions,
                references=references,
            )

        eval_metric = metric.compute()
        accelerator.print(f"epoch {epoch}:", eval_metric)

        # Report checkpoint and metrics to Ray Train
        # ==========================================
        with TemporaryDirectory() as tmpdir:
            if accelerator.is_main_process:
                unwrapped_model = accelerator.unwrap_model(model)
                accelerator.save(unwrapped_model, f"{tmpdir}/ckpt_{epoch}.bin")
                checkpoint = Checkpoint.from_directory(tmpdir)
            else:
                checkpoint = None
            ray.train.report(metrics=eval_metric, checkpoint=checkpoint)


if __name__ == "__main__":
    config = {
        "lr": 2e-5,
        "num_epochs": 3,
        "seed": 42,
        "train_batch_size": 16,
        "eval_batch_size": 32,
    }

    # Prepare Ray Datasets
    hf_datasets = load_dataset("glue", "mrpc")
    ray_datasets = {
        "train": ray.data.from_huggingface(hf_datasets["train"]),
        "validation": ray.data.from_huggingface(hf_datasets["validation"]),
    }
    config["train_dataset_size"] = ray_datasets["train"].count()

    trainer = TorchTrainer(
        train_func,
        train_loop_config=config,
        datasets=ray_datasets,
        dataset_config=DataConfig(datasets_to_split=["train", "validation"]),
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
        # If running in a multi-node cluster, this is where you
        # should configure the run's persistent storage that is accessible
        # across all worker nodes.
        # run_config=ray.train.RunConfig(storage_path="s3://..."),
    )

    result = trainer.fit()

# __accelerate_torch_basic_example_end__
