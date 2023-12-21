# __accelerate_torch_basic_example_no_raydata_start__
"""
Minimal Ray Train + Accelerate example adapted from
https://github.com/huggingface/accelerate/blob/main/examples/nlp_example.py

Fine-tune a BERT model with Hugging Face Accelerate and Ray Train
"""

from tempfile import TemporaryDirectory

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

import ray.train
from ray.train import Checkpoint, ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):
    """Your training function that will be launched on each worker."""

    # Unpack training configs
    lr = config["lr"]
    seed = config["seed"]
    num_epochs = config["num_epochs"]
    train_batch_size = config["train_batch_size"]
    eval_batch_size = config["eval_batch_size"]

    set_seed(seed)

    # Initialize accelerator
    accelerator = Accelerator()

    # Load datasets and metrics
    metric = evaluate.load("glue", "mrpc")

    # Prepare PyTorch DataLoaders
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
        outputs = {k: v.to(accelerator.device) for k, v in outputs.items()}
        return outputs

    # Instantiate dataloaders.
    train_dataloader = DataLoader(
        hf_datasets["train"],
        shuffle=True,
        collate_fn=collate_fn,
        batch_size=train_batch_size,
        drop_last=True,
    )
    eval_dataloader = DataLoader(
        hf_datasets["validation"],
        shuffle=False,
        collate_fn=collate_fn,
        batch_size=eval_batch_size,
        drop_last=True,
    )
    # ====================================================

    # Instantiate the model, optimizer, lr_scheduler
    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-cased", return_dict=True
    )

    optimizer = AdamW(params=model.parameters(), lr=lr)

    steps_per_epoch = len(train_dataloader)
    lr_scheduler = get_linear_schedule_with_warmup(
        optimizer=optimizer,
        num_warmup_steps=100,
        num_training_steps=(steps_per_epoch * num_epochs),
    )

    # Prepare everything with accelerator
    (
        model,
        optimizer,
        train_dataloader,
        eval_dataloader,
        lr_scheduler,
    ) = accelerator.prepare(
        model, optimizer, train_dataloader, eval_dataloader, lr_scheduler
    )

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

        # Report Checkpoint and metrics to Ray Train
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

    trainer = TorchTrainer(
        train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
        # If running in a multi-node cluster, this is where you
        # should configure the run's persistent storage that is accessible
        # across all worker nodes.
        # run_config=ray.train.RunConfig(storage_path="s3://..."),
    )

    result = trainer.fit()

# __accelerate_torch_basic_example_no_raydata_end__
