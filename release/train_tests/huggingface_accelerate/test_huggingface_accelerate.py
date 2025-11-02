import tempfile

import torch
import evaluate
from datasets import load_dataset
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    AdamW,
    get_linear_schedule_with_warmup,
)
from accelerate import Accelerator

import ray
import ray.train
from ray.train import Checkpoint, ScalingConfig
from ray.train.torch import TorchTrainer


def train_func():
    # Instantiate the accelerator
    accelerator = Accelerator()

    # Datasets
    dataset = load_dataset("yelp_review_full")
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    def tokenize_function(examples):
        outputs = tokenizer(examples["text"], padding="max_length", truncation=True)
        outputs["labels"] = examples["label"]
        return outputs

    small_train_dataset = (
        dataset["train"].select(range(100)).map(tokenize_function, batched=True)
    )
    small_eval_dataset = (
        dataset["test"].select(range(100)).map(tokenize_function, batched=True)
    )

    # Remove unwanted columns and convert datasets to PyTorch format
    columns_to_remove = [
        "text",
        "label",
    ]  # Remove original columns, keep tokenized ones
    small_train_dataset = small_train_dataset.remove_columns(columns_to_remove)
    small_eval_dataset = small_eval_dataset.remove_columns(columns_to_remove)

    small_train_dataset.set_format("torch")
    small_eval_dataset.set_format("torch")

    # Create data loaders
    train_dataloader = torch.utils.data.DataLoader(
        small_train_dataset, batch_size=16, shuffle=True
    )
    eval_dataloader = torch.utils.data.DataLoader(
        small_eval_dataset, batch_size=16, shuffle=False
    )

    # Model
    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-cased", num_labels=5
    )

    # Optimizer and scheduler
    optimizer = AdamW(model.parameters(), lr=2e-5)

    num_training_steps = len(train_dataloader) * 3  # 3 epochs
    lr_scheduler = get_linear_schedule_with_warmup(
        optimizer=optimizer,
        num_warmup_steps=0,
        num_training_steps=num_training_steps,
    )

    # Prepare everything for distributed training
    (
        model,
        optimizer,
        train_dataloader,
        eval_dataloader,
        lr_scheduler,
    ) = accelerator.prepare(
        model, optimizer, train_dataloader, eval_dataloader, lr_scheduler
    )

    # Evaluation metric
    metric = evaluate.load("accuracy")

    # Start training
    num_epochs = 3

    for epoch in range(num_epochs):
        # Training
        model.train()
        total_loss = 0

        for batch in train_dataloader:
            outputs = model(**batch)
            loss = outputs.loss
            accelerator.backward(loss)

            optimizer.step()
            lr_scheduler.step()
            optimizer.zero_grad()

            total_loss += loss.item()

        # Evaluation
        model.eval()
        for batch in eval_dataloader:
            with torch.no_grad():
                outputs = model(**batch)

            predictions = outputs.logits.argmax(dim=-1)
            predictions, references = accelerator.gather_for_metrics(
                (predictions, batch["labels"])
            )
            metric.add_batch(predictions=predictions, references=references)

        eval_results = metric.compute()
        accelerator.print(f"Epoch {epoch + 1}: {eval_results}")

        # Report metrics and checkpoint to Ray Train
        metrics = {
            "epoch": epoch + 1,
            "train_loss": total_loss / len(train_dataloader),
            "eval_accuracy": eval_results["accuracy"],
        }

        # Create checkpoint
        with tempfile.TemporaryDirectory() as tmpdir:
            if accelerator.is_main_process:
                unwrapped_model = accelerator.unwrap_model(model)
                unwrapped_model.save_pretrained(tmpdir)
                tokenizer.save_pretrained(tmpdir)
                checkpoint = Checkpoint.from_directory(tmpdir)
            else:
                checkpoint = None

            ray.train.report(metrics=metrics, checkpoint=checkpoint)


def test_huggingface_accelerate():
    # Define a Ray TorchTrainer to launch `train_func` on all workers
    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
        # If running in a multi-node cluster, this is where you
        # should configure the run's persistent storage that is accessible
        # across all worker nodes.
        run_config=ray.train.RunConfig(
            storage_path="/mnt/cluster_storage/huggingface_accelerate_run"
        ),
    )
    result: ray.train.Result = trainer.fit()

    # Verify training completed successfully
    assert result.metrics is not None
    assert "eval_accuracy" in result.metrics
    assert result.checkpoint is not None

    # Load the trained model from checkpoint
    with result.checkpoint.as_directory() as checkpoint_dir:
        model = AutoModelForSequenceClassification.from_pretrained(  # noqa: F841
            checkpoint_dir
        )
        tokenizer = AutoTokenizer.from_pretrained(checkpoint_dir)  # noqa: F841


if __name__ == "__main__":
    test_huggingface_accelerate()
