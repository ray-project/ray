# __deepspeed_torch_basic_example_start__
"""
Minimal Ray Train + DeepSpeed example adapted from
https://github.com/huggingface/accelerate/blob/main/examples/nlp_example.py

Fine-tune a BERT model with DeepSpeed ZeRO-3 and Ray Train and Ray Data
"""

import os
import deepspeed
import torch

from datasets import load_dataset
from deepspeed.accelerator import get_accelerator
from tempfile import TemporaryDirectory
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
from ray.train import DataConfig, ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer

# TODO(ml-team): Remove this:
os.environ["RAY_AIR_NEW_PERSISTENCE_MODE"] = "1"
ray.init(runtime_env={"env_vars": {"RAY_AIR_NEW_PERSISTENCE_MODE": "1"}})


def train_func(config):
    """Your training function that will be launched on each worker."""

    # Set global random seed
    set_seed(config["seed"])
    train_batch_size = config["train_batch_size"]
    eval_batch_size = config["eval_batch_size"]

    # Instantiate the model
    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-cased", return_dict=True
    )

    # Initialize DeepSpeed Engine
    model, optimizer, _, lr_scheduler = deepspeed.initialize(
        model=model,
        model_parameters=model.parameters(),
        config=deepspeed_config,
    )

    # Fetch Ray Dataset shards
    train_ds = ray.train.get_dataset_shard("train")
    eval_ds = ray.train.get_dataset_shard("validation")

    # Define the GPU device for the current worker
    device = get_accelerator().device_name(model.local_rank)
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    # Use torchmetrics for distributed evaluation
    f1 = BinaryF1Score().to(device)
    accuracy = BinaryAccuracy().to(device)

    def collate_fn(batch):
        outputs = tokenizer(
            list(batch["sentence1"]),
            list(batch["sentence2"]),
            truncation=True,
            padding="longest",
            return_tensors="pt",
        )
        outputs["labels"] = torch.LongTensor(batch["label"])
        outputs = {k: v.to(device) for k, v in outputs.items()}
        return outputs

    # Now we train the model
    for epoch in range(config["num_epochs"]):
        model.train()
        for batch in train_ds.iter_torch_batches(
            batch_size=train_batch_size, collate_fn=collate_fn
        ):
            outputs = model(**batch)
            loss = outputs.loss
            model.backward(loss)
            optimizer.step()
            lr_scheduler.step()
            optimizer.zero_grad()

        model.eval()
        for batch in eval_ds.iter_torch_batches(
            batch_size=eval_batch_size, collate_fn=collate_fn
        ):
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

        # Report Checkpoint and metrics to Ray Train
        # ==========================================
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
        "train_batch_size": 16,
        "gradient_clipping": True,
        "steps_per_print": 10,
        "wall_clock_breakdown": False,
    }

    training_config = {
        "seed": 42,
        "num_epochs": 3,
        "train_batch_size": 16,
        "eval_batch_size": 32,
        "deepspeed_config": deepspeed_config,
    }

    # Prepare Ray Datasets
    hf_datasets = load_dataset("glue", "mrpc")
    ray_datasets = {
        key: ray.data.from_huggingface(hf_ds) for key, hf_ds in hf_datasets.items()
    }

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=training_config,
        datasets=ray_datasets,
        dataset_config=DataConfig(datasets_to_split=["train", "validation"]),
        run_config=RunConfig(
            name="ray-deepspeed-demo",
            checkpoint_config=CheckpointConfig(
                num_to_keep=2,
                checkpoint_score_attribute="f1",
                checkpoint_score_order="max",
            ),
        ),
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
    )

    result = trainer.fit()

    # Retrieve the best checkponints from results
    result.best_checkpoints

# __deepspeed_torch_basic_example_end__
