import numpy as np
import pandas as pd
import os
import socket
import ray
import torch
import torch.distributed as dist
from datasets import load_dataset
import ray.data
from ray.train import torch as ray_torch
from transformers import (
    Trainer,
    TrainingArguments,
    AutoModelForCausalLM,
    AutoTokenizer,
    default_data_collator,
)
from transformers.utils.logging import disable_progress_bar, enable_progress_bar
from ray.train.huggingface.transformers._transformers_utils import (
    prepare_trainer,
    RayTrainReportCallback,
)
from ray.train.torch import TorchTrainer
from ray.air.config import RunConfig, ScalingConfig
import ray.train as train

# Toggle for CPU or GPU
use_gpu = False

# Example cluster config
num_workers = 10
cpus_per_worker = 12
block_size = 512
model_name = "openai-community/gpt2-large"


def main():
    print("Initializing Ray...")
    ray.init(
        # Updated versions here:
        runtime_env={
            "pip": [
                "datasets",
                "evaluate",
                "accelerate==1.5.2",
                "transformers==4.29.2",
                "torch>=1.12.0",
                "deepspeed==0.16.4",
            ],
        },
    )

    print("Ray initialized, downloading model on each node...")
    _ = run_on_every_node(download_model)

    print("Loading tiny_shakespeare dataset...")
    current_dataset = load_dataset("tiny_shakespeare")

    ray_datasets = {
        "train": ray.data.from_huggingface(current_dataset["train"]),
        "validation": ray.data.from_huggingface(current_dataset["validation"]),
    }

    print("Processing datasets...")
    processed_datasets = {
        key: (
            ds.map_batches(split_text, batch_format="pandas").map_batches(
                tokenize, batch_format="pandas"
            )
        )
        for key, ds in ray_datasets.items()
    }

    # Per-device micro-batch size
    batch_size = 16
    # Example: single grad_accum step
    epochs = 2

    # Steps per epoch
    train_ds_size = processed_datasets["train"].count()
    steps_per_epoch = train_ds_size // (batch_size * num_workers)

    # Azure Blob mount where the tuning results will be published
    storage_path = "/results"

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={
            "epochs": epochs,
            "batch_size": batch_size,
            "steps_per_epoch": steps_per_epoch,
            "use_gpu": use_gpu,
        },
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            use_gpu=use_gpu,
            resources_per_worker={"CPU": cpus_per_worker},
        ),
        datasets=processed_datasets,
        run_config=RunConfig(storage_path=storage_path),
    )

    print("Running trainer.fit()...")
    results = trainer.fit()


def split_text(batch: pd.DataFrame) -> pd.DataFrame:
    text = list(batch["text"])
    flat_text = "".join(text)
    lines = [
        x.strip()
        for x in flat_text.split("\n")
        if x.strip() and not x.strip()[-1] == ":"
    ]
    return pd.DataFrame(lines, columns=["text"])


def tokenize(batch: pd.DataFrame) -> dict:
    tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=False)
    tokenizer.pad_token = tokenizer.eos_token
    ret = tokenizer(
        list(batch["text"]),
        truncation=True,
        max_length=block_size,
        padding="max_length",
        return_tensors="np",
    )
    ret["labels"] = ret["input_ids"].copy()
    return dict(ret)


def force_on_node(node_id, func):
    """Helper to pin a remote func to a specific node."""
    scheduling_strategy = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=node_id,
        soft=False,
    )
    return ray.remote(func).options(scheduling_strategy=scheduling_strategy)


def run_on_every_node(remote_func_or_actor_class, **remote_kwargs):
    """Runs a remote function on every node. Skips GPU node if use_gpu=False."""
    refs = []
    for node in ray.nodes():
        if node["Alive"] and (not use_gpu or node["Resources"].get("GPU", 0) > 0):
            remote_on_node = force_on_node(node["NodeID"], remote_func_or_actor_class)
            refs.append(remote_on_node.remote(**remote_kwargs))
    return ray.get(refs)


def download_model():
    """Download and cache the model using Hugging Face's transformers library."""
    from transformers import AutoTokenizer, AutoModelForCausalLM

    print(f"Downloading {model_name} model/tokenizer to cache...")
    _ = AutoTokenizer.from_pretrained(model_name)
    _ = AutoModelForCausalLM.from_pretrained(model_name)
    print(f"Model {model_name} is cached on this node.")
    return True


def train_func(config):
    """Per-worker training function with manual process-group init for CPU or GPU."""
    # Ray Train context
    ctx = train.get_context()
    rank = ctx.get_world_rank()
    world_size = ctx.get_world_size()
    local_rank = ctx.get_local_rank()

    # Basic hyperparams
    use_gpu = config["use_gpu"]
    batch_size = config["batch_size"]
    epochs = config["epochs"]
    steps_per_epoch = config["steps_per_epoch"]

    # Manually set environment so Torch/DeepSpeed sees correct info
    os.environ["RANK"] = str(rank)
    os.environ["WORLD_SIZE"] = str(world_size)
    os.environ["LOCAL_RANK"] = str(local_rank)
    os.environ["MASTER_ADDR"] = "127.0.0.1"

    # Decide backend
    backend = "nccl" if use_gpu else "gloo"

    # Initialize torch distributed if not already
    if not dist.is_initialized():
        dist.init_process_group(
            backend=backend,
            init_method="env://",
            rank=rank,
            world_size=world_size,
        )

    # Enable TF32 if GPU
    if use_gpu:
        torch.backends.cuda.matmul.allow_tf32 = True

    import deepspeed

    deepspeed.init_distributed()

    # Build explicit DeepSpeed config to avoid mismatch
    deepspeed_cfg = {
        "optimizer": {
            "type": "AdamW",
            "params": {
                "lr": "auto",
                "betas": "auto",
                "eps": "auto",
            },
        },
        "zero_optimization": {
            "stage": 3,
            "offload_optimizer": {
                "device": "cpu",
                "pin_memory": True,
            },
            "contiguous_gradients": True,
            "gather_16bit_weights_on_model_save": True,
        },
        "gradient_accumulation_steps": "auto",
        "gradient_clipping": "auto",
        "train_batch_size": "auto",
        "train_micro_batch_size_per_gpu": "auto",
        "steps_per_print": 10,
        "wall_clock_breakdown": False,
    }
    if use_gpu:
        deepspeed_cfg["fp16"] = {
            "enabled": "auto",
            "initial_scale_power": 8,
            "hysteresis": 4,
            "consecutive_hysteresis": True,
        }
        deepspeed_cfg["bf16"] = {"enabled": "auto"}
        deepspeed_cfg["zero_optimization"].update(
            {
                "overlap_comm": True,
                "reduce_bucket_size": "auto",
                "stage3_prefetch_bucket_size": "auto",
                "stage3_param_persistence_threshold": "auto",
                "gather_16bit_weights_on_model_save": True,
                "round_robin_gradients": True,
            }
        )

    disable_progress_bar()

    training_args = TrainingArguments(
        output_dir="output",
        per_device_train_batch_size=batch_size,
        gradient_accumulation_steps=1,
        max_steps=steps_per_epoch * epochs,
        save_strategy="steps",
        save_steps=steps_per_epoch,
        logging_steps=1,
        fp16=use_gpu,
        no_cuda=not use_gpu,
        log_on_each_node=False,
        gradient_checkpointing=True,
        deepspeed=deepspeed_cfg,
        report_to="none",
        disable_tqdm=True,
        push_to_hub=False,
    )

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    tokenizer.pad_token = tokenizer.eos_token

    print(f"[Worker {rank}] Loading model {model_name}...")
    model = AutoModelForCausalLM.from_pretrained(model_name, use_cache=False)
    model.resize_token_embeddings(len(tokenizer))
    print(f"[Worker {rank}] Model loaded")

    enable_progress_bar()
    import evaluate as hf_evaluate

    metric = hf_evaluate.load("accuracy")

    train_ds = train.get_dataset_shard("train")
    eval_ds = train.get_dataset_shard("validation")

    train_ds_iterable = train_ds.iter_torch_batches(
        batch_size=batch_size,
        local_shuffle_buffer_size=batch_size * world_size,
    )
    eval_ds_iterable = eval_ds.iter_torch_batches(batch_size=batch_size)

    def compute_metrics(eval_pred):
        logits, labels = eval_pred
        predictions = np.argmax(logits, axis=-1)
        return metric.compute(predictions=predictions, references=labels)

    hf_trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds_iterable,
        eval_dataset=eval_ds_iterable,
        compute_metrics=compute_metrics,
        tokenizer=tokenizer,
        data_collator=default_data_collator,
    )
    hf_trainer.add_callback(RayTrainReportCallback())
    hf_trainer = prepare_trainer(hf_trainer)
    hf_trainer.train()


if __name__ == "__main__":
    main()
