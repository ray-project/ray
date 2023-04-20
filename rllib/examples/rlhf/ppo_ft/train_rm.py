import argparse
import os
from typing import Tuple

from datasets import load_dataset
import evaluate
from huggingface_hub import snapshot_download
import numpy as np
import torch
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    TrainingArguments,
    default_data_collator
)

import ray
from ray.air import session
from ray.air.config import DatasetConfig, ScalingConfig
from ray.train.huggingface import HuggingFaceTrainer

from reward_model import RewardModelTrainer, get_reward_model


def get_model(model_name: str) -> Tuple[AutoModelForCausalLM, AutoTokenizer]:
    """Get model and tokenizer."""

    # Cache model locally if necessary.
    model_dir = snapshot_download(
        repo_id=model_name,
        allow_patterns=["*"],
        # Skip downloading TF, FLAX, and Rust weight files.
        ignore_patterns=["*.safetensors", "*.msgpack", "*.h5", "*.tflite", "*.ot"],
        revision=None,
    )

    reward_model = get_reward_model(model_dir)

    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    tokenizer.pad_token = tokenizer.eos_token

    return reward_model, tokenizer


def get_datasets(
    dataset_name: str,
    tokenizer: AutoTokenizer,
    block_size: int,
) -> Tuple[ray.data.Dataset, ray.data.Dataset]:
    """Download and pre-process the datasets."""
    # Load sharded data.
    datasets = load_dataset(dataset_name)

    # Block size or tokenzier's max length, whichever is smaller.
    block_size = min(block_size, tokenizer.model_max_length)

    def tokenize(row):
        # TODO(jungong) : need to repeatedly handle the part that runs over max_length,
        # with a reasonable stride window.
        chosen = tokenizer(
            row["prompt"] + row["chosen"],
            max_length=block_size,
            truncation=True,
            padding="max_length",
            return_tensors="np",
        )
        row["chosen_input_ids"] = chosen["input_ids"]
        row["chosen_attention_mask"] = chosen["attention_mask"]

        rejected = tokenizer(
            row["prompt"] + row["rejected"],
            max_length=block_size,
            truncation=True,
            padding="max_length",
            return_tensors="np",
        )
        row["rejected_input_ids"] = rejected["input_ids"]
        row["rejected_attention_mask"] = rejected["attention_mask"]

        # Column 0 always contains the preferred response.
        row["labels"] = 0

        return row

    def preprocess(ds):
        return ds.map(
            tokenize, remove_columns=["chosen", "rejected", "prompt", "response"]
        )

    return preprocess(datasets["train"]), preprocess(datasets["test"])


def trainer_init_per_worker(model, tokenizer, args):    
    # Use the actual number of CPUs assigned by Ray
    os.environ["OMP_NUM_THREADS"] = str(
        session.get_trial_resources().bundles[-1].get("CPU", 1)
    )
    # Enable tf32 for better performance
    torch.backends.cuda.matmul.allow_tf32 = True

    # Get sharded dataset.
    train_dataset, eval_dataset = get_datasets(
        args.dataset_name,
        tokenizer,
        args.block_size
    )

    deepspeed = {
        "fp16": {
            "enabled": "auto",
            "initial_scale_power": 8,
        },
        "bf16": {"enabled": "auto"},
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
            "offload_param": {
                "device": "cpu",
                "pin_memory": True,
            },
            "overlap_comm": True,
            "contiguous_gradients": True,
            "reduce_bucket_size": "auto",
            "stage3_prefetch_bucket_size": "auto",
            "stage3_param_persistence_threshold": "auto",
            "gather_16bit_weights_on_model_save": True,
            "round_robin_gradients": True,
        },
        "gradient_accumulation_steps": "auto",
        "gradient_clipping": "auto",
        "steps_per_print": 10,
        "train_batch_size": "auto",
        "train_micro_batch_size_per_gpu": "auto",
        "wall_clock_breakdown": False,
    }

    print("Preparing training arguments")
    training_args = TrainingArguments(
        "output",
        per_device_train_batch_size=args.batch_size,
        logging_steps=1,
        logging_strategy="steps",
        save_strategy="no",
        evaluation_strategy="steps",
        per_device_eval_batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        weight_decay=args.weight_decay,
        warmup_steps=args.warmup_steps,
        num_train_epochs=args.epochs,
        push_to_hub=False,
        disable_tqdm=True,  # declutter the output a little
        label_names=["labels"],
        # bf16=True,
        fp16=True,
        gradient_checkpointing=True,
        deepspeed=deepspeed,
    )

    metric = evaluate.load("accuracy")

    def compute_metrics(eval_pred):
        rewards, labels = eval_pred
        predictions = np.argmax(rewards, axis=-1)
        return metric.compute(predictions=predictions, references=labels)

    trainer = RewardModelTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        compute_metrics=compute_metrics,
        tokenizer=tokenizer,
        data_collator=default_data_collator,
    )

    return trainer


def train(args):
    """Supervise fine-tune a LLM."""
    model, tokenizer = get_model(args.model_name)

    trainer = HuggingFaceTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config={
            "model": model,
            "tokenizer": tokenizer,
            "args": args,
        },
        scaling_config=ScalingConfig(
            num_workers=args.num_workers,
            use_gpu=True,
            resources_per_worker={"GPU": 1, "CPU": args.cpus_per_worker},
        ),
    )
    
    result = trainer.fit()
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--model_name", type=str, default="gpt2"
    )
    parser.add_argument(
        "--dataset_name", type=str, default="Dahoas/rm-static"
    )
    parser.add_argument(
        "--num_workers", type=int, default=8,
        help="Number of DDP workers to use."
    )
    parser.add_argument(
        "--cpus_per_worker", type=int, default=4,
        help="Number of CPUs to use for each DDP worker."
    )
    parser.add_argument(
        "--epochs", type=int, default=3,
        help="Max number of epochs to train."
    )
    parser.add_argument(
        "--max_len", type=int, default=1024,
        help="Max number of tokens to generate."
    )
    parser.add_argument(
        "--block_size", type=int, default=1024,
        help=(
            "Optional input sequence length after tokenization. "
            "The training dataset will be truncated in block of this size."
        )
    )
    parser.add_argument(
        "--batch_size", type=int, default=16,
        help="Per device batch size."
    )
    parser.add_argument("--learning_rate", type=float, default=5e-6)
    parser.add_argument("--warmup_steps", type=int, default=0)
    parser.add_argument("--weight_decay", type=float, default=0.01)

    args = parser.parse_args()

    train(args)
