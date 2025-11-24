import argparse
import logging
import os
import tempfile
import uuid
from typing import Any, Dict

os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

import deepspeed
import torch
from datasets import DownloadConfig, load_dataset
from torch.utils.data import DataLoader
from transformers import AutoModelForCausalLM, AutoTokenizer

import ray
import ray.train
import ray.train.torch
from ray.train import Checkpoint, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer

logger = logging.getLogger(__name__)


def log_rank0(message: str) -> None:
    if ray.train.get_context().get_world_rank() == 0:
        logger.info(message)


def get_tokenizer(model_name: str, trust_remote_code: bool = True) -> Any:
    """
    Load and configure the tokenizer for the given model.

    Args:
        model_name: Name of the model to load tokenizer for
        trust_remote_code: Whether to trust remote code

    Returns:
        Configured tokenizer
    """
    tokenizer = AutoTokenizer.from_pretrained(
        model_name, trust_remote_code=trust_remote_code
    )

    # Set pad token if not already set
    if tokenizer.pad_token is None:
        if tokenizer.eos_token is not None:
            tokenizer.pad_token = tokenizer.eos_token
        else:
            # Fallback for models without eos_token
            tokenizer.pad_token = tokenizer.unk_token

    return tokenizer


def setup_dataloader(
    model_name: str, dataset_name: str, seq_length: int, batch_size: int
) -> DataLoader:
    tokenizer = get_tokenizer(model_name)

    dataset = load_dataset(
        dataset_name,
        split="train[:1%]",
        download_config=DownloadConfig(disable_tqdm=True),
    )

    def tokenize_function(examples):
        return tokenizer(
            examples["text"],
            padding="max_length",
            max_length=seq_length,
            truncation=True,
        )

    tokenized_dataset = dataset.map(
        tokenize_function, batched=True, num_proc=1, keep_in_memory=True
    )
    tokenized_dataset.set_format(type="torch", columns=["input_ids", "attention_mask"])
    data_loader = DataLoader(tokenized_dataset, batch_size=batch_size, shuffle=True)

    return ray.train.torch.prepare_data_loader(data_loader)


def setup_model_and_optimizer(
    model_name: str, learning_rate: float, ds_config: Dict[str, Any]
) -> deepspeed.runtime.engine.DeepSpeedEngine:
    model = AutoModelForCausalLM.from_pretrained(model_name)
    log_rank0(
        f"Model loaded: {model_name} (#parameters: {sum(p.numel() for p in model.parameters())})"
    )

    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    ds_engine, optimizer, _, _ = deepspeed.initialize(
        model=model,
        optimizer=optimizer,
        config=ds_config,
    )
    return ds_engine


def report_metrics_and_save_checkpoint(
    ds_engine: deepspeed.runtime.engine.DeepSpeedEngine, metrics: Dict[str, Any]
) -> None:
    ctx = ray.train.get_context()
    epoch_value = metrics["epoch"]

    with tempfile.TemporaryDirectory() as tmp_dir:
        checkpoint_dir = os.path.join(tmp_dir, "checkpoint")
        os.makedirs(checkpoint_dir, exist_ok=True)

        ds_engine.save_checkpoint(checkpoint_dir)

        epoch_file = os.path.join(checkpoint_dir, "epoch.txt")
        with open(epoch_file, "w", encoding="utf-8") as f:
            f.write(str(epoch_value))

        checkpoint = Checkpoint.from_directory(tmp_dir)
        ray.train.report(metrics, checkpoint=checkpoint)

        if ctx.get_world_rank() == 0:
            experiment_name = ctx.get_experiment_name()
            log_rank0(
                f"Checkpoint saved successfully for experiment {experiment_name} at {checkpoint_dir}. Metrics: {metrics}"
            )


def load_checkpoint(
    ds_engine: deepspeed.runtime.engine.DeepSpeedEngine, ckpt: ray.train.Checkpoint
) -> int:
    next_epoch = 0
    try:
        with ckpt.as_directory() as checkpoint_dir:
            log_rank0(f"Loading checkpoint from {checkpoint_dir}")
            epoch_dir = os.path.join(checkpoint_dir, "checkpoint")
            if not os.path.isdir(epoch_dir):
                epoch_dir = checkpoint_dir

            ds_engine.load_checkpoint(epoch_dir)

            epoch_file = os.path.join(epoch_dir, "epoch.txt")
            if os.path.isfile(epoch_file):
                with open(epoch_file, "r", encoding="utf-8") as f:
                    last_epoch = int(f.read().strip())
                next_epoch = last_epoch + 1

            if torch.distributed.is_available() and torch.distributed.is_initialized():
                torch.distributed.barrier()

        log_rank0("Successfully loaded distributed checkpoint")
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {e}")
        raise RuntimeError(f"Checkpoint loading failed: {e}") from e
    return next_epoch


def train_loop(config: Dict[str, Any]) -> None:

    ds_engine = setup_model_and_optimizer(
        config["model_name"], config["learning_rate"], config["ds_config"]
    )

    # Load checkpoint if exists
    ckpt = ray.train.get_checkpoint()
    start_epoch = 0
    if ckpt:
        start_epoch = load_checkpoint(ds_engine, ckpt)

    if start_epoch > 0:
        log_rank0(f"Resuming training from epoch {start_epoch}")

    train_loader = setup_dataloader(
        config["model_name"],
        config["dataset_name"],
        config["seq_length"],
        config["batch_size"],
    )
    steps_per_epoch = len(train_loader)
    device = ray.train.torch.get_device()

    # Set model to training mode
    ds_engine.train()

    for epoch in range(start_epoch, config["epochs"]):
        if ray.train.get_context().get_world_size() > 1 and hasattr(
            train_loader, "sampler"
        ):
            sampler = getattr(train_loader, "sampler", None)
            if sampler and hasattr(sampler, "set_epoch"):
                sampler.set_epoch(epoch)

        running_loss = 0.0
        num_batches = 0
        for step, batch in enumerate(train_loader):
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            outputs = ds_engine(
                input_ids=input_ids,
                attention_mask=attention_mask,
                labels=input_ids,
                use_cache=False,
            )
            loss = outputs.loss
            log_rank0(
                f"Epoch: {epoch} Step: {step + 1}/{steps_per_epoch} Loss: {loss.item()}"
            )

            ds_engine.backward(loss)
            ds_engine.step()

            running_loss += loss.item()
            num_batches += 1

            if config["debug_steps"] > 0 and step + 1 >= config["debug_steps"]:
                log_rank0(f"Debug steps finished. Stopping epoch {epoch}.")
                break

        report_metrics_and_save_checkpoint(
            ds_engine,
            {"loss": running_loss / num_batches, "epoch": epoch},
        )


def main():
    args = get_args()
    print(args)

    scaling_config = ScalingConfig(
        num_workers=args.num_workers, use_gpu=not args.cpu_only
    )

    ds_config = {
        "train_micro_batch_size_per_gpu": args.batch_size,
        "bf16": {"enabled": True},
        "grad_accum_dtype": "bf16",
        "zero_optimization": {
            "stage": args.zero_stage,
            "overlap_comm": True,
            "contiguous_gradients": True,
        },
        "gradient_clipping": 1.0,
    }

    train_loop_config = {
        "epochs": args.num_epochs,
        "learning_rate": args.learning_rate,
        "batch_size": args.batch_size,
        "ds_config": ds_config,
        "model_name": args.model_name,
        "seq_length": args.seq_length,
        "dataset_name": args.dataset_name,
        "debug_steps": args.debug_steps,
    }

    name = (
        f"deepspeed_sample_{uuid.uuid4().hex[:8]}"
        if args.resume_experiment is None
        else args.resume_experiment
    )
    print(f"Experiment name: {name}")
    run_config = RunConfig(
        storage_path=args.storage_path,
        name=name,
    )

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=scaling_config,
        train_loop_config=train_loop_config,
        run_config=run_config,
    )

    result = trainer.fit()
    print(f"Training finished. Result: {result}")


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", type=str, default="gpt2")
    parser.add_argument("--dataset_name", type=str, default="ag_news")
    parser.add_argument("--batch_size", type=int, default=1)
    parser.add_argument("--num_epochs", type=int, default=1)
    parser.add_argument("--seq_length", type=int, default=512)
    parser.add_argument("--learning_rate", type=float, default=1e-6)
    parser.add_argument("--zero_stage", type=int, default=3)
    parser.add_argument("--num_workers", type=int, default=2)
    parser.add_argument("--cpu_only", action="store_true", help="Disable GPU usage")
    parser.add_argument("--storage_path", type=str, default="/mnt/cluster_storage")
    parser.add_argument(
        "--resume_experiment",
        type=str,
        default=None,
        help="Path to the experiment to resume from",
    )
    parser.add_argument("--debug_steps", type=int, default=0)

    return parser.parse_args()


if __name__ == "__main__":
    main()
