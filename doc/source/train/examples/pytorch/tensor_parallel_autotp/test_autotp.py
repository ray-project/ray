#!/usr/bin/env python
"""Test script for DeepSpeed AutoTP with Ray Train, including checkpoint loading verification."""

import subprocess
subprocess.run(r'''pip install torch transformers datasets deepspeed''',
               shell=True,
               check=True,
               executable='/bin/bash')

# Enable Ray Train V2 for the latest train APIs
import os
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

import json
import logging
import tempfile
import uuid
from typing import List

import torch
import torch.distributed as dist

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from datasets import DownloadConfig, load_dataset
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
from transformers import AutoTokenizer

import ray.train


def create_dataloader(
    model_name: str,
    dataset_name: str,
    seq_length: int,
    batch_size: int,
    dp_rank: int,
    dp_size: int,
    seed: int = 42,
    dataset_percentage: float = 10.0,
) -> DataLoader:
    """
    Create dataloader with TP-aware sharding.

    IMPORTANT: Uses dp_rank/dp_size for sharding (NOT world_rank/world_size).
    This ensures all TP ranks in the same DP group see identical batches.
    """
    world_rank = ray.train.get_context().get_world_rank()

    # Handle datasets that require a config name
    dataset_config = "wikitext-2-raw-v1" if dataset_name == "wikitext" else None
    split_spec = f"train[:{int(dataset_percentage)}%]"

    # Rank 0 downloads first to avoid conflicts
    if world_rank == 0:
        tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        dataset = load_dataset(
            dataset_name, dataset_config, split=split_spec,
            download_config=DownloadConfig(disable_tqdm=True),
        )
    dist.barrier()

    # Other ranks load from cache
    if world_rank != 0:
        tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        dataset = load_dataset(
            dataset_name, dataset_config, split=split_spec,
            download_config=DownloadConfig(disable_tqdm=True),
        )

    # Set pad token if needed
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # Tokenize dataset
    def tokenize_fn(examples):
        return tokenizer(
            examples["text"], padding="max_length", max_length=seq_length, truncation=True
        )

    tokenized = dataset.map(
        tokenize_fn, batched=True, num_proc=1, keep_in_memory=True,
        remove_columns=dataset.column_names,
    )

    # Add labels (same as input_ids for causal LM)
    def add_labels(examples):
        examples["labels"] = examples["input_ids"].copy()
        return examples

    tokenized = tokenized.map(add_labels, batched=True, num_proc=1, keep_in_memory=True)
    tokenized.set_format(type="torch", columns=["input_ids", "attention_mask", "labels"])

    # [1] Use DP rank/size for sharding (ensures TP ranks get same data)
    sampler = DistributedSampler(
        tokenized, num_replicas=dp_size, rank=dp_rank, shuffle=True, seed=seed
    )

    return DataLoader(tokenized, batch_size=batch_size, sampler=sampler, drop_last=True)

import deepspeed
from deepspeed.module_inject.layers import set_autotp_mode
from transformers import AutoConfig, AutoModelForCausalLM

import ray.train.torch


class ModelParallelUnit:
    """
    Model Parallel Unit (MPU) interface for DeepSpeed.

    DeepSpeed uses this to understand the parallelism topology and
    perform gradient all-reduce only across data parallel ranks.
    """

    def __init__(
        self,
        tp_group: dist.ProcessGroup,
        dp_group: dist.ProcessGroup,
        tp_size: int,
        dp_size: int,
        tp_rank: int,
        dp_rank: int,
    ):
        self._tp_group = tp_group
        self._dp_group = dp_group
        self._tp_size = tp_size
        self._dp_size = dp_size
        self._tp_rank = tp_rank
        self._dp_rank = dp_rank

    def get_data_parallel_group(self) -> dist.ProcessGroup:
        return self._dp_group

    def get_model_parallel_group(self) -> dist.ProcessGroup:
        return self._tp_group

    def get_data_parallel_world_size(self) -> int:
        return self._dp_size

    def get_model_parallel_world_size(self) -> int:
        return self._tp_size

    def get_data_parallel_rank(self) -> int:
        return self._dp_rank

    def get_model_parallel_rank(self) -> int:
        return self._tp_rank


def setup_model_with_autotp(
    model_name: str,
    tp_size: int,
    dp_size: int,
    world_rank: int,
    world_size: int,
    device: torch.device,
    config: dict,
):
    """
    Set up the model with DeepSpeed AutoTP and optional data parallelism.

    Returns:
        tuple: (engine, tp_group, dp_group, tp_rank, dp_rank)
    """
    dtype = torch.bfloat16

    # Calculate TP and DP rank
    tp_rank = world_rank % tp_size
    dp_rank = world_rank // tp_size

    # Validate configuration
    if dp_size * tp_size != world_size:
        raise ValueError(
            f"dp_size ({dp_size}) * tp_size ({tp_size}) must equal "
            f"world_size ({world_size})"
        )

    # Load model config and validate TP compatibility
    hf_config = AutoConfig.from_pretrained(model_name, trust_remote_code=True)
    if hf_config.num_key_value_heads % tp_size != 0:
        raise ValueError(
            f"TP size {tp_size} must divide num_key_value_heads "
            f"{hf_config.num_key_value_heads}"
        )

    if world_rank == 0:
        logger.info(f"Setting up 2D mesh: dp_size={dp_size}, tp_size={tp_size}")

    # [1] Create TP and DP process groups
    tp_group = None
    dp_group = None

    # Create TP groups (processes in the same row)
    for dp_idx in range(dp_size):
        tp_group_ranks = list(range(dp_idx * tp_size, (dp_idx + 1) * tp_size))
        group = dist.new_group(tp_group_ranks)
        if world_rank in tp_group_ranks:
            tp_group = group

    # Create DP groups (processes in the same column)
    for tp_idx in range(tp_size):
        dp_group_ranks = [tp_idx + dp_idx * tp_size for dp_idx in range(dp_size)]
        group = dist.new_group(dp_group_ranks)
        if world_rank in dp_group_ranks:
            dp_group = group

    if world_rank == 0:
        logger.info(f"Process groups created: tp_rank={tp_rank}, dp_rank={dp_rank}")

    # [2] Enable AutoTP instrumentation BEFORE model creation
    set_autotp_mode(training=True)

    # [3] Create model on the target device
    prev_device = torch.get_default_device()
    torch.set_default_device(device)
    model = AutoModelForCausalLM.from_config(hf_config).to(dtype=dtype)
    torch.set_default_device(prev_device)

    # [4] Apply TP sharding with deepspeed.tp_model_init()
    model = deepspeed.tp_model_init(
        model,
        tp_size=tp_size,
        dtype=dtype,
        tp_group=tp_group,
    )

    # [5] Build DeepSpeed config
    batch_size = config.get("batch_size", 1)
    effective_dp = dp_size if tp_size > 1 else world_size

    ds_config = {
        "train_batch_size": batch_size * effective_dp,
        "train_micro_batch_size_per_gpu": batch_size,
        "gradient_accumulation_steps": 1,
        "gradient_clipping": config.get("max_grad_norm", 1.0),
        "zero_optimization": {
            "stage": 1,
            "overlap_comm": True,
        },
        "tensor_parallel": {
            "autotp_size": tp_size,
        },
        "data_parallel_size": dp_size,
        "zero_allow_untested_optimizer": True,
        "bf16": {
            "enabled": True,
            "bf16_master_weights_and_grads": True,
            "bf16_optimizer_states": True,
        },
        "steps_per_print": 2000,
        "wall_clock_breakdown": False,
    }

    # [6] Create optimizer
    params = list(model.parameters())
    optimizer = torch.optim.AdamW(
        params,
        lr=config.get("learning_rate", 1e-5),
        weight_decay=config.get("weight_decay", 0.01),
    )

    # [7] Create MPU for DeepSpeed
    mpu = ModelParallelUnit(
        tp_group=tp_group,
        dp_group=dp_group,
        tp_size=tp_size,
        dp_size=dp_size,
        tp_rank=tp_rank,
        dp_rank=dp_rank,
    )

    # [8] Initialize DeepSpeed engine
    engine, optimizer, _, _ = deepspeed.initialize(
        model=model,
        optimizer=optimizer,
        config=ds_config,
        mpu=mpu if tp_size > 1 else None,
    )

    if world_rank == 0:
        num_params = sum(p.numel() for p in params)
        logger.info(f"Model initialized with {num_params:,} parameters")
        if dp_size > 1:
            logger.info(f"2D parallelism: {dp_size} DP x {tp_size} TP")

    return engine, tp_group, dp_group, tp_rank, dp_rank


from ray.train import Checkpoint


def save_checkpoint(
    engine,
    world_rank: int,
    epoch: int,
    step: int,
    avg_loss: float,
) -> None:
    """Save checkpoint and report to Ray Train."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        checkpoint_dir = os.path.join(tmp_dir, "checkpoint")
        os.makedirs(checkpoint_dir, exist_ok=True)

        # Each rank saves its model/optimizer shard
        torch.save(
            engine.module.state_dict(),
            os.path.join(checkpoint_dir, f"model_rank{world_rank}.pt"),
        )
        torch.save(
            engine.optimizer.state_dict(),
            os.path.join(checkpoint_dir, f"optimizer_rank{world_rank}.pt"),
        )

        # Save metadata (from rank 0 only)
        if world_rank == 0:
            with open(os.path.join(checkpoint_dir, "metadata.json"), "w") as f:
                json.dump({"epoch": epoch, "step": step}, f)

        # All workers must call report() with their checkpoint
        checkpoint = Checkpoint.from_directory(tmp_dir)
        ray.train.report({"loss": avg_loss, "epoch": epoch}, checkpoint=checkpoint)


def train_func(config):
    """
    Main training loop executed by each Ray Train worker.

    This function:
    1. Sets up TP and DP process groups
    2. Creates and shards the model with DeepSpeed AutoTP
    3. Runs the training loop with checkpointing
    """
    # Get Ray Train context
    world_rank = ray.train.get_context().get_world_rank()
    world_size = ray.train.get_context().get_world_size()
    device = ray.train.torch.get_device()

    tp_size = config["tp_size"]
    dp_size = config["dp_size"]

    if world_rank == 0:
        logger.info(f"Worker started: world_rank={world_rank}, world_size={world_size}")

    # Set up model with DeepSpeed AutoTP
    engine, tp_group, dp_group, tp_rank, dp_rank = setup_model_with_autotp(
        model_name=config["model_name"],
        tp_size=tp_size,
        dp_size=dp_size,
        world_rank=world_rank,
        world_size=world_size,
        device=device,
        config=config,
    )

    if world_rank == 0:
        if dp_size > 1:
            logger.info(f"2D parallelism: {dp_size} DP x {tp_size} TP")

    # Create dataloader with TP-aware sharding
    dataloader = create_dataloader(
        model_name=config["model_name"],
        dataset_name=config["dataset_name"],
        seq_length=config["seq_length"],
        batch_size=config["batch_size"],
        dp_rank=dp_rank,
        dp_size=dp_size,
        seed=config.get("seed", 42),
        dataset_percentage=config.get("dataset_percentage", 10.0),
    )

    steps_per_epoch = len(dataloader)
    if world_rank == 0:
        logger.info(f"Dataloader created: {steps_per_epoch} steps per epoch")

    # Training loop
    engine.train()

    for epoch in range(config["num_epochs"]):
        dataloader.sampler.set_epoch(epoch)

        running_loss = 0.0
        num_batches = 0

        for step, batch in enumerate(dataloader):
            # Move batch to device
            batch = {k: v.to(device) for k, v in batch.items()}

            # Forward pass with labels for loss computation
            outputs = engine(
                input_ids=batch["input_ids"],
                attention_mask=batch["attention_mask"],
                labels=batch["labels"],
                use_cache=False,
            )
            loss = outputs.loss

            # Backward pass (through DeepSpeed engine)
            engine.backward(loss)

            # Optimizer step (through DeepSpeed engine)
            engine.step()

            # Track loss
            loss_value = loss.item()
            running_loss += loss_value
            num_batches += 1

            # Log progress
            if world_rank == 0 and step % config.get("log_interval", 10) == 0:
                logger.info(
                    f"Epoch: {epoch} Step: {step + 1}/{steps_per_epoch} Loss: {loss_value:.4f}"
                )

            # Debug mode: stop early for testing
            if config.get("debug_steps", 0) > 0 and step + 1 >= config["debug_steps"]:
                if world_rank == 0:
                    logger.info(f"Debug steps finished. Stopping epoch {epoch}.")
                break

        # Calculate average loss for epoch
        avg_loss = running_loss / num_batches if num_batches > 0 else 0.0

        # Save checkpoint at end of epoch
        save_checkpoint(engine, world_rank, epoch, step, avg_loss)

        if world_rank == 0:
            logger.info(f"Epoch {epoch} completed. Average loss: {avg_loss:.4f}")


def load_checkpoint_func(config):
    """
    Function to verify checkpoint loading works correctly.

    This function:
    1. Sets up the model with DeepSpeed AutoTP (same as training)
    2. Loads the checkpoint from the previous training run
    3. Verifies the checkpoint was loaded correctly
    """
    # Get Ray Train context
    world_rank = ray.train.get_context().get_world_rank()
    world_size = ray.train.get_context().get_world_size()
    device = ray.train.torch.get_device()

    tp_size = config["tp_size"]
    dp_size = config["dp_size"]

    if world_rank == 0:
        logger.info(f"Checkpoint loading test: world_rank={world_rank}, world_size={world_size}")

    # Set up model with DeepSpeed AutoTP
    engine, tp_group, dp_group, tp_rank, dp_rank = setup_model_with_autotp(
        model_name=config["model_name"],
        tp_size=tp_size,
        dp_size=dp_size,
        world_rank=world_rank,
        world_size=world_size,
        device=device,
        config=config,
    )

    # Get checkpoint from Ray Train
    checkpoint = ray.train.get_checkpoint()
    if checkpoint is None:
        raise ValueError("No checkpoint found!")

    if world_rank == 0:
        logger.info(f"Loading checkpoint...")

    # Load checkpoint
    with checkpoint.as_directory() as checkpoint_dir:
        # List files in checkpoint directory
        checkpoint_subdir = os.path.join(checkpoint_dir, "checkpoint")
        if world_rank == 0:
            files = os.listdir(checkpoint_subdir)
            logger.info(f"Checkpoint files: {files}")

        # Load model state dict for this rank
        model_path = os.path.join(checkpoint_subdir, f"model_rank{world_rank}.pt")
        if os.path.exists(model_path):
            state_dict = torch.load(model_path, map_location=device)
            engine.module.load_state_dict(state_dict)
            if world_rank == 0:
                logger.info(f"Model state dict loaded successfully")
        else:
            raise FileNotFoundError(f"Model checkpoint not found: {model_path}")

        # Load optimizer state dict for this rank
        optimizer_path = os.path.join(checkpoint_subdir, f"optimizer_rank{world_rank}.pt")
        if os.path.exists(optimizer_path):
            optimizer_state_dict = torch.load(optimizer_path, map_location=device)
            engine.optimizer.load_state_dict(optimizer_state_dict)
            if world_rank == 0:
                logger.info(f"Optimizer state dict loaded successfully")
        else:
            raise FileNotFoundError(f"Optimizer checkpoint not found: {optimizer_path}")

        # Load and verify metadata
        metadata_path = os.path.join(checkpoint_subdir, "metadata.json")
        if os.path.exists(metadata_path):
            with open(metadata_path, "r") as f:
                metadata = json.load(f)
            if world_rank == 0:
                logger.info(f"Checkpoint metadata: {metadata}")

    # Do a forward pass to verify the model works after loading
    if world_rank == 0:
        logger.info("Running forward pass to verify checkpoint loading...")

    # Create a dummy input
    dummy_input = torch.randint(0, 1000, (1, 32), device=device)
    dummy_attention_mask = torch.ones(1, 32, device=device)

    engine.eval()
    with torch.no_grad():
        outputs = engine(
            input_ids=dummy_input,
            attention_mask=dummy_attention_mask,
            use_cache=False,
        )

    if world_rank == 0:
        logger.info(f"Forward pass successful! Output shape: {outputs.logits.shape}")
        logger.info("Checkpoint loading verification PASSED!")

    # Report success
    ray.train.report({"checkpoint_loaded": True, "verification": "passed"})


if __name__ == "__main__":
    from ray.train import RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer

    # Use a local temp directory for storage
    storage_path = tempfile.mkdtemp(prefix="ray_tp_test_")
    print(f"Using storage path: {storage_path}")

    # Parallelism configuration
    tp_size = 2  # Tensor parallel degree
    dp_size = 2  # Data parallel degree
    num_workers = tp_size * dp_size  # Total workers must equal tp_size * dp_size

    # Configure distributed training resources
    scaling_config = ScalingConfig(
        num_workers=num_workers,
        use_gpu=True,
    )

    # Training configuration
    train_loop_config = {
        # Model and data
        "model_name": "Qwen/Qwen2.5-0.5B",  # Small model for demo
        "dataset_name": "wikitext",
        "dataset_percentage": 5.0,  # Use 5% of dataset for faster demo
        # Parallelism
        "tp_size": tp_size,
        "dp_size": dp_size,
        # Training hyperparameters
        "batch_size": 1,
        "seq_length": 512,
        "num_epochs": 1,
        "learning_rate": 1e-5,
        "weight_decay": 0.01,
        # Logging and debug
        "log_interval": 5,
        "debug_steps": 10,  # Stop after 10 steps for faster testing
        "seed": 42,
    }

    # Create experiment name
    experiment_name = f"tp_autotp_test_{uuid.uuid4().hex[:8]}"

    # Configure run settings
    run_config = RunConfig(
        storage_path=storage_path,
        name=experiment_name,
    )

    # ========== PHASE 1: Training ==========
    print("=" * 60)
    print("PHASE 1: Training with DeepSpeed AutoTP")
    print("=" * 60)

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
        train_loop_config=train_loop_config,
        run_config=run_config,
    )

    print(f"Starting tensor parallel training with {tp_size}-way TP and {dp_size}-way DP...")
    result = trainer.fit()
    print("Training completed successfully!")
    print(f"Final metrics: {result.metrics}")
    print(f"Checkpoint path: {result.checkpoint}")

    # ========== PHASE 2: Checkpoint Loading Verification ==========
    print("\n" + "=" * 60)
    print("PHASE 2: Checkpoint Loading Verification")
    print("=" * 60)

    # Create a new trainer that will load the checkpoint
    resume_config = RunConfig(
        storage_path=storage_path,
        name=experiment_name + "_resume",
    )

    resume_trainer = TorchTrainer(
        train_loop_per_worker=load_checkpoint_func,
        scaling_config=scaling_config,
        train_loop_config=train_loop_config,
        run_config=resume_config,
        resume_from_checkpoint=result.checkpoint,
    )

    print("Verifying checkpoint loading...")
    resume_result = resume_trainer.fit()
    print("Checkpoint loading verification completed!")
    print(f"Verification metrics: {resume_result.metrics}")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED!")
    print("=" * 60)
