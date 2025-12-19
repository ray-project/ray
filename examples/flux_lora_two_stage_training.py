"""
Two-Stage Training Pipeline: Flux Transformer + LoRA Adapter
=============================================================

This example demonstrates how to use Ray Data and Ray Train to implement a
two-stage training pipeline for the FLUX.1-Fill-dev inpainting transformer:

Stage 1: Full transformer fine-tuning (adapter disabled)
Stage 2: LoRA-only fine-tuning (transformer frozen)

The process alternates between these stages until desired checkpoints are obtained.

Dataset Format (JSONL):
    {"user_input": {"image": "path/to/image.png", "mask": "path/to/mask.png"}, 
     "output": "path/to/target.png"}

Requirements:
    pip install ray[train] torch diffusers transformers peft accelerate deepspeed pillow
"""

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import torch
from PIL import Image

import ray
import ray.train
from ray.train import Checkpoint, DataConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class TrainingConfig:
    """Configuration for the two-stage training pipeline."""
    
    # Model settings
    model_name: str = "black-forest-labs/FLUX.1-Fill-dev"
    
    # Training settings
    num_epochs_stage1: int = 3
    num_epochs_stage2: int = 5
    batch_size: int = 1
    learning_rate_stage1: float = 1e-5
    learning_rate_stage2: float = 1e-4
    gradient_accumulation_steps: int = 4
    
    # LoRA settings
    lora_rank: int = 64
    lora_alpha: int = 96
    lora_dropout: float = 0.05
    lora_target_modules: tuple = (
        "proj_in",
        "proj_out",
        "to_q",
        "to_k", 
        "to_v",
        "to_out.0",
        "ff.net.0.proj",
        "ff.net.2",
    )
    
    # DeepSpeed settings
    use_deepspeed: bool = True
    zero_stage: int = 2
    offload_optimizer: bool = True
    offload_param: bool = False
    
    # Output settings
    output_dir: str = "outputs/flux_fill_training"
    num_cycles: int = 2  # Number of Stage1 -> Stage2 cycles
    
    # Data settings
    image_size: int = 512
    seed: int = 42


def get_deepspeed_config(config: TrainingConfig, stage: int) -> Dict[str, Any]:
    """Generate DeepSpeed configuration for training."""
    
    # Use ZeRO-2 for Stage 1 (full transformer), ZeRO-2 for Stage 2 (LoRA only)
    return {
        "optimizer": {
            "type": "AdamW",
            "params": {
                "lr": config.learning_rate_stage1 if stage == 1 else config.learning_rate_stage2,
                "betas": [0.9, 0.999],
                "eps": 1e-8,
                "weight_decay": 0.01,
            },
        },
        "scheduler": {
            "type": "WarmupLR",
            "params": {
                "warmup_num_steps": 100,
            },
        },
        "bf16": {"enabled": True},
        "zero_optimization": {
            "stage": config.zero_stage,
            "offload_optimizer": {
                "device": "cpu" if config.offload_optimizer else "none",
            },
            "offload_param": {
                "device": "cpu" if config.offload_param else "none",
            },
            "contiguous_gradients": True,
            "overlap_comm": True,
        },
        "gradient_accumulation_steps": config.gradient_accumulation_steps,
        "gradient_clipping": 1.0,
        "train_micro_batch_size_per_gpu": config.batch_size,
        "wall_clock_breakdown": False,
    }


# =============================================================================
# Data Loading with Ray Data
# =============================================================================

def load_and_preprocess_image(path: str, size: int) -> torch.Tensor:
    """Load and preprocess an image to tensor."""
    image = Image.open(path).convert("RGB")
    image = image.resize((size, size), Image.LANCZOS)
    # Normalize to [-1, 1] for diffusion models
    tensor = torch.tensor([list(image.getdata())], dtype=torch.float32)
    tensor = tensor.reshape(3, size, size) / 127.5 - 1.0
    return tensor


def load_mask(path: str, size: int) -> torch.Tensor:
    """Load and preprocess a binary mask."""
    mask = Image.open(path).convert("L")
    mask = mask.resize((size, size), Image.NEAREST)
    tensor = torch.tensor(list(mask.getdata()), dtype=torch.float32)
    tensor = tensor.reshape(1, size, size) / 255.0
    return tensor


def preprocess_batch(batch: Dict[str, Any], image_size: int) -> Dict[str, torch.Tensor]:
    """Preprocess a batch of training examples."""
    images = []
    masks = []
    targets = []
    
    for i in range(len(batch["user_input"])):
        user_input = batch["user_input"][i]
        output_path = batch["output"][i]
        
        # Handle nested dict structure from JSONL
        if isinstance(user_input, str):
            user_input = json.loads(user_input)
        
        image_path = user_input["image"]
        mask_path = user_input["mask"]
        
        images.append(load_and_preprocess_image(image_path, image_size))
        masks.append(load_mask(mask_path, image_size))
        targets.append(load_and_preprocess_image(output_path, image_size))
    
    return {
        "images": torch.stack(images),
        "masks": torch.stack(masks),
        "targets": torch.stack(targets),
    }


def create_ray_dataset(jsonl_path: str) -> ray.data.Dataset:
    """Create a Ray Dataset from JSONL file."""
    # Read JSONL file with Ray Data
    ds = ray.data.read_json(jsonl_path, lines=True)
    return ds


# =============================================================================
# Model Setup
# =============================================================================

def setup_flux_transformer(config: TrainingConfig, enable_lora: bool = False):
    """
    Set up the Flux transformer model.
    
    For demonstration, we show the structure - in practice you would use:
    from diffusers import FluxTransformer2DModel
    """
    try:
        from diffusers import FluxTransformer2DModel
        
        transformer = FluxTransformer2DModel.from_pretrained(
            config.model_name,
            subfolder="transformer",
            torch_dtype=torch.bfloat16,
        )
    except ImportError:
        # Fallback for demonstration - create a simple transformer
        print("WARNING: diffusers not available, using mock transformer for demonstration")
        transformer = torch.nn.Sequential(
            torch.nn.Linear(512, 1024),
            torch.nn.GELU(),
            torch.nn.Linear(1024, 512),
        ).to(torch.bfloat16)
    
    if enable_lora:
        transformer = add_lora_to_transformer(transformer, config)
    
    return transformer


def add_lora_to_transformer(transformer, config: TrainingConfig):
    """Add LoRA adapters to the transformer."""
    try:
        from peft import LoraConfig, get_peft_model
        
        lora_config = LoraConfig(
            r=config.lora_rank,
            lora_alpha=config.lora_alpha,
            lora_dropout=config.lora_dropout,
            target_modules=list(config.lora_target_modules),
            init_lora_weights=True,
        )
        
        transformer = get_peft_model(transformer, lora_config)
        
        # Freeze base model weights
        for name, param in transformer.named_parameters():
            if "lora" not in name.lower():
                param.requires_grad = False
        
        return transformer
    
    except ImportError:
        print("WARNING: peft not available, returning model without LoRA")
        return transformer


def freeze_transformer_for_lora(transformer):
    """Freeze all transformer weights except LoRA parameters."""
    for name, param in transformer.named_parameters():
        if "lora" in name.lower():
            param.requires_grad = True
        else:
            param.requires_grad = False
    return transformer


# =============================================================================
# Training Functions
# =============================================================================

def train_stage1(config: Dict[str, Any]):
    """
    Stage 1: Full transformer fine-tuning.
    
    The transformer is fully trained with all parameters updated.
    Uses DeepSpeed ZeRO-2 for memory efficiency.
    """
    import deepspeed
    from deepspeed.accelerator import get_accelerator
    
    training_config = TrainingConfig(**config.get("training_config", {}))
    deepspeed_config = get_deepspeed_config(training_config, stage=1)
    
    # Get Ray Data shard for this worker
    train_ds = ray.train.get_dataset_shard("train")
    
    # Setup model
    transformer = setup_flux_transformer(training_config, enable_lora=False)
    
    # Initialize DeepSpeed
    model_engine, optimizer, _, lr_scheduler = deepspeed.initialize(
        model=transformer,
        model_parameters=transformer.parameters(),
        config=deepspeed_config,
    )
    device = get_accelerator().device_name(model_engine.local_rank)
    
    # Resume from checkpoint if available
    checkpoint = ray.train.get_checkpoint()
    start_epoch = 0
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            checkpoint_path = os.path.join(checkpoint_dir, "model")
            if os.path.exists(checkpoint_path):
                model_engine.load_checkpoint(checkpoint_path)
                # Extract epoch from metadata if available
                metadata_path = os.path.join(checkpoint_dir, "metadata.json")
                if os.path.exists(metadata_path):
                    with open(metadata_path) as f:
                        metadata = json.load(f)
                        start_epoch = metadata.get("epoch", 0) + 1
    
    # Training loop
    for epoch in range(start_epoch, training_config.num_epochs_stage1):
        model_engine.train()
        total_loss = 0.0
        num_batches = 0
        
        # Iterate through Ray Data batches
        for batch in train_ds.iter_torch_batches(
            batch_size=training_config.batch_size,
            collate_fn=lambda b: preprocess_batch(b, training_config.image_size),
        ):
            images = batch["images"].to(device, dtype=torch.bfloat16)
            masks = batch["masks"].to(device, dtype=torch.bfloat16)
            targets = batch["targets"].to(device, dtype=torch.bfloat16)
            
            # Forward pass: combine image with mask for inpainting
            masked_images = images * (1 - masks)
            
            # In practice, you would encode to latents and use the diffusion process
            # This is simplified for demonstration
            predictions = model_engine(masked_images.flatten(1))
            predictions = predictions.reshape_as(targets)
            
            # Compute loss (MSE for inpainting)
            loss = torch.nn.functional.mse_loss(predictions, targets)
            
            # Backward pass with DeepSpeed
            model_engine.backward(loss)
            model_engine.step()
            
            total_loss += loss.item()
            num_batches += 1
        
        avg_loss = total_loss / max(num_batches, 1)
        
        # Synchronize before checkpointing
        torch.distributed.barrier()
        
        # Save checkpoint
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save DeepSpeed checkpoint (each worker saves its shard)
            model_engine.save_checkpoint(tmpdir)
            
            # Save metadata on rank 0
            if model_engine.global_rank == 0:
                metadata = {"epoch": epoch, "stage": 1, "loss": avg_loss}
                with open(os.path.join(tmpdir, "metadata.json"), "w") as f:
                    json.dump(metadata, f)
            
            torch.distributed.barrier()
            
            checkpoint = Checkpoint.from_directory(tmpdir)
            ray.train.report(
                metrics={"loss": avg_loss, "epoch": epoch, "stage": 1},
                checkpoint=checkpoint,
            )
        
        if model_engine.global_rank == 0:
            print(f"Stage 1 - Epoch {epoch}: Loss = {avg_loss:.6f}")


def train_stage2(config: Dict[str, Any]):
    """
    Stage 2: LoRA adapter fine-tuning.
    
    The transformer base weights are frozen, only LoRA parameters are updated.
    """
    import deepspeed
    from deepspeed.accelerator import get_accelerator
    
    training_config = TrainingConfig(**config.get("training_config", {}))
    deepspeed_config = get_deepspeed_config(training_config, stage=2)
    
    # Get Ray Data shard for this worker
    train_ds = ray.train.get_dataset_shard("train")
    
    # Setup model with LoRA
    transformer = setup_flux_transformer(training_config, enable_lora=True)
    transformer = freeze_transformer_for_lora(transformer)
    
    # Load base model weights from Stage 1 checkpoint
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            # Load the Stage 1 checkpoint into the base model
            checkpoint_path = os.path.join(checkpoint_dir, "model")
            if os.path.exists(checkpoint_path):
                # Load state dict, filtering for base model weights
                state_dict = torch.load(
                    os.path.join(checkpoint_path, "pytorch_model.bin"),
                    map_location="cpu",
                )
                # Load only non-LoRA weights
                transformer_state = {
                    k: v for k, v in state_dict.items() 
                    if "lora" not in k.lower()
                }
                transformer.load_state_dict(transformer_state, strict=False)
    
    # Get only trainable (LoRA) parameters for DeepSpeed
    trainable_params = [p for p in transformer.parameters() if p.requires_grad]
    
    # Initialize DeepSpeed with only LoRA parameters
    model_engine, optimizer, _, lr_scheduler = deepspeed.initialize(
        model=transformer,
        model_parameters=trainable_params,
        config=deepspeed_config,
    )
    device = get_accelerator().device_name(model_engine.local_rank)
    
    # Training loop
    for epoch in range(training_config.num_epochs_stage2):
        model_engine.train()
        total_loss = 0.0
        num_batches = 0
        
        for batch in train_ds.iter_torch_batches(
            batch_size=training_config.batch_size,
            collate_fn=lambda b: preprocess_batch(b, training_config.image_size),
        ):
            images = batch["images"].to(device, dtype=torch.bfloat16)
            masks = batch["masks"].to(device, dtype=torch.bfloat16)
            targets = batch["targets"].to(device, dtype=torch.bfloat16)
            
            masked_images = images * (1 - masks)
            
            # Forward pass through LoRA-adapted model
            predictions = model_engine(masked_images.flatten(1))
            predictions = predictions.reshape_as(targets)
            
            loss = torch.nn.functional.mse_loss(predictions, targets)
            
            model_engine.backward(loss)
            model_engine.step()
            
            total_loss += loss.item()
            num_batches += 1
        
        avg_loss = total_loss / max(num_batches, 1)
        
        torch.distributed.barrier()
        
        # Save checkpoint (LoRA weights only)
        with tempfile.TemporaryDirectory() as tmpdir:
            model_engine.save_checkpoint(tmpdir)
            
            if model_engine.global_rank == 0:
                # Also save just the LoRA weights for easy loading
                lora_state = {
                    k: v for k, v in transformer.state_dict().items()
                    if "lora" in k.lower()
                }
                torch.save(lora_state, os.path.join(tmpdir, "lora_weights.pt"))
                
                metadata = {"epoch": epoch, "stage": 2, "loss": avg_loss}
                with open(os.path.join(tmpdir, "metadata.json"), "w") as f:
                    json.dump(metadata, f)
            
            torch.distributed.barrier()
            
            checkpoint = Checkpoint.from_directory(tmpdir)
            ray.train.report(
                metrics={"loss": avg_loss, "epoch": epoch, "stage": 2},
                checkpoint=checkpoint,
            )
        
        if model_engine.global_rank == 0:
            print(f"Stage 2 - Epoch {epoch}: Loss = {avg_loss:.6f}")


# =============================================================================
# Main Pipeline Orchestration
# =============================================================================

def run_two_stage_pipeline(
    jsonl_path: str,
    config: Optional[TrainingConfig] = None,
    num_workers: int = 4,
    use_gpu: bool = True,
) -> Dict[str, Any]:
    """
    Run the complete two-stage training pipeline.
    
    Args:
        jsonl_path: Path to the JSONL dataset file
        config: Training configuration
        num_workers: Number of distributed workers
        use_gpu: Whether to use GPUs
        
    Returns:
        Dictionary containing results from both stages
    """
    if config is None:
        config = TrainingConfig()
    
    # Initialize Ray if not already initialized
    if not ray.is_initialized():
        ray.init()
    
    # Create Ray Dataset from JSONL
    train_dataset = create_ray_dataset(jsonl_path)
    ray_datasets = {"train": train_dataset}
    
    # Configuration for Ray Train
    scaling_config = ScalingConfig(
        num_workers=num_workers,
        use_gpu=use_gpu,
        resources_per_worker={"GPU": 1} if use_gpu else {},
    )
    
    dataset_config = DataConfig(datasets_to_split=["train"])
    
    results = {"cycles": []}
    last_checkpoint = None
    
    # Run training cycles
    for cycle in range(config.num_cycles):
        print(f"\n{'='*60}")
        print(f"Starting Cycle {cycle + 1}/{config.num_cycles}")
        print(f"{'='*60}")
        
        cycle_result = {"cycle": cycle + 1}
        
        # ===== Stage 1: Full Transformer Training =====
        print(f"\n--- Stage 1: Full Transformer Fine-tuning ---")
        
        stage1_run_config = RunConfig(
            name=f"flux_stage1_cycle{cycle}",
            storage_path=config.output_dir,
        )
        
        stage1_trainer = TorchTrainer(
            train_loop_per_worker=train_stage1,
            train_loop_config={
                "training_config": config.__dict__,
            },
            scaling_config=scaling_config,
            run_config=stage1_run_config,
            datasets=ray_datasets,
            dataset_config=dataset_config,
            resume_from_checkpoint=last_checkpoint,
        )
        
        stage1_result = stage1_trainer.fit()
        cycle_result["stage1"] = {
            "metrics": stage1_result.metrics,
            "checkpoint": stage1_result.checkpoint,
        }
        last_checkpoint = stage1_result.checkpoint
        
        print(f"Stage 1 completed. Final loss: {stage1_result.metrics.get('loss', 'N/A')}")
        
        # ===== Stage 2: LoRA Adapter Training =====
        print(f"\n--- Stage 2: LoRA Adapter Fine-tuning ---")
        
        stage2_run_config = RunConfig(
            name=f"flux_stage2_cycle{cycle}",
            storage_path=config.output_dir,
        )
        
        stage2_trainer = TorchTrainer(
            train_loop_per_worker=train_stage2,
            train_loop_config={
                "training_config": config.__dict__,
            },
            scaling_config=scaling_config,
            run_config=stage2_run_config,
            datasets=ray_datasets,
            dataset_config=dataset_config,
            resume_from_checkpoint=last_checkpoint,
        )
        
        stage2_result = stage2_trainer.fit()
        cycle_result["stage2"] = {
            "metrics": stage2_result.metrics,
            "checkpoint": stage2_result.checkpoint,
        }
        last_checkpoint = stage2_result.checkpoint
        
        print(f"Stage 2 completed. Final loss: {stage2_result.metrics.get('loss', 'N/A')}")
        
        results["cycles"].append(cycle_result)
    
    results["final_checkpoint"] = last_checkpoint
    return results


# =============================================================================
# Alternative: Single Script with Stage Flag
# =============================================================================

def train_flux_single_stage(config: Dict[str, Any]):
    """
    Single training function that handles both stages based on config.
    
    This is useful when you want to run stages separately via CLI flags:
    - Stage 1: train_transformer=True, lora_enabled=False
    - Stage 2: train_transformer=False, lora_enabled=True
    """
    training_config = TrainingConfig(**config.get("training_config", {}))
    train_transformer = config.get("train_transformer", True)
    lora_enabled = config.get("lora_enabled", False)
    
    if train_transformer and not lora_enabled:
        # Stage 1: Full transformer training
        train_stage1(config)
    elif not train_transformer and lora_enabled:
        # Stage 2: LoRA only training
        train_stage2(config)
    else:
        raise ValueError(
            "Invalid configuration: "
            "Stage 1 requires train_transformer=True, lora_enabled=False; "
            "Stage 2 requires train_transformer=False, lora_enabled=True"
        )


def run_single_stage(
    jsonl_path: str,
    stage: int,
    config: Optional[TrainingConfig] = None,
    resume_from: Optional[str] = None,
    num_workers: int = 4,
    use_gpu: bool = True,
):
    """
    Run a single training stage.
    
    Args:
        jsonl_path: Path to JSONL dataset
        stage: 1 for full transformer, 2 for LoRA
        config: Training configuration
        resume_from: Path to checkpoint to resume from
        num_workers: Number of workers
        use_gpu: Use GPUs
    """
    if config is None:
        config = TrainingConfig()
    
    if not ray.is_initialized():
        ray.init()
    
    train_dataset = create_ray_dataset(jsonl_path)
    
    train_fn = train_stage1 if stage == 1 else train_stage2
    stage_name = "transformer" if stage == 1 else "lora"
    
    scaling_config = ScalingConfig(
        num_workers=num_workers,
        use_gpu=use_gpu,
    )
    
    run_config = RunConfig(
        name=f"flux_fill_{stage_name}",
        storage_path=config.output_dir,
    )
    
    resume_checkpoint = None
    if resume_from:
        resume_checkpoint = Checkpoint.from_directory(resume_from)
    
    trainer = TorchTrainer(
        train_loop_per_worker=train_fn,
        train_loop_config={"training_config": config.__dict__},
        scaling_config=scaling_config,
        run_config=run_config,
        datasets={"train": train_dataset},
        dataset_config=DataConfig(datasets_to_split=["train"]),
        resume_from_checkpoint=resume_checkpoint,
    )
    
    result = trainer.fit()
    print(f"Stage {stage} completed!")
    print(f"Final metrics: {result.metrics}")
    print(f"Checkpoint saved to: {result.checkpoint}")
    
    return result


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Two-Stage Flux Transformer + LoRA Training with Ray"
    )
    parser.add_argument(
        "--jsonl-path",
        type=str,
        required=True,
        help="Path to JSONL dataset file",
    )
    parser.add_argument(
        "--stage",
        type=int,
        choices=[0, 1, 2],
        default=0,
        help="Stage to run: 0=both (cycling), 1=transformer only, 2=lora only",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=4,
        help="Number of distributed workers",
    )
    parser.add_argument(
        "--num-cycles",
        type=int,
        default=2,
        help="Number of Stage1->Stage2 cycles (only for stage=0)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs/flux_fill_training",
        help="Output directory for checkpoints",
    )
    parser.add_argument(
        "--resume-from",
        type=str,
        default=None,
        help="Path to checkpoint to resume from",
    )
    parser.add_argument(
        "--no-gpu",
        action="store_true",
        help="Disable GPU usage",
    )
    
    args = parser.parse_args()
    
    config = TrainingConfig(
        output_dir=args.output_dir,
        num_cycles=args.num_cycles,
    )
    
    if args.stage == 0:
        # Run full two-stage pipeline with cycling
        results = run_two_stage_pipeline(
            jsonl_path=args.jsonl_path,
            config=config,
            num_workers=args.num_workers,
            use_gpu=not args.no_gpu,
        )
        print("\n" + "="*60)
        print("Training Complete!")
        print("="*60)
        print(f"Final checkpoint: {results['final_checkpoint']}")
        
    else:
        # Run single stage
        result = run_single_stage(
            jsonl_path=args.jsonl_path,
            stage=args.stage,
            config=config,
            resume_from=args.resume_from,
            num_workers=args.num_workers,
            use_gpu=not args.no_gpu,
        )
