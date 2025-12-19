"""
Simplified Two-Stage Training Example: Ray Data + Ray Train
============================================================

This is a minimal example showing the core Ray patterns for the
Flux Transformer + LoRA two-stage training pipeline.

Key concepts demonstrated:
1. Loading JSONL data with ray.data.read_json()
2. Using ray.train.get_dataset_shard() for distributed data
3. Using iter_torch_batches() for efficient data iteration
4. TorchTrainer for distributed training
5. Checkpoint management between stages
"""

import os
import tempfile
from typing import Any, Dict

import torch
import torch.nn as nn

import ray
import ray.train
from ray.train import Checkpoint, DataConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer


# =============================================================================
# Step 1: Define Your Model (simplified for demonstration)
# =============================================================================

class SimpleFluxTransformer(nn.Module):
    """Mock transformer for demonstration."""
    
    def __init__(self, hidden_dim: int = 512):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim * 2),
            nn.GELU(),
            nn.Linear(hidden_dim * 2, hidden_dim),
        )
        self.decoder = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim * 2),
            nn.GELU(),
            nn.Linear(hidden_dim * 2, hidden_dim),
        )
    
    def forward(self, x, mask=None):
        encoded = self.encoder(x)
        if mask is not None:
            encoded = encoded * (1 - mask)
        return self.decoder(encoded)


class LoRALayer(nn.Module):
    """Simple LoRA adapter layer."""
    
    def __init__(self, in_features: int, out_features: int, rank: int = 64, alpha: int = 96):
        super().__init__()
        self.lora_A = nn.Parameter(torch.zeros(in_features, rank))
        self.lora_B = nn.Parameter(torch.zeros(rank, out_features))
        self.scale = alpha / rank
        nn.init.kaiming_uniform_(self.lora_A)
        nn.init.zeros_(self.lora_B)
    
    def forward(self, x):
        return (x @ self.lora_A @ self.lora_B) * self.scale


class FluxWithLoRA(nn.Module):
    """Transformer with LoRA adapters attached."""
    
    def __init__(self, base_model: SimpleFluxTransformer, rank: int = 64):
        super().__init__()
        self.base_model = base_model
        
        # Freeze base model
        for param in self.base_model.parameters():
            param.requires_grad = False
        
        # Add LoRA adapters
        self.lora_encoder = LoRALayer(512, 1024, rank=rank)
        self.lora_decoder = LoRALayer(512, 1024, rank=rank)
    
    def forward(self, x, mask=None):
        # Base model forward (frozen)
        with torch.no_grad():
            encoded = self.base_model.encoder(x)
        
        # Add LoRA contribution (trainable)
        lora_out = self.lora_encoder(x)
        encoded = encoded + lora_out[:, :encoded.shape[1]]
        
        if mask is not None:
            encoded = encoded * (1 - mask)
        
        with torch.no_grad():
            decoded = self.base_model.decoder(encoded)
        
        lora_out2 = self.lora_decoder(encoded)
        decoded = decoded + lora_out2[:, :decoded.shape[1]]
        
        return decoded


# =============================================================================
# Step 2: Data Processing with Ray Data
# =============================================================================

def collate_batch(batch: Dict[str, Any]) -> Dict[str, torch.Tensor]:
    """
    Collate function for Ray Data batches.
    
    In practice, this would load images from paths in the JSONL.
    Here we generate synthetic data for demonstration.
    """
    batch_size = len(batch["user_input"])
    
    # Synthetic data matching the JSONL structure
    # In real use, you would load images from batch["user_input"]["image"], etc.
    images = torch.randn(batch_size, 512)  # [B, hidden_dim]
    masks = torch.zeros(batch_size, 512)   # [B, hidden_dim]
    targets = torch.randn(batch_size, 512) # [B, hidden_dim]
    
    return {
        "images": images,
        "masks": masks, 
        "targets": targets,
    }


# =============================================================================
# Step 3: Stage 1 Training Function - Full Transformer
# =============================================================================

def train_stage1_fn(config: Dict[str, Any]):
    """
    Stage 1: Train the full transformer.
    
    Key Ray patterns:
    - ray.train.get_dataset_shard("train") - get this worker's data partition
    - dataset.iter_torch_batches() - iterate with automatic batching
    - ray.train.report() - report metrics and checkpoints
    """
    
    lr = config.get("learning_rate", 1e-4)
    num_epochs = config.get("num_epochs", 3)
    batch_size = config.get("batch_size", 4)
    
    # ===== Get distributed data shard =====
    train_ds = ray.train.get_dataset_shard("train")
    
    # ===== Setup model =====
    model = SimpleFluxTransformer(hidden_dim=512)
    
    # Wrap model for distributed training
    model = ray.train.torch.prepare_model(model)
    
    optimizer = torch.optim.AdamW(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()
    
    device = ray.train.torch.get_device()
    
    # ===== Training loop =====
    for epoch in range(num_epochs):
        model.train()
        total_loss = 0.0
        num_batches = 0
        
        # Iterate through Ray Data with automatic batching
        for batch in train_ds.iter_torch_batches(
            batch_size=batch_size,
            collate_fn=collate_batch,
        ):
            images = batch["images"].to(device)
            masks = batch["masks"].to(device)
            targets = batch["targets"].to(device)
            
            optimizer.zero_grad()
            
            # Forward pass
            predictions = model(images, masks)
            loss = loss_fn(predictions, targets)
            
            # Backward pass
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
            num_batches += 1
        
        avg_loss = total_loss / max(num_batches, 1)
        
        # ===== Save checkpoint =====
        with tempfile.TemporaryDirectory() as tmpdir:
            # Get base model for saving (unwrap DDP if needed)
            base_model = model.module if hasattr(model, "module") else model
            torch.save(base_model.state_dict(), os.path.join(tmpdir, "model.pt"))
            
            checkpoint = Checkpoint.from_directory(tmpdir)
            
            # Report metrics and checkpoint to Ray Train
            ray.train.report(
                metrics={"loss": avg_loss, "epoch": epoch, "stage": 1},
                checkpoint=checkpoint,
            )
        
        print(f"[Stage 1] Epoch {epoch}: Loss = {avg_loss:.6f}")


# =============================================================================
# Step 4: Stage 2 Training Function - LoRA Only
# =============================================================================

def train_stage2_fn(config: Dict[str, Any]):
    """
    Stage 2: Train only the LoRA adapters.
    
    Key patterns:
    - ray.train.get_checkpoint() - resume from previous stage
    - Freeze base model, train only LoRA parameters
    """
    
    lr = config.get("learning_rate", 1e-3)  # Higher LR for LoRA
    num_epochs = config.get("num_epochs", 5)
    batch_size = config.get("batch_size", 4)
    lora_rank = config.get("lora_rank", 64)
    
    train_ds = ray.train.get_dataset_shard("train")
    
    # ===== Load base model from Stage 1 checkpoint =====
    base_model = SimpleFluxTransformer(hidden_dim=512)
    
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            model_path = os.path.join(checkpoint_dir, "model.pt")
            if os.path.exists(model_path):
                state_dict = torch.load(model_path, map_location="cpu")
                base_model.load_state_dict(state_dict)
                print("Loaded base model from Stage 1 checkpoint")
    
    # ===== Wrap with LoRA (base model frozen) =====
    model = FluxWithLoRA(base_model, rank=lora_rank)
    model = ray.train.torch.prepare_model(model)
    
    # Only optimize LoRA parameters
    lora_params = [p for n, p in model.named_parameters() if "lora" in n.lower()]
    optimizer = torch.optim.AdamW(lora_params, lr=lr)
    loss_fn = nn.MSELoss()
    
    device = ray.train.torch.get_device()
    
    for epoch in range(num_epochs):
        model.train()
        total_loss = 0.0
        num_batches = 0
        
        for batch in train_ds.iter_torch_batches(
            batch_size=batch_size,
            collate_fn=collate_batch,
        ):
            images = batch["images"].to(device)
            masks = batch["masks"].to(device)
            targets = batch["targets"].to(device)
            
            optimizer.zero_grad()
            predictions = model(images, masks)
            loss = loss_fn(predictions, targets)
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
            num_batches += 1
        
        avg_loss = total_loss / max(num_batches, 1)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save both base model and LoRA weights
            base_model_unwrapped = model.module if hasattr(model, "module") else model
            
            # Save base model
            torch.save(
                base_model_unwrapped.base_model.state_dict(),
                os.path.join(tmpdir, "model.pt")
            )
            
            # Save LoRA weights separately
            lora_state = {
                k: v for k, v in base_model_unwrapped.state_dict().items()
                if "lora" in k.lower()
            }
            torch.save(lora_state, os.path.join(tmpdir, "lora_weights.pt"))
            
            checkpoint = Checkpoint.from_directory(tmpdir)
            ray.train.report(
                metrics={"loss": avg_loss, "epoch": epoch, "stage": 2},
                checkpoint=checkpoint,
            )
        
        print(f"[Stage 2] Epoch {epoch}: Loss = {avg_loss:.6f}")


# =============================================================================
# Step 5: Main Pipeline - Orchestrating Two Stages
# =============================================================================

def run_pipeline(jsonl_path: str, num_workers: int = 2, use_gpu: bool = False):
    """
    Run the complete two-stage pipeline.
    
    This demonstrates:
    1. Creating a Ray Dataset from JSONL
    2. Running Stage 1 with TorchTrainer
    3. Passing checkpoint to Stage 2
    4. Running Stage 2 with frozen base + LoRA
    """
    
    # Initialize Ray
    if not ray.is_initialized():
        ray.init()
    
    # ===== Create Ray Dataset from JSONL =====
    # ray.data.read_json supports JSONL files directly
    train_dataset = ray.data.read_json(jsonl_path, lines=True)
    
    print(f"Dataset schema: {train_dataset.schema()}")
    print(f"Dataset count: {train_dataset.count()}")
    
    # ===== Configure distributed training =====
    scaling_config = ScalingConfig(
        num_workers=num_workers,
        use_gpu=use_gpu,
    )
    
    dataset_config = DataConfig(
        datasets_to_split=["train"],  # Split data across workers
    )
    
    # ===== Stage 1: Full Transformer Training =====
    print("\n" + "="*50)
    print("STAGE 1: Full Transformer Fine-tuning")
    print("="*50 + "\n")
    
    stage1_trainer = TorchTrainer(
        train_loop_per_worker=train_stage1_fn,
        train_loop_config={
            "learning_rate": 1e-4,
            "num_epochs": 3,
            "batch_size": 4,
        },
        scaling_config=scaling_config,
        run_config=RunConfig(name="flux_stage1"),
        datasets={"train": train_dataset},
        dataset_config=dataset_config,
    )
    
    stage1_result = stage1_trainer.fit()
    print(f"\nStage 1 complete! Final loss: {stage1_result.metrics['loss']:.6f}")
    
    # ===== Stage 2: LoRA Training =====
    # Pass the Stage 1 checkpoint to Stage 2
    print("\n" + "="*50)
    print("STAGE 2: LoRA Adapter Fine-tuning")
    print("="*50 + "\n")
    
    stage2_trainer = TorchTrainer(
        train_loop_per_worker=train_stage2_fn,
        train_loop_config={
            "learning_rate": 1e-3,
            "num_epochs": 5,
            "batch_size": 4,
            "lora_rank": 64,
        },
        scaling_config=scaling_config,
        run_config=RunConfig(name="flux_stage2"),
        datasets={"train": train_dataset},
        dataset_config=dataset_config,
        # Pass Stage 1 checkpoint to resume from
        resume_from_checkpoint=stage1_result.checkpoint,
    )
    
    stage2_result = stage2_trainer.fit()
    print(f"\nStage 2 complete! Final loss: {stage2_result.metrics['loss']:.6f}")
    
    return {
        "stage1_result": stage1_result,
        "stage2_result": stage2_result,
    }


# =============================================================================
# Example: Creating Sample Data
# =============================================================================

def create_sample_jsonl(output_path: str, num_samples: int = 100):
    """Create a sample JSONL file for testing."""
    import json
    
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    
    with open(output_path, "w") as f:
        for i in range(num_samples):
            sample = {
                "user_input": {
                    "image": f"/data/images/train_{i:06d}.png",
                    "mask": f"/data/masks/train_{i:06d}.png",
                },
                "output": f"/data/targets/train_{i:06d}.png",
            }
            f.write(json.dumps(sample) + "\n")
    
    print(f"Created sample JSONL with {num_samples} entries at {output_path}")


# =============================================================================
# Run Example
# =============================================================================

if __name__ == "__main__":
    # Create sample data
    sample_jsonl = "/tmp/flux_training_data.jsonl"
    create_sample_jsonl(sample_jsonl, num_samples=50)
    
    # Run the two-stage pipeline
    results = run_pipeline(
        jsonl_path=sample_jsonl,
        num_workers=2,
        use_gpu=False,  # Set to True if you have GPUs
    )
    
    print("\n" + "="*50)
    print("PIPELINE COMPLETE")
    print("="*50)
    print(f"Stage 1 checkpoint: {results['stage1_result'].checkpoint}")
    print(f"Stage 2 checkpoint: {results['stage2_result'].checkpoint}")
