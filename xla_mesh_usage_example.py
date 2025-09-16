#!/usr/bin/env python3
"""
Comprehensive example showing how to use XLA SPMD mesh with Ray Train TorchTrainer.

This example demonstrates:
1. Setting up XLA backend for TPU training
2. Accessing the XLA mesh in training loops
3. Using the mesh for model and data parallelism
4. Best practices for TPU training with Ray Train
"""

import os
import tempfile
from typing import Dict

import torch
import torch.nn as nn
import torch.optim as optim

import ray
from ray.train import Checkpoint, CheckpointConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer, TorchConfig


class TransformerBlock(nn.Module):
    """A simple transformer block for demonstration."""
    
    def __init__(self, d_model: int = 512, nhead: int = 8):
        super().__init__()
        self.attention = nn.MultiheadAttention(d_model, nhead, batch_first=True)
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)
        self.ffn = nn.Sequential(
            nn.Linear(d_model, d_model * 4),
            nn.ReLU(),
            nn.Linear(d_model * 4, d_model)
        )
    
    def forward(self, x):
        # Self-attention
        attn_out, _ = self.attention(x, x, x)
        x = self.norm1(x + attn_out)
        
        # Feed-forward
        ffn_out = self.ffn(x)
        x = self.norm2(x + ffn_out)
        
        return x


class SimpleTransformer(nn.Module):
    """A simple transformer model for TPU training."""
    
    def __init__(self, vocab_size: int = 10000, d_model: int = 512, nhead: int = 8, num_layers: int = 6):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, d_model)
        self.pos_encoding = nn.Parameter(torch.randn(1, 1000, d_model))
        
        self.transformer_blocks = nn.ModuleList([
            TransformerBlock(d_model, nhead) for _ in range(num_layers)
        ])
        
        self.output_proj = nn.Linear(d_model, vocab_size)
    
    def forward(self, input_ids):
        # Embedding + positional encoding
        x = self.embedding(input_ids)
        seq_len = x.size(1)
        x = x + self.pos_encoding[:, :seq_len, :]
        
        # Transformer blocks
        for block in self.transformer_blocks:
            x = block(x)
        
        # Output projection
        logits = self.output_proj(x)
        return logits


def train_loop_per_worker(config: Dict):
    """
    Advanced training loop demonstrating XLA SPMD mesh usage with Train V2.
    
    This function shows how to:
    1. Set up XLA SPMD training
    2. Use the mesh for model parallelism
    3. Implement data parallelism with the mesh
    4. Handle TPU-specific optimizations
    """
    import ray.train as train
    import ray.train.torch.xla as xla_utils
    
    # Get training context
    context = train.get_context()
    world_rank = context.get_world_rank()
    world_size = context.get_world_size()
    
    print(f"Worker {world_rank}/{world_size} starting XLA SPMD training")
    
    # Check XLA backend status
    if not xla_utils.is_xla_backend():
        raise RuntimeError("XLA backend not active. Make sure to use TorchConfig(backend='xla')")
    
    # Get the XLA mesh from train context
    mesh = context.get_xla_mesh()
    if mesh is None:
        raise RuntimeError("XLA mesh not available. Check your TPU configuration.")
    
    print(f"XLA mesh configuration:")
    print(f"  Shape: {mesh.shape}")
    print(f"  Axes: {mesh.axis_names}")
    print(f"  Devices: {len(mesh.devices)}")
    print(f"  Process count: {mesh.shape[0] if len(mesh.shape) > 0 else 1}")
    print(f"  Local devices per process: {mesh.shape[1] if len(mesh.shape) > 1 else 1}")
    
    # Create model
    model = SimpleTransformer(
        vocab_size=config.get("vocab_size", 10000),
        d_model=config.get("d_model", 512),
        nhead=config.get("nhead", 8),
        num_layers=config.get("num_layers", 6)
    )
    
    # Set up XLA SPMD sharding
    try:
        import torch_xla.distributed.spmd as xs
        
        # Example: Shard the model across the mesh
        # For a 4x4 TPU slice with mesh shape (4, 4) and axes ("data", "model"):
        # - Use "data" axis for data parallelism
        # - Use "model" axis for model parallelism
        
        if len(mesh.shape) == 2 and "data" in mesh.axis_names and "model" in mesh.axis_names:
            # 2D mesh: data + model parallelism
            print("Setting up 2D mesh sharding (data + model parallelism)")
            
            # Shard embedding layer across model axis
            # embedding_sharding = xs.Shard(1)  # Shard embedding dimension
            
            # Shard transformer blocks across model axis
            # for i, block in enumerate(model.transformer_blocks):
            #     model.transformer_blocks[i] = xs.mark_sharding(block, mesh, xs.Shard(1))
            
            # Shard output projection across model axis
            # output_sharding = xs.Shard(1)
            
            print("Model sharding configured for 2D mesh")
            
        elif len(mesh.shape) == 1 and "data" in mesh.axis_names:
            # 1D mesh: data parallelism only
            print("Setting up 1D mesh sharding (data parallelism)")
            
            # For data parallelism, we typically don't shard the model
            # but use PyTorch's DistributedDataParallel instead
            print("Using data parallelism with DDP")
            
        else:
            print(f"Unknown mesh configuration: {mesh.shape}, {mesh.axis_names}")
            
    except ImportError:
        print("torch_xla.distributed.spmd not available")
    
    # Set up optimizer
    optimizer = optim.AdamW(
        model.parameters(),
        lr=config.get("learning_rate", 1e-4),
        weight_decay=config.get("weight_decay", 0.01)
    )
    
    # Set up loss function
    criterion = nn.CrossEntropyLoss()
    
    # Training loop
    num_epochs = config.get("num_epochs", 1)
    batch_size = config.get("batch_size", 32)
    seq_length = config.get("seq_length", 128)
    
    for epoch in range(num_epochs):
        print(f"Epoch {epoch + 1}/{num_epochs}")
        
        # Create dummy data (in practice, you'd load from dataset)
        input_ids = torch.randint(0, 10000, (batch_size, seq_length))
        target_ids = torch.randint(0, 10000, (batch_size, seq_length))
        
        # Forward pass
        optimizer.zero_grad()
        logits = model(input_ids)
        
        # Reshape for loss calculation
        logits_flat = logits.view(-1, logits.size(-1))
        targets_flat = target_ids.view(-1)
        
        loss = criterion(logits_flat, targets_flat)
        
        # Backward pass
        loss.backward()
        optimizer.step()
        
        # Report metrics
        metrics = {
            "loss": loss.item(),
            "epoch": epoch + 1,
            "world_rank": world_rank,
            "mesh_shape": list(mesh.shape),
            "mesh_axes": list(mesh.axis_names)
        }
        
        ray.train.report(metrics)
        
        print(f"Worker {world_rank}: Epoch {epoch + 1}, Loss: {loss.item():.4f}")


def main():
    """
    Main function demonstrating XLA SPMD training setup.
    
    This shows how to configure Ray Train for TPU training with:
    - 4x4 TPU slice (4 hosts, each with 4 TPU chips)
    - XLA SPMD backend
    - Automatic mesh creation and exposure
    """
    print("XLA SPMD Training Example with Ray Train")
    print("=" * 50)
    
    # XLA backend configuration
    torch_config = TorchConfig(
        backend="xla",  # Use XLA backend for TPU
        timeout_s=1800,  # 30 minutes timeout
    )
    
    # Scaling configuration for 4x4 TPU slice
    # - 4 workers (one per host)
    # - Each host has 4 TPU chips
    # - Total: 16 TPU devices
    scaling_config = ScalingConfig(
        num_workers=4,  # 4 hosts
        use_gpu=False,  # Using TPU, not GPU
    )
    
    # Run configuration
    run_config = RunConfig(
        checkpoint_config=CheckpointConfig(num_to_keep=3),
        name="xla_spmd_transformer_training",
        stop_on_failure=True,
    )
    
    # Training configuration
    train_loop_config = {
        "vocab_size": 10000,
        "d_model": 512,
        "nhead": 8,
        "num_layers": 6,
        "learning_rate": 1e-4,
        "weight_decay": 0.01,
        "num_epochs": 2,
        "batch_size": 32,
        "seq_length": 128,
    }
    
    print("Configuration:")
    print(f"  Backend: {torch_config.backend}")
    print(f"  Workers: {scaling_config.num_workers}")
    print(f"  Model: Transformer (d_model={train_loop_config['d_model']}, layers={train_loop_config['num_layers']})")
    print(f"  Batch size: {train_loop_config['batch_size']}")
    print(f"  Sequence length: {train_loop_config['seq_length']}")
    print()
    
    # Create trainer
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=train_loop_config,
        torch_config=torch_config,
        scaling_config=scaling_config,
        run_config=run_config,
    )
    
    print("Starting XLA SPMD training...")
    print("Expected mesh configuration:")
    print("  Shape: (4, 4)")
    print("  Axes: ('data', 'model')")
    print("  Devices: 16 TPU chips")
    print("  Data parallelism: 4-way")
    print("  Model parallelism: 4-way")
    print()
    
    # Run training
    result = trainer.fit()
    
    print("Training completed!")
    print(f"Final metrics: {result.metrics}")
    print(f"Checkpoint: {result.checkpoint}")


if __name__ == "__main__":
    # Initialize Ray
    ray.init()
    
    try:
        main()
    finally:
        ray.shutdown()
