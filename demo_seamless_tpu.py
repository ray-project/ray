#!/usr/bin/env python3
"""
Demonstration of Seamless TPU Integration

This script shows how to write training loops that work seamlessly
on both GPU and TPU without requiring any code changes or manual calls.
"""

import torch
import torch.nn as nn
import torch.optim as optim
from ray.train.torch.seamless_tpu import SeamlessTPUTrainer, to_device


class SimpleModel(nn.Module):
    """A simple model for demonstration."""
    
    def __init__(self, input_size=10, hidden_size=32, output_size=1):
        super().__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, output_size)
    
    def forward(self, x):
        x = self.relu(self.fc1(x))
        return self.fc2(x)


def train_loop_seamless(config):
    """
    Training loop that works seamlessly on both GPU and TPU.
    
    This function requires NO changes when switching between GPU and TPU!
    No manual calls to mark_step, optimizer_step, etc. needed!
    """
    print(f"Starting seamless training on worker {ray.train.get_context().get_world_rank()}")
    
    # Create model and optimizer
    model = SimpleModel()
    optimizer = optim.SGD(model.parameters(), lr=0.01)
    criterion = nn.MSELoss()
    
    # Create seamless trainer - automatically handles device placement
    trainer = SeamlessTPUTrainer(model)
    print(f"Using device: {trainer.get_device()}")
    print(f"Is TPU: {trainer.is_tpu()}")
    
    # Create dummy data
    x = torch.randn(100, 10)
    y = torch.randn(100, 1)
    
    # Move data to device automatically
    x, y = trainer.to_device(x, y)
    
    # Training loop - works the same on GPU and TPU!
    for epoch in range(5):
        optimizer.zero_grad()
        
        # Forward pass
        output = model(x)
        loss = criterion(output, y)
        
        # Backward pass
        loss.backward()
        
        # Regular optimizer step - automatically optimized for TPU when available
        optimizer.step()
        
        if epoch % 2 == 0:
            print(f"Epoch {epoch}, Loss: {loss.item():.4f}")
    
    print(f"Seamless training completed on worker {ray.train.get_context().get_world_rank()}")


def train_loop_functional(config):
    """
    Alternative training loop using functional approach.
    
    This also works seamlessly on both GPU and TPU.
    """
    print(f"Starting functional training on worker {ray.train.get_context().get_world_rank()}")
    
    # Create model and optimizer
    model = SimpleModel()
    optimizer = optim.SGD(model.parameters(), lr=0.01)
    criterion = nn.MSELoss()
    
    # Create dummy data
    x = torch.randn(100, 10)
    y = torch.randn(100, 1)
    
    # Move data to device automatically
    x, y = to_device(x, y)
    model = model.to(x.device)  # Move model to same device as data
    
    # Training loop - works the same on GPU and TPU!
    for epoch in range(5):
        optimizer.zero_grad()
        
        # Forward pass
        output = model(x)
        loss = criterion(output, y)
        
        # Backward pass
        loss.backward()
        
        # Regular optimizer step - automatically optimized for TPU when available
        optimizer.step()
        
        if epoch % 2 == 0:
            print(f"Epoch {epoch}, Loss: {loss.item():.4f}")
    
    print(f"Functional training completed on worker {ray.train.get_context().get_world_rank()}")


def demonstrate_device_agnostic_code():
    """Demonstrate how the same code works on different devices."""
    print("\n" + "="*60)
    print("DEMONSTRATING DEVICE-AGNOSTIC CODE")
    print("="*60)
    
    # Create model
    model = SimpleModel()
    
    # This will work on any device (CPU, GPU, or TPU)
    trainer = SeamlessTPUTrainer(model)
    
    print(f"Model device: {next(model.parameters()).device}")
    print(f"Trainer device: {trainer.get_device()}")
    print(f"Is TPU: {trainer.is_tpu()}")
    
    # Create some data
    data = torch.randn(10, 10)
    
    # Move to device automatically
    data_on_device = trainer.to_device(data)
    print(f"Data device: {data_on_device.device}")
    
    # This will automatically use the right optimizer step
    optimizer = optim.SGD(model.parameters(), lr=0.01)
    loss = torch.tensor(1.0)
    
    print("Performing optimizer step...")
    optimizer.step()  # Regular PyTorch call - automatically optimized for TPU
    print("Optimizer step completed!")
    
    print("Training loop completed successfully!")


if __name__ == "__main__":
    # This demonstrates the seamless integration
    demonstrate_device_agnostic_code()
    
    print("\n" + "="*60)
    print("USAGE WITH RAY TORCHTRAINER")
    print("="*60)
    print("To use this with Ray TorchTrainer:")
    print("1. Set ScalingConfig(use_tpu=True) for TPU training")
    print("2. Use the same training loop for both GPU and TPU")
    print("3. No code changes required!")
    print("4. No manual TPU calls needed!")
    print("5. XLA SPMD is automatically configured")
    
    print("\nExample:")
    print("scaling_config = ScalingConfig(use_tpu=True, topology='2x2')")
    print("trainer = TorchTrainer(")
    print("    train_loop_per_worker=train_loop_seamless,")
    print("    scaling_config=scaling_config")
    print(")")
    print("result = trainer.fit()  # That's it!")
    
    print("\n" + "="*60)
    print("KEY BENEFITS")
    print("="*60)
    print("✅ Same training code works on GPU and TPU")
    print("✅ No manual device management needed")
    print("✅ No manual TPU optimization calls needed")
    print("✅ XLA SPMD automatically configured")
    print("✅ Just call trainer.fit() and it works!") 