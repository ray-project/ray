"""Shared utilities for JIT checkpoint testing with subprocess-based SIGTERM tests."""

import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional


def create_training_script(
    storage_path: str,
    enable_jit: bool = True,
    kill_wait: float = 2.0,
    num_workers: int = 1,
    num_epochs: int = 20,
    checkpoint_freq: Optional[int] = None,
    trainer_type: str = "DataParallelTrainer",
) -> str:
    """Generate training script that can be run in subprocess.

    Args:
        storage_path: Path where checkpoints will be stored
        enable_jit: Whether to enable JIT checkpointing
        kill_wait: Kill wait time for JIT checkpointing
        num_workers: Number of training workers
        num_epochs: Total number of training epochs
        checkpoint_freq: Regular checkpoint frequency (None for no regular checkpoints)
        trainer_type: Type of trainer to use ("DataParallelTrainer" or "TorchTrainer")

    Returns:
        Python script as string that can be executed in subprocess
    """

    if trainer_type == "TorchTrainer":
        return _create_torch_trainer_script(
            storage_path,
            enable_jit,
            kill_wait,
            num_workers,
            num_epochs,
            checkpoint_freq,
        )
    else:
        return _create_data_parallel_trainer_script(
            storage_path,
            enable_jit,
            kill_wait,
            num_workers,
            num_epochs,
            checkpoint_freq,
        )


def _create_data_parallel_trainer_script(
    storage_path: str,
    enable_jit: bool,
    kill_wait: float,
    num_workers: int,
    num_epochs: int,
    checkpoint_freq: Optional[int],
) -> str:
    """Create DataParallelTrainer script."""

    checkpoint_config = ""
    if checkpoint_freq:
        checkpoint_config = f"""
        checkpoint_config=CheckpointConfig(
            checkpoint_frequency={checkpoint_freq},
            num_to_keep=3
        ),"""

    jit_config = ""
    if enable_jit:
        jit_config = f"""
        jit_checkpoint_config=JITCheckpointConfig(
            enabled=True,
            kill_wait={kill_wait}
        ),"""

    script = f'''
import os
import sys
import time
import tempfile
import torch
import torch.nn as nn
import torch.nn.functional as F

import ray
from ray import train
from ray.train import Checkpoint, RunConfig, ScalingConfig, CheckpointConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train._internal.jit_checkpoint_config import JITCheckpointConfig

def train_func(config):
    """Training function that saves state and can resume."""
    rank = train.get_context().get_world_rank()

    # Create simple model
    model = nn.Linear(10, 1)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

    # Load from checkpoint if resuming
    start_epoch = 0
    checkpoint = train.get_checkpoint()

    if checkpoint:
        print(f"[Rank {{rank}}] Loading checkpoint...")
        with checkpoint.as_directory() as checkpoint_dir:
            model_path = os.path.join(checkpoint_dir, "model.pt")
            if os.path.exists(model_path):
                model.load_state_dict(torch.load(model_path))

            epoch_path = os.path.join(checkpoint_dir, "epoch.pt")
            if os.path.exists(epoch_path):
                start_epoch = torch.load(epoch_path) + 1
                print(f"[Rank {{rank}}] Resuming from epoch {{start_epoch}}")

    # Training loop
    for epoch in range(start_epoch, {num_epochs}):
        # Train for a few steps
        for step in range(10):
            x = torch.randn(32, 10)
            y = torch.randn(32, 1)

            optimizer.zero_grad()
            output = model(x)
            loss = F.mse_loss(output, y)
            loss.backward()
            optimizer.step()

        # Save checkpoint
        with tempfile.TemporaryDirectory() as temp_dir:
            torch.save(model.state_dict(), os.path.join(temp_dir, "model.pt"))
            torch.save(epoch, os.path.join(temp_dir, "epoch.pt"))

            checkpoint = Checkpoint.from_directory(temp_dir)

            if rank == 0:
                train.report({{"epoch": epoch, "loss": loss.item()}}, checkpoint=checkpoint)
            else:
                train.report({{"epoch": epoch, "loss": loss.item()}})

        print(f"[Rank {{rank}}] Completed epoch {{epoch}}, loss={{loss.item():.4f}}")

# Initialize Ray
ray.init(num_cpus={num_workers * 2}, ignore_reinit_error=True)

# Create trainer
trainer = DataParallelTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers={num_workers}),
    run_config=RunConfig(
        name="jit_test",
        storage_path="{storage_path}",{checkpoint_config}{jit_config}
    )
)

print("Starting training...")
result = trainer.fit()

print(f"Training completed. Final metrics: {{result.metrics}}")

ray.shutdown()
'''
    return script


def _create_torch_trainer_script(
    storage_path: str,
    enable_jit: bool,
    kill_wait: float,
    num_workers: int,
    num_epochs: int,
    checkpoint_freq: Optional[int],
) -> str:
    """Create TorchTrainer script."""

    checkpoint_config = ""
    if checkpoint_freq:
        checkpoint_config = f"""
        checkpoint_config=CheckpointConfig(
            checkpoint_frequency={checkpoint_freq},
            num_to_keep=3
        ),"""

    jit_config = ""
    if enable_jit:
        jit_config = f"""
        jit_checkpoint_config=JITCheckpointConfig(
            enabled=True,
            kill_wait={kill_wait}
        ),"""

    script = f'''
import os
import sys
import time
import tempfile
import torch
import torch.nn as nn
import torch.nn.functional as F

import ray
from ray import train
from ray.train import Checkpoint, RunConfig, ScalingConfig, CheckpointConfig
from ray.train.torch import TorchTrainer
from ray.train._internal.jit_checkpoint_config import JITCheckpointConfig

def train_func(config):
    """Training function for TorchTrainer."""
    rank = train.get_context().get_world_rank()

    # Create model and optimizer
    model = nn.Linear(10, 1)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

    # Load from checkpoint if resuming
    start_epoch = 0
    checkpoint = train.get_checkpoint()

    if checkpoint:
        print(f"[Rank {{rank}}] Loading checkpoint...")
        with checkpoint.as_directory() as checkpoint_dir:
            model_path = os.path.join(checkpoint_dir, "model.pt")
            if os.path.exists(model_path):
                model.load_state_dict(torch.load(model_path))

            epoch_path = os.path.join(checkpoint_dir, "epoch.pt")
            if os.path.exists(epoch_path):
                start_epoch = torch.load(epoch_path) + 1
                print(f"[Rank {{rank}}] Resuming from epoch {{start_epoch}}")

    # Training loop
    for epoch in range(start_epoch, {num_epochs}):
        # Train for a few steps
        for step in range(10):
            x = torch.randn(32, 10)
            y = torch.randn(32, 1)

            optimizer.zero_grad()
            output = model(x)
            loss = F.mse_loss(output, y)
            loss.backward()
            optimizer.step()

        # Save checkpoint
        with tempfile.TemporaryDirectory() as temp_dir:
            torch.save(model.state_dict(), os.path.join(temp_dir, "model.pt"))
            torch.save(epoch, os.path.join(temp_dir, "epoch.pt"))

            checkpoint = Checkpoint.from_directory(temp_dir)

            if rank == 0:
                train.report({{"epoch": epoch, "loss": loss.item()}}, checkpoint=checkpoint)
            else:
                train.report({{"epoch": epoch, "loss": loss.item()}})

        print(f"[Rank {{rank}}] Completed epoch {{epoch}}, loss={{loss.item():.4f}}")

# Initialize Ray
ray.init(num_cpus={num_workers * 2}, ignore_reinit_error=True)

# Create trainer
trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers={num_workers}),
    run_config=RunConfig(
        name="jit_test",
        storage_path="{storage_path}",{checkpoint_config}{jit_config}
    )
)

print("Starting training...")
result = trainer.fit()

print(f"Training completed. Final metrics: {{result.metrics}}")

ray.shutdown()
'''
    return script


def run_training_subprocess(
    script_content: str, wait_time: float = 5.0, capture_output: bool = True
) -> subprocess.Popen:
    """Start training in subprocess and return process handle.

    Args:
        script_content: Python script to execute
        wait_time: Time to wait after starting process
        capture_output: Whether to capture stdout/stderr

    Returns:
        subprocess.Popen process handle
    """
    # Write script to temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        script_path = f.name

    try:
        # Start subprocess
        if capture_output:
            proc = subprocess.Popen(
                [sys.executable, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        else:
            proc = subprocess.Popen([sys.executable, script_path])

        # Wait for process to start
        time.sleep(wait_time)

        return proc

    finally:
        # Clean up script file
        try:
            os.unlink(script_path)
        except OSError:
            pass


def send_sigterm_and_wait(
    proc: subprocess.Popen, kill_wait: float, buffer: float = 3.0
) -> None:
    """Send SIGTERM to process and wait for checkpoint completion.

    Args:
        proc: Process to send SIGTERM to
        kill_wait: Kill wait time configured in JIT checkpointing
        buffer: Additional buffer time for checkpoint completion
    """
    print(f"Sending SIGTERM to process {proc.pid}")
    proc.send_signal(signal.SIGTERM)

    # Wait for checkpoint to complete
    total_wait = kill_wait + buffer
    print(f"Waiting {total_wait}s for JIT checkpoint to complete...")
    time.sleep(total_wait)


def verify_checkpoint_exists(
    storage_path: str,
    min_epoch: Optional[int] = None,
    experiment_name: str = "jit_test",
) -> tuple[bool, list[Path]]:
    """Verify checkpoint was created and optionally check epoch.

    Args:
        storage_path: Storage path where checkpoints should be created
        min_epoch: Minimum epoch number to look for
        experiment_name: Name of the experiment

    Returns:
        Tuple of (checkpoint_exists, list_of_checkpoint_paths)
    """
    storage_path = Path(storage_path)

    # Look for checkpoint directories
    checkpoint_dirs = list(storage_path.rglob("checkpoint_*"))

    if not checkpoint_dirs:
        return False, []

    # If min_epoch specified, verify at least one checkpoint has that epoch
    if min_epoch is not None:
        for checkpoint_dir in checkpoint_dirs:
            epoch_path = checkpoint_dir / "epoch.pt"
            if epoch_path.exists():
                try:
                    epoch = torch.load(epoch_path)
                    if epoch >= min_epoch:
                        return True, checkpoint_dirs
                except Exception:
                    continue
        return False, checkpoint_dirs

    return True, checkpoint_dirs


def verify_model_state_in_checkpoint(
    checkpoint_path: Path, expected_epoch: Optional[int] = None
) -> bool:
    """Verify that checkpoint contains model state.

    Args:
        checkpoint_path: Path to checkpoint directory
        expected_epoch: Expected epoch number

    Returns:
        True if checkpoint contains valid model state
    """
    try:
        # Check for model file
        model_path = checkpoint_path / "model.pt"
        if not model_path.exists():
            return False

        # Load model state
        model_state = torch.load(model_path)
        if not isinstance(model_state, dict):
            return False

        # Check for expected keys (PyTorch model state_dict)
        if "weight" not in model_state and "bias" not in model_state:
            return False

        # Check epoch if specified
        if expected_epoch is not None:
            epoch_path = checkpoint_path / "epoch.pt"
            if epoch_path.exists():
                epoch = torch.load(epoch_path)
                if epoch != expected_epoch:
                    return False

        return True

    except Exception:
        return False


def cleanup_process(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    """Clean up subprocess, ensuring it's terminated.

    Args:
        proc: Process to clean up
        timeout: Timeout for graceful termination
    """
    try:
        # Try graceful termination first
        proc.terminate()
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        # Force kill if graceful termination fails
        proc.kill()
        proc.wait(timeout=timeout)
    except Exception:
        # Ignore other errors during cleanup
        pass


def find_worker_pids(parent_pid: int) -> list[int]:
    """Find Ray worker PIDs for a given parent process.

    Args:
        parent_pid: PID of the parent process

    Returns:
        List of worker PIDs
    """
    try:
        import psutil

        parent = psutil.Process(parent_pid)
        worker_pids = []

        for child in parent.children(recursive=True):
            # Look for Python processes that might be Ray workers
            if "python" in child.name().lower():
                worker_pids.append(child.pid)

        return worker_pids

    except ImportError:
        # psutil not available
        return []
    except Exception:
        return []
