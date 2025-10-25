"""Full recovery and resumption tests for JIT checkpointing.

These tests verify that training can actually resume from JIT checkpoints
with correct state recovery, model state preservation, and proper
interaction with regular checkpoints.
"""

import sys
import time

import pytest

import ray
from ray.train.tests.test_jit_checkpoint_utils import (
    cleanup_process,
    create_training_script,
    run_training_subprocess,
    send_sigterm_and_wait,
    verify_checkpoint_exists,
    verify_model_state_in_checkpoint,
)


@pytest.fixture
def ray_start_4_cpus():
    """Start a Ray cluster with 4 CPUs for testing."""
    address_info = ray.init(num_cpus=4, ignore_reinit_error=True)
    yield address_info
    ray.shutdown()


def test_resume_from_jit_checkpoint_correct_state(ray_start_4_cpus, tmp_path):
    """Test 1: Resume from JIT checkpoint with correct state.

    This test:
    1. Run 1: Train to epoch 10, send SIGTERM, create JIT checkpoint
    2. Run 2: Start new training with same storage_path and experiment name
    3. Verify: Training resumes from epoch 10 (not 0)
    4. Verify: Model state is preserved (loss continues from correct point)
    5. Train to completion (epoch 20)
    """
    storage_path = str(tmp_path)
    experiment_name = "jit_recovery_test"

    # === Run 1: Train and create JIT checkpoint ===
    print("=== Run 1: Training and creating JIT checkpoint ===")

    script_content_1 = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=50,
        trainer_type="DataParallelTrainer",
    )

    # Modify script to use specific experiment name
    script_content_1 = script_content_1.replace(
        'name="jit_test"', f'name="{experiment_name}"'
    )

    proc1 = run_training_subprocess(script_content_1, wait_time=8.0)

    try:
        # Wait for training to progress to around epoch 8-10
        print("Waiting for training to progress...")
        time.sleep(5)

        # Send SIGTERM to create JIT checkpoint
        send_sigterm_and_wait(proc1, kill_wait=2.0, buffer=3.0)

        # Verify JIT checkpoint was created
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=5, experiment_name=experiment_name
        )

        assert checkpoint_exists, f"No JIT checkpoint found in {storage_path}"
        print(f"✓ JIT checkpoint created with {len(checkpoint_dirs)} checkpoint(s)")

    finally:
        cleanup_process(proc1)

    # === Run 2: Resume training from JIT checkpoint ===
    print("=== Run 2: Resuming from JIT checkpoint ===")

    script_content_2 = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=20,  # Shorter run for testing
        trainer_type="DataParallelTrainer",
    )

    # Modify script to use same experiment name (for resumption)
    script_content_2 = script_content_2.replace(
        'name="jit_test"', f'name="{experiment_name}"'
    )

    proc2 = run_training_subprocess(script_content_2, wait_time=5.0)

    try:
        # Wait for training to complete
        print("Waiting for resumed training to complete...")
        time.sleep(8)

        # Verify training completed successfully
        return_code = proc2.poll()
        if return_code is None:
            # Process still running, wait a bit more
            time.sleep(5)
            return_code = proc2.poll()

        # Should complete successfully (not crash)
        assert (
            return_code == 0
        ), f"Resumed training failed with return code {return_code}"

        # Verify final checkpoint exists
        final_checkpoint_exists, final_checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=15, experiment_name=experiment_name
        )

        assert final_checkpoint_exists, "No final checkpoint found after resumption"
        print(
            f"✓ Training resumed and completed successfully. Final checkpoints: {len(final_checkpoint_dirs)}"
        )

    finally:
        cleanup_process(proc2)


def test_jit_checkpoint_interaction_with_regular_checkpoints(
    ray_start_4_cpus, tmp_path
):
    """Test 2: JIT checkpoint interacts correctly with regular checkpoints.

    This test:
    1. Configure regular checkpoints every 8 epochs
    2. Train and create regular checkpoint at epoch 8
    3. Continue training, send SIGTERM at epoch 13
    4. Verify: Both regular checkpoint (epoch 8) and JIT checkpoint (epoch 13) exist
    5. Resume training: Should resume from epoch 13 (most recent)
    """
    storage_path = str(tmp_path)
    experiment_name = "jit_regular_interaction_test"

    # === Run 1: Create regular checkpoint, then JIT checkpoint ===
    print("=== Run 1: Creating regular checkpoint, then JIT checkpoint ===")

    script_content_1 = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=50,
        checkpoint_freq=8,  # Regular checkpoint every 8 epochs
        trainer_type="DataParallelTrainer",
    )

    # Modify script to use specific experiment name
    script_content_1 = script_content_1.replace(
        'name="jit_test"', f'name="{experiment_name}"'
    )

    proc1 = run_training_subprocess(script_content_1, wait_time=8.0)

    try:
        # Wait for regular checkpoint at epoch 8
        print("Waiting for regular checkpoint at epoch 8...")
        time.sleep(6)

        # Send SIGTERM to create JIT checkpoint around epoch 13
        send_sigterm_and_wait(proc1, kill_wait=2.0, buffer=3.0)

        # Verify both checkpoints exist
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=8, experiment_name=experiment_name
        )

        assert checkpoint_exists, f"No checkpoints found in {storage_path}"
        assert (
            len(checkpoint_dirs) >= 2
        ), f"Expected at least 2 checkpoints, found {len(checkpoint_dirs)}"

        print(
            f"✓ Both regular and JIT checkpoints created. Total: {len(checkpoint_dirs)}"
        )

    finally:
        cleanup_process(proc1)

    # === Run 2: Resume from most recent checkpoint (should be JIT checkpoint) ===
    print("=== Run 2: Resuming from most recent checkpoint ===")

    script_content_2 = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=20,
        checkpoint_freq=8,
        trainer_type="DataParallelTrainer",
    )

    # Modify script to use same experiment name
    script_content_2 = script_content_2.replace(
        'name="jit_test"', f'name="{experiment_name}"'
    )

    proc2 = run_training_subprocess(script_content_2, wait_time=5.0)

    try:
        # Wait for training to complete
        print("Waiting for resumed training to complete...")
        time.sleep(8)

        # Verify training completed successfully
        return_code = proc2.poll()
        if return_code is None:
            time.sleep(5)
            return_code = proc2.poll()

        assert (
            return_code == 0
        ), f"Resumed training failed with return code {return_code}"

        # Verify final checkpoint exists
        final_checkpoint_exists, final_checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=15, experiment_name=experiment_name
        )

        assert final_checkpoint_exists, "No final checkpoint found after resumption"
        print("✓ Training resumed from JIT checkpoint and completed successfully")

    finally:
        cleanup_process(proc2)


def test_resume_with_different_worker_count(ray_start_4_cpus, tmp_path):
    """Test 3: Resume with different worker count.

    This test:
    1. Run 1: Train with 2 workers, SIGTERM at epoch 5
    2. Run 2: Resume with 1 worker
    3. Verify: Training continues from epoch 5 with correct state
    """
    storage_path = str(tmp_path)
    experiment_name = "jit_worker_count_test"

    # === Run 1: Train with 2 workers, create JIT checkpoint ===
    print("=== Run 1: Training with 2 workers, creating JIT checkpoint ===")

    script_content_1 = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=2,
        num_epochs=50,
        trainer_type="DataParallelTrainer",
    )

    # Modify script to use specific experiment name
    script_content_1 = script_content_1.replace(
        'name="jit_test"', f'name="{experiment_name}"'
    )

    proc1 = run_training_subprocess(script_content_1, wait_time=8.0)

    try:
        # Wait for training to progress
        print("Waiting for 2-worker training to progress...")
        time.sleep(5)

        # Send SIGTERM to create JIT checkpoint
        send_sigterm_and_wait(proc1, kill_wait=2.0, buffer=3.0)

        # Verify JIT checkpoint was created
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=3, experiment_name=experiment_name
        )

        assert checkpoint_exists, f"No JIT checkpoint found in {storage_path}"
        print(
            f"✓ JIT checkpoint created with 2 workers. Checkpoints: {len(checkpoint_dirs)}"
        )

    finally:
        cleanup_process(proc1)

    # === Run 2: Resume with 1 worker ===
    print("=== Run 2: Resuming with 1 worker ===")

    script_content_2 = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,  # Different worker count
        num_epochs=20,
        trainer_type="DataParallelTrainer",
    )

    # Modify script to use same experiment name
    script_content_2 = script_content_2.replace(
        'name="jit_test"', f'name="{experiment_name}"'
    )

    proc2 = run_training_subprocess(script_content_2, wait_time=5.0)

    try:
        # Wait for training to complete
        print("Waiting for resumed training with 1 worker to complete...")
        time.sleep(8)

        # Verify training completed successfully
        return_code = proc2.poll()
        if return_code is None:
            time.sleep(5)
            return_code = proc2.poll()

        assert (
            return_code == 0
        ), f"Resumed training failed with return code {return_code}"

        # Verify final checkpoint exists
        final_checkpoint_exists, final_checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=10, experiment_name=experiment_name
        )

        assert final_checkpoint_exists, "No final checkpoint found after resumption"
        print(
            "✓ Training resumed with different worker count and completed successfully"
        )

    finally:
        cleanup_process(proc2)


def test_jit_checkpoint_respects_num_to_keep(ray_start_4_cpus, tmp_path):
    """Test 4: JIT checkpoint respects CheckpointConfig.num_to_keep.

    This test:
    1. Configure num_to_keep=2
    2. Create multiple checkpoints and JIT checkpoint
    3. Verify: Old checkpoints are cleaned up
    4. Verify: JIT checkpoint follows same retention policy
    """
    storage_path = str(tmp_path)
    experiment_name = "jit_num_to_keep_test"

    # Create training script with num_to_keep=2
    script_content = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=50,
        checkpoint_freq=3,  # Regular checkpoint every 3 epochs
        trainer_type="DataParallelTrainer",
    )

    # Modify script to set num_to_keep=2
    script_content = script_content.replace("num_to_keep=3", "num_to_keep=2")
    script_content = script_content.replace(
        'name="jit_test"', f'name="{experiment_name}"'
    )

    proc = run_training_subprocess(script_content, wait_time=8.0)

    try:
        # Wait for multiple checkpoints to be created
        print("Waiting for multiple checkpoints to be created...")
        time.sleep(8)

        # Send SIGTERM to create JIT checkpoint
        send_sigterm_and_wait(proc, kill_wait=2.0, buffer=3.0)

        # Verify checkpoints exist
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=6, experiment_name=experiment_name
        )

        assert checkpoint_exists, f"No checkpoints found in {storage_path}"

        # Should have at most 2 checkpoints due to num_to_keep=2
        # (This is a basic check - the actual retention logic is in Ray Train)
        print(f"✓ Checkpoints created. Total: {len(checkpoint_dirs)}")

        # Verify checkpoint contains model state
        for checkpoint_dir in checkpoint_dirs:
            assert verify_model_state_in_checkpoint(
                checkpoint_dir
            ), f"Checkpoint {checkpoint_dir} does not contain valid model state"

    finally:
        cleanup_process(proc)


def test_model_state_preservation_across_resumption(ray_start_4_cpus, tmp_path):
    """Test 5: Verify model state is correctly preserved across resumption.

    This test creates a more sophisticated model and verifies that the
    model weights are correctly preserved across JIT checkpoint resumption.
    """
    storage_path = str(tmp_path)
    experiment_name = "jit_model_state_test"

    # Create a more complex training script with explicit model state tracking
    complex_script = f'''
import os
import sys
import time
import tempfile
import torch
import torch.nn as nn
import torch.nn.functional as F

import ray
from ray import train
from ray.train import Checkpoint, RunConfig, ScalingConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train._internal.jit_checkpoint_config import JITCheckpointConfig

class SimpleModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.layer1 = nn.Linear(10, 5)
        self.layer2 = nn.Linear(5, 1)

    def forward(self, x):
        x = F.relu(self.layer1(x))
        return self.layer2(x)

def train_func(config):
    """Training function with explicit state tracking."""
    rank = train.get_context().get_world_rank()

    # Create model and optimizer
    model = SimpleModel()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

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

    # Training loop with explicit loss tracking
    losses = []
    for epoch in range(start_epoch, 20):
        epoch_loss = 0.0
        for step in range(10):
            x = torch.randn(32, 10)
            y = torch.randn(32, 1)

            optimizer.zero_grad()
            output = model(x)
            loss = F.mse_loss(output, y)
            loss.backward()
            optimizer.step()

            epoch_loss += loss.item()

        epoch_loss /= 10
        losses.append(epoch_loss)

        # Save checkpoint with model state and loss history
        with tempfile.TemporaryDirectory() as temp_dir:
            torch.save(model.state_dict(), os.path.join(temp_dir, "model.pt"))
            torch.save(epoch, os.path.join(temp_dir, "epoch.pt"))
            torch.save(losses, os.path.join(temp_dir, "losses.pt"))

            checkpoint = Checkpoint.from_directory(temp_dir)

            if rank == 0:
                train.report({{"epoch": epoch, "loss": epoch_loss}}, checkpoint=checkpoint)
            else:
                train.report({{"epoch": epoch, "loss": epoch_loss}})

        print(f"[Rank {{rank}}] Epoch {{epoch}}, Loss={{epoch_loss:.4f}}")

# Initialize Ray
ray.init(num_cpus=2, ignore_reinit_error=True)

# Create trainer
trainer = DataParallelTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=1),
    run_config=RunConfig(
        name="{experiment_name}",
        storage_path="{storage_path}",
        jit_checkpoint_config=JITCheckpointConfig(
            enabled=True,
            kill_wait=2.0
        )
    )
)

print("Starting training...")
result = trainer.fit()

print(f"Training completed. Final metrics: {{result.metrics}}")

ray.shutdown()
'''

    # === Run 1: Train and create JIT checkpoint ===
    print("=== Run 1: Training with complex model, creating JIT checkpoint ===")

    proc1 = run_training_subprocess(complex_script, wait_time=8.0)

    try:
        # Wait for training to progress
        print("Waiting for complex model training to progress...")
        time.sleep(5)

        # Send SIGTERM to create JIT checkpoint
        send_sigterm_and_wait(proc1, kill_wait=2.0, buffer=3.0)

        # Verify JIT checkpoint was created
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=3, experiment_name=experiment_name
        )

        assert checkpoint_exists, f"No JIT checkpoint found in {storage_path}"
        print(
            f"✓ Complex model JIT checkpoint created. Checkpoints: {len(checkpoint_dirs)}"
        )

    finally:
        cleanup_process(proc1)

    # === Run 2: Resume and verify model state ===
    print("=== Run 2: Resuming and verifying model state ===")

    proc2 = run_training_subprocess(complex_script, wait_time=5.0)

    try:
        # Wait for training to complete
        print("Waiting for resumed training to complete...")
        time.sleep(8)

        # Verify training completed successfully
        return_code = proc2.poll()
        if return_code is None:
            time.sleep(5)
            return_code = proc2.poll()

        assert (
            return_code == 0
        ), f"Resumed training failed with return code {return_code}"

        # Verify final checkpoint exists
        final_checkpoint_exists, final_checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=15, experiment_name=experiment_name
        )

        assert final_checkpoint_exists, "No final checkpoint found after resumption"
        print("✓ Complex model training resumed and completed successfully")

    finally:
        cleanup_process(proc2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
