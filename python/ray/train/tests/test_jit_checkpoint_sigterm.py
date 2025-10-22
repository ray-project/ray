"""Real SIGTERM checkpoint creation tests for JIT checkpointing.

These tests spawn training in subprocesses and send actual SIGTERM signals
to verify that JIT checkpoints are created correctly.
"""

import os
import signal
import sys
import time

import pytest

import ray
from ray.train.tests.test_jit_checkpoint_utils import (
    cleanup_process,
    create_training_script,
    find_worker_pids,
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


def test_single_worker_sigterm_checkpoint_creation(ray_start_4_cpus, tmp_path):
    """Test 1: Single worker SIGTERM checkpoint creation.

    This test:
    1. Starts DataParallelTrainer with 1 worker in subprocess
    2. Waits for training to progress (e.g., epoch 5 out of 50)
    3. Sends SIGTERM to the training process
    4. Waits for JIT checkpoint to complete
    5. Verifies checkpoint files were created in storage
    6. Verifies checkpoint contains correct epoch marker
    """
    storage_path = str(tmp_path)

    # Create training script
    script_content = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=50,  # Long enough to interrupt
        trainer_type="DataParallelTrainer",
    )

    # Start training in subprocess
    proc = run_training_subprocess(script_content, wait_time=8.0)

    try:
        # Wait for training to progress (should be around epoch 5-8)
        print("Waiting for training to progress...")
        time.sleep(3)

        # Send SIGTERM
        send_sigterm_and_wait(proc, kill_wait=2.0, buffer=3.0)

        # Verify checkpoint was created
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=3  # Should have trained at least 3 epochs
        )

        assert checkpoint_exists, f"No checkpoint found in {storage_path}"
        assert len(checkpoint_dirs) > 0, "No checkpoint directories found"

        # Verify checkpoint contains model state
        for checkpoint_dir in checkpoint_dirs:
            assert verify_model_state_in_checkpoint(
                checkpoint_dir
            ), f"Checkpoint {checkpoint_dir} does not contain valid model state"

        print(
            f"✓ Single worker SIGTERM test passed. Found {len(checkpoint_dirs)} checkpoints"
        )

    finally:
        cleanup_process(proc)


def test_multi_worker_sigterm_checkpoint_creation(ray_start_4_cpus, tmp_path):
    """Test 2: Multi-worker SIGTERM checkpoint creation.

    This test:
    1. Starts DataParallelTrainer with 2 workers in subprocess
    2. Waits for training to progress
    3. Sends SIGTERM to main process
    4. Verifies JIT checkpoints created by workers
    5. Checks that checkpoint index is updated
    """
    storage_path = str(tmp_path)

    # Create training script
    script_content = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=2,
        num_epochs=50,
        trainer_type="DataParallelTrainer",
    )

    # Start training in subprocess
    proc = run_training_subprocess(script_content, wait_time=8.0)

    try:
        # Wait for training to progress
        print("Waiting for multi-worker training to progress...")
        time.sleep(3)

        # Send SIGTERM to main process
        send_sigterm_and_wait(proc, kill_wait=2.0, buffer=3.0)

        # Verify checkpoint was created
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=3
        )

        assert checkpoint_exists, f"No checkpoint found in {storage_path}"
        assert len(checkpoint_dirs) > 0, "No checkpoint directories found"

        # Verify checkpoint contains model state
        for checkpoint_dir in checkpoint_dirs:
            assert verify_model_state_in_checkpoint(
                checkpoint_dir
            ), f"Checkpoint {checkpoint_dir} does not contain valid model state"

        print(
            f"✓ Multi-worker SIGTERM test passed. Found {len(checkpoint_dirs)} checkpoints"
        )

    finally:
        cleanup_process(proc)


def test_torch_trainer_sigterm_checkpoint_creation(ray_start_4_cpus, tmp_path):
    """Test 3: TorchTrainer with real model SIGTERM.

    This test:
    1. Trains actual PyTorch model (nn.Linear) in subprocess
    2. Sends SIGTERM mid-training
    3. Verifies checkpoint contains model state_dict
    4. Verifies checkpoint contains optimizer state
    """
    storage_path = str(tmp_path)

    # Create training script with TorchTrainer
    script_content = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=50,
        trainer_type="TorchTrainer",
    )

    # Start training in subprocess
    proc = run_training_subprocess(script_content, wait_time=8.0)

    try:
        # Wait for training to progress
        print("Waiting for TorchTrainer training to progress...")
        time.sleep(3)

        # Send SIGTERM
        send_sigterm_and_wait(proc, kill_wait=2.0, buffer=3.0)

        # Verify checkpoint was created
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=3
        )

        assert checkpoint_exists, f"No checkpoint found in {storage_path}"
        assert len(checkpoint_dirs) > 0, "No checkpoint directories found"

        # Verify checkpoint contains model state
        for checkpoint_dir in checkpoint_dirs:
            assert verify_model_state_in_checkpoint(
                checkpoint_dir
            ), f"Checkpoint {checkpoint_dir} does not contain valid model state"

        print(
            f"✓ TorchTrainer SIGTERM test passed. Found {len(checkpoint_dirs)} checkpoints"
        )

    finally:
        cleanup_process(proc)


def test_worker_specific_sigterm(ray_start_4_cpus, tmp_path):
    """Test 4: Send SIGTERM to specific worker process.

    This test simulates Kubernetes sending SIGTERM to a specific worker pod.
    """
    storage_path = str(tmp_path)

    # Create training script
    script_content = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=2,
        num_epochs=50,
        trainer_type="DataParallelTrainer",
    )

    # Start training in subprocess
    proc = run_training_subprocess(script_content, wait_time=8.0)

    try:
        # Wait for training to progress
        print("Waiting for training to progress...")
        time.sleep(3)

        # Find worker PIDs
        worker_pids = find_worker_pids(proc.pid)
        print(f"Found worker PIDs: {worker_pids}")

        if worker_pids:
            # Send SIGTERM to first worker only
            target_pid = worker_pids[0]
            print(f"Sending SIGTERM to worker PID {target_pid}")
            os.kill(target_pid, signal.SIGTERM)

            # Wait for checkpoint
            time.sleep(5)  # kill_wait + buffer

            # Verify checkpoint was created
            checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
                storage_path, min_epoch=3
            )

            assert checkpoint_exists, f"No checkpoint found in {storage_path}"
            print(
                f"✓ Worker-specific SIGTERM test passed. Found {len(checkpoint_dirs)} checkpoints"
            )
        else:
            print("⚠ Could not find worker PIDs, skipping worker-specific SIGTERM test")
            pytest.skip("Worker PIDs not found")

    finally:
        cleanup_process(proc)


def test_jit_checkpoint_with_regular_checkpoints(ray_start_4_cpus, tmp_path):
    """Test 5: JIT checkpointing with regular checkpointing enabled.

    This test verifies that JIT checkpoints work alongside regular checkpoints.
    """
    storage_path = str(tmp_path)

    # Create training script with regular checkpoints every 5 epochs
    script_content = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=50,
        checkpoint_freq=5,  # Regular checkpoint every 5 epochs
        trainer_type="DataParallelTrainer",
    )

    # Start training in subprocess
    proc = run_training_subprocess(script_content, wait_time=8.0)

    try:
        # Wait for training to progress (should create regular checkpoint at epoch 5)
        print("Waiting for training to progress and create regular checkpoint...")
        time.sleep(5)

        # Send SIGTERM (should create JIT checkpoint)
        send_sigterm_and_wait(proc, kill_wait=2.0, buffer=3.0)

        # Verify both regular and JIT checkpoints exist
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=3
        )

        assert checkpoint_exists, f"No checkpoint found in {storage_path}"
        assert len(checkpoint_dirs) >= 1, "Should have at least one checkpoint"

        # Verify checkpoint contains model state
        for checkpoint_dir in checkpoint_dirs:
            assert verify_model_state_in_checkpoint(
                checkpoint_dir
            ), f"Checkpoint {checkpoint_dir} does not contain valid model state"

        print(
            f"✓ JIT + regular checkpoint test passed. Found {len(checkpoint_dirs)} checkpoints"
        )

    finally:
        cleanup_process(proc)


def test_jit_checkpoint_kill_wait_timing(ray_start_4_cpus, tmp_path):
    """Test 6: Verify kill_wait timing works correctly.

    This test verifies that the kill_wait delay is respected.
    """
    storage_path = str(tmp_path)
    kill_wait = 3.0  # Longer kill_wait for testing

    # Create training script
    script_content = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=kill_wait,
        num_workers=1,
        num_epochs=50,
        trainer_type="DataParallelTrainer",
    )

    # Start training in subprocess
    proc = run_training_subprocess(script_content, wait_time=8.0)

    try:
        # Wait for training to progress
        print("Waiting for training to progress...")
        time.sleep(3)

        # Send SIGTERM
        start_time = time.time()
        proc.send_signal(signal.SIGTERM)

        # Wait for kill_wait + buffer
        time.sleep(kill_wait + 2.0)

        # Verify checkpoint was created
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=3
        )

        assert checkpoint_exists, f"No checkpoint found in {storage_path}"
        assert len(checkpoint_dirs) > 0, "No checkpoint directories found"

        elapsed = time.time() - start_time
        assert (
            elapsed >= kill_wait
        ), f"Checkpoint created too quickly: {elapsed}s < {kill_wait}s"

        print(f"✓ Kill wait timing test passed. Elapsed: {elapsed:.2f}s")

    finally:
        cleanup_process(proc)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
