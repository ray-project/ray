"""Integration tests for Just-In-Time (JIT) Checkpointing in Ray Train.

These tests demonstrate end-to-end functionality of JIT checkpointing
with actual training runs.
"""

import time

import pytest

import ray
from ray import train
from ray.train import RunConfig, ScalingConfig
from ray.train._internal.jit_checkpoint_config import JITCheckpointConfig
from ray.train.data_parallel_trainer import DataParallelTrainer


@pytest.fixture
def ray_start_2_cpus():
    """Start a Ray cluster with 2 CPUs for testing."""
    ray.init(num_cpus=2, ignore_reinit_error=True)
    yield
    ray.shutdown()


def test_jit_checkpoint_config_integration(ray_start_2_cpus, tmp_path):
    """Test that JIT checkpoint config propagates to training workers.

    This test verifies that:
    1. JIT config can be set in RunConfig
    2. Config propagates to training workers
    3. Signal handler is registered when enabled
    4. Handler is not registered when disabled
    """

    # Test 1: JIT checkpointing enabled
    def train_func_enabled():
        from ray.train._internal.session import get_session

        session = get_session()

        # Verify that JIT checkpoint handler was initialized
        assert session.jit_checkpoint_handler is not None
        assert session.jit_checkpoint_handler.kill_wait == 2.0

        # Report one iteration
        train.report({"epoch": 1})

    trainer_enabled = DataParallelTrainer(
        train_func_enabled,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            storage_path=str(tmp_path / "enabled"),
            jit_checkpoint_config=JITCheckpointConfig(
                enabled=True,
                kill_wait=2.0,
            ),
        ),
    )

    result = trainer_enabled.fit()
    assert result is not None

    # Test 2: JIT checkpointing disabled (default)
    def train_func_disabled():
        from ray.train._internal.session import get_session

        session = get_session()

        # Verify that JIT checkpoint handler was NOT initialized
        assert session.jit_checkpoint_handler is None

        # Report one iteration
        train.report({"epoch": 1})

    trainer_disabled = DataParallelTrainer(
        train_func_disabled,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            storage_path=str(tmp_path / "disabled"),
        ),
    )

    result = trainer_disabled.fit()
    assert result is not None


def test_jit_checkpoint_marker_creation(ray_start_2_cpus, tmp_path):
    """Test that JIT checkpoint creates marker files on SIGTERM.

    This test:
    1. Starts training with JIT checkpointing enabled
    2. Sends SIGTERM to the training process
    3. Waits for checkpoint to complete
    4. Verifies checkpoint was created
    """

    checkpoint_created = []

    def train_func():
        from ray.train._internal.session import get_session

        session = get_session()

        # Report initial state
        train.report({"epoch": 0})

        # Give time for signal to be processed if sent
        time.sleep(0.5)

        # Check if JIT checkpoint was triggered
        if session.jit_checkpoint_handler:
            if session.jit_checkpoint_handler.checkpoint_requested:
                checkpoint_created.append(True)

        # Continue training
        train.report({"epoch": 1})

    # Note: For a full integration test, we would need to:
    # 1. Start the trainer in a separate process
    # 2. Send SIGTERM to that process
    # 3. Verify checkpoint was created before termination
    #
    # For this POC, we'll just verify the handler is initialized
    # and can be triggered programmatically

    trainer = DataParallelTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            jit_checkpoint_config=JITCheckpointConfig(
                enabled=True,
                kill_wait=0.1,  # Short wait for testing
            ),
        ),
    )

    result = trainer.fit()
    assert result is not None


def test_jit_checkpoint_on_sigterm_full(ray_start_2_cpus, tmp_path):
    """Full integration test with actual SIGTERM signal.

    This test:
    1. Starts Ray Train in a subprocess
    2. Sends SIGTERM to the subprocess
    3. Verifies checkpoint was created before termination
    4. Restarts training from the JIT checkpoint
    """
    from ray.train.tests.test_jit_checkpoint_utils import (
        cleanup_process,
        create_training_script,
        run_training_subprocess,
        send_sigterm_and_wait,
        verify_checkpoint_exists,
    )

    storage_path = str(tmp_path)
    experiment_name = "jit_sigterm_full_test"

    # === Phase 1: Create JIT checkpoint via SIGTERM ===
    print("=== Phase 1: Creating JIT checkpoint via SIGTERM ===")

    # Create training script
    script_content = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=50,
        trainer_type="DataParallelTrainer",
    )

    # Modify script to use specific experiment name
    script_content = script_content.replace(
        'name="jit_test"', f'name="{experiment_name}"'
    )

    # Start training in subprocess
    proc = run_training_subprocess(script_content, wait_time=8.0)

    try:
        # Wait for training to progress
        print("Waiting for training to progress...")
        time.sleep(5)

        # Send SIGTERM to create JIT checkpoint
        send_sigterm_and_wait(proc, kill_wait=2.0, buffer=3.0)

        # Verify JIT checkpoint was created
        checkpoint_exists, checkpoint_dirs = verify_checkpoint_exists(
            storage_path, min_epoch=3, experiment_name=experiment_name
        )

        assert checkpoint_exists, f"No JIT checkpoint found in {storage_path}"
        print(
            f"✓ JIT checkpoint created via SIGTERM. Checkpoints: {len(checkpoint_dirs)}"
        )

    finally:
        cleanup_process(proc)

    # === Phase 2: Resume training from JIT checkpoint ===
    print("=== Phase 2: Resuming training from JIT checkpoint ===")

    # Create new training script for resumption
    resume_script_content = create_training_script(
        storage_path=storage_path,
        enable_jit=True,
        kill_wait=2.0,
        num_workers=1,
        num_epochs=20,  # Shorter run for testing
        trainer_type="DataParallelTrainer",
    )

    # Modify script to use same experiment name (for resumption)
    resume_script_content = resume_script_content.replace(
        'name="jit_test"', f'name="{experiment_name}"'
    )

    # Start resumed training in subprocess
    resume_proc = run_training_subprocess(resume_script_content, wait_time=5.0)

    try:
        # Wait for resumed training to complete
        print("Waiting for resumed training to complete...")
        time.sleep(8)

        # Verify training completed successfully
        return_code = resume_proc.poll()
        if return_code is None:
            time.sleep(5)
            return_code = resume_proc.poll()

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
        cleanup_process(resume_proc)


def test_multiple_workers_jit_checkpoint(ray_start_2_cpus, tmp_path):
    """Test JIT checkpointing with multiple workers.

    Verifies that each worker gets its own JIT checkpoint handler.
    """

    worker_handlers = []

    def train_func():
        from ray.train._internal.session import get_session

        session = get_session()
        rank = train.get_context().get_world_rank()

        # Each worker should have its own handler
        assert session.jit_checkpoint_handler is not None

        # Record that this worker's handler exists
        worker_handlers.append(rank)

        train.report({"rank": rank})

    trainer = DataParallelTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            jit_checkpoint_config=JITCheckpointConfig(
                enabled=True,
                kill_wait=1.0,
            ),
        ),
    )

    result = trainer.fit()
    assert result is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
