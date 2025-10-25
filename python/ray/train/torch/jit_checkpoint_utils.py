"""
Utilities for automatic JIT checkpointing in PyTorch training.

This module provides hooks to enable automatic checkpoint saving on SIGTERM
without requiring users to write checkpointing code.
"""

import logging
import os
import tempfile
from typing import Optional

import torch

from ray.train import Checkpoint, get_context

logger = logging.getLogger(__name__)


def enable_auto_jit_checkpoint(
    model: torch.nn.Module,
    optimizer: Optional[torch.optim.Optimizer] = None,
    additional_state: Optional[dict] = None,
):
    """Enable automatic JIT checkpointing for PyTorch training.

    This function registers the model and optimizer with the training session,
    allowing the JIT checkpoint handler to automatically save their state
    when SIGTERM is received.

    Users should call this ONCE at the beginning of their training function,
    after creating model and optimizer.

    Example:
        def train_func(config):
            model = NeuralNetwork()
            model = train.torch.prepare_model(model)
            optimizer = torch.optim.Adam(model.parameters())

            # Enable auto JIT checkpoint - ONE line of code!
            train.torch.enable_auto_jit_checkpoint(model, optimizer)

            # Now train normally - NO manual checkpointing needed!
            for epoch in range(num_epochs):
                # ... training ...
                train.report({"loss": loss})  # No checkpoint parameter!
                # JIT checkpoint will auto-save current model/optimizer state on SIGTERM

    Args:
        model: The PyTorch model to checkpoint
        optimizer: The optimizer to checkpoint (optional)
        additional_state: Any additional state to include in checkpoints (optional)
    """
    def create_jit_checkpoint():
        """Function called by JIT checkpoint handler to save current state"""
        with tempfile.TemporaryDirectory() as tmpdir:
            import os

            # Save model state
            # Handle DistributedDataParallel wrapper
            if hasattr(model, "module"):
                torch.save(model.module.state_dict(), os.path.join(tmpdir, "model.pt"))
            else:
                torch.save(model.state_dict(), os.path.join(tmpdir, "model.pt"))

            # Save optimizer state if provided
            if optimizer is not None:
                # Handle wrapped optimizer
                if hasattr(optimizer, "optimizer"):
                    torch.save(optimizer.optimizer.state_dict(), os.path.join(tmpdir, "optimizer.pt"))
                else:
                    torch.save(optimizer.state_dict(), os.path.join(tmpdir, "optimizer.pt"))

            # Save additional state if provided
            if additional_state is not None:
                torch.save(additional_state, os.path.join(tmpdir, "extra_state.pt"))

            logger.info("Auto JIT checkpoint created with current model/optimizer state")
            return Checkpoint.from_directory(tmpdir)

    # Register the checkpoint function with the training session
    try:
        ctx = get_context()
        if ctx:
            # Store in session state so JIT checkpoint handler can access it
            from ray.train._internal.session import get_session
            session = get_session()
            if session:
                session.set_state("jit_checkpoint_fn", create_jit_checkpoint)
                logger.info(
                    "Auto JIT checkpoint enabled - model and optimizer will be "
                    "automatically saved on SIGTERM"
                )
            else:
                logger.warning("No training session found, auto JIT checkpoint not enabled")
        else:
            logger.warning("Not in training context, auto JIT checkpoint not enabled")
    except Exception as e:
        logger.warning(f"Failed to enable auto JIT checkpoint: {e}")


def _auto_register_model(model: torch.nn.Module):
    """Automatically register model for JIT checkpointing if enabled via environment.

    This is called internally by prepare_model when JIT checkpointing is enabled.
    """
    # Check if JIT checkpoint is enabled via environment variable
    jit_enabled = os.getenv("RAY_TRAIN_JIT_CHECKPOINT_ENABLED", "").lower() == "true"

    if not jit_enabled:
        return model

    # Store model reference in session for JIT checkpoint access
    try:
        from ray.train._internal.session import get_session
        session = get_session()
        if session:
            session.set_state("_auto_jit_model", model)
            logger.debug("Model registered for automatic JIT checkpointing")
            _try_setup_auto_jit_checkpoint_in_session(session)
    except Exception as e:
        logger.debug(f"Could not auto-register model: {e}")

    return model


def _auto_register_optimizer(optimizer: torch.optim.Optimizer):
    """Automatically register optimizer for JIT checkpointing if enabled via environment.

    This is called internally by prepare_optimizer when JIT checkpointing is enabled.
    """
    # Check if JIT checkpoint is enabled via environment variable
    jit_enabled = os.getenv("RAY_TRAIN_JIT_CHECKPOINT_ENABLED", "").lower() == "true"

    if not jit_enabled:
        return optimizer

    # Store optimizer reference in session for JIT checkpoint access
    try:
        from ray.train._internal.session import get_session
        session = get_session()
        if session:
            session.set_state("_auto_jit_optimizer", optimizer)
            logger.debug("Optimizer registered for automatic JIT checkpointing")
            _try_setup_auto_jit_checkpoint_in_session(session)
    except Exception as e:
        logger.debug(f"Could not auto-register optimizer: {e}")

    return optimizer


def _try_setup_auto_jit_checkpoint_in_session(session):
    """Try to set up auto JIT checkpoint using session-stored model/optimizer."""

    model = session.get_state("_auto_jit_model")
    if model is None:
        return  # Need at least a model

    optimizer = session.get_state("_auto_jit_optimizer")

    # Create checkpoint function that accesses session state
    def create_auto_jit_checkpoint():
        """Auto-generated checkpoint function using session-stored model/optimizer."""
        # Get current references from session
        current_model = session.get_state("_auto_jit_model")
        current_optimizer = session.get_state("_auto_jit_optimizer")
        current_extra_state = session.get_state("_auto_jit_additional_state")

        # Create persistent temporary directory (don't auto-delete)
        import os
        tmpdir = tempfile.mkdtemp(prefix="jit_checkpoint_")

        try:
            # Save model state
            if current_model is not None:
                if hasattr(current_model, "module"):
                    torch.save(current_model.module.state_dict(), os.path.join(tmpdir, "model.pt"))
                else:
                    torch.save(current_model.state_dict(), os.path.join(tmpdir, "model.pt"))
                logger.debug("Saved model state to JIT checkpoint")

            # Save optimizer state if available
            if current_optimizer is not None:
                if hasattr(current_optimizer, "optimizer"):
                    torch.save(current_optimizer.optimizer.state_dict(), os.path.join(tmpdir, "optimizer.pt"))
                else:
                    torch.save(current_optimizer.state_dict(), os.path.join(tmpdir, "optimizer.pt"))
                logger.debug("Saved optimizer state to JIT checkpoint")

            # Automatically save training metadata (iteration, time, etc.)
            # Users don't need to do anything - this is tracked automatically by Ray Train
            training_metadata = {
                "iteration": session.iteration,
                "time_total": session.time_total,
                "timestamp": __import__("time").time(),
            }

            # Include user's extra state if they set it (optional, not required)
            if current_extra_state is not None:
                training_metadata.update(current_extra_state)

            torch.save(training_metadata, os.path.join(tmpdir, "training_metadata.pt"))
            logger.info(f"Created JIT checkpoint with model, optimizer, and training metadata (iteration={training_metadata['iteration']})")

            # Return checkpoint - persist_current_checkpoint will copy files before tmpdir is cleaned
            return Checkpoint.from_directory(tmpdir)
        except Exception:
            # Clean up tmpdir on error
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise

    # Register the checkpoint function
    session.set_state("jit_checkpoint_fn", create_auto_jit_checkpoint)
    logger.info(
        "Automatic JIT checkpointing enabled for model and optimizer"
    )
