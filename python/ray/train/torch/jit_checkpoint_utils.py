"""
Utilities for automatic JIT checkpointing in PyTorch training.

This module provides hooks to enable automatic checkpoint saving on SIGTERM
without requiring users to write checkpointing code.
"""

import logging
import os
import shutil
import tempfile
import time
from typing import Optional

import torch

from ray.train import Checkpoint, get_context
from ray import train

logger = logging.getLogger(__name__)


class AutoJITCheckpointManager:
    """Manages automatic JIT checkpoint save/load without user intervention.
    
    This class provides a callback-based approach similar to Transformers'
    JITCheckpointCallback, achieving true zero-code automation for checkpoint
    management in Ray Train.
    
    The manager:
    - Automatically loads checkpoint state when model/optimizer are registered
    - Tracks training iteration internally (no user tracking needed)
    - Creates checkpoints on train.report() calls
    - Creates checkpoints on SIGTERM signals
    - Handles all checkpoint complexity transparently
    """

    def __init__(self):
        """Initialize the auto JIT checkpoint manager."""
        self.registered_objects = {}
        self.training_state = {'iteration': 0}
        self.checkpoint_loaded = False
        logger.debug("AutoJITCheckpointManager initialized")

    def register_model(self, model: torch.nn.Module):
        """Register model for automatic checkpoint management.
        
        Called automatically by prepare_model() when JIT checkpointing is enabled.
        Triggers automatic checkpoint loading if a checkpoint exists.
        
        Args:
            model: The prepared PyTorch model (may be wrapped in DDP)
        """
        self.registered_objects['model'] = model
        logger.debug("Model registered with AutoJITCheckpointManager")
        self._try_auto_load_checkpoint()

    def register_optimizer(self, optimizer: torch.optim.Optimizer):
        """Register optimizer for automatic checkpoint management.
        
        Called automatically by prepare_optimizer() when JIT checkpointing is enabled.
        
        Args:
            optimizer: The prepared PyTorch optimizer (may be wrapped)
        """
        self.registered_objects['optimizer'] = optimizer
        logger.debug("Optimizer registered with AutoJITCheckpointManager")
        # Try to load checkpoint again in case it wasn't loaded yet
        self._try_auto_load_checkpoint()

    def _try_auto_load_checkpoint(self):
        """Attempt to automatically load checkpoint when model is registered.
        
        This is called when model or optimizer is registered. It checks if:
        1. Model is registered (required)
        2. Checkpoint hasn't been loaded yet
        3. A checkpoint exists via train.get_checkpoint()
        
        If all conditions are met, automatically loads the checkpoint state.
        Enhanced with better retry logic for timing issues with Ray Train's auto-resume.
        """
        if 'model' not in self.registered_objects:
            logger.debug("Model not yet registered, skipping checkpoint load")
            return
        
        if self.checkpoint_loaded:
            logger.debug("Checkpoint already loaded, skipping")
            return
        
        # Enhanced retry logic with exponential backoff
        # Handles timing issues where Ray Train's auto-resume loads checkpoint
        # asynchronously and train.get_checkpoint() isn't immediately available
        import time
        for attempt in range(20):
            checkpoint = train.get_checkpoint()
            if checkpoint:
                logger.info(f"Checkpoint detected (attempt {attempt + 1}), auto-loading state...")
                self._load_checkpoint_state(checkpoint)
                self.checkpoint_loaded = True
                return
            
            if attempt < 19:
                # Exponential backoff: 0.1s, 0.2s, 0.4s, ... up to 3.2s total
                delay = min(0.1 * (2 ** attempt), 0.5)
                logger.debug(f"Checkpoint not available yet, retrying in {delay:.2f}s (attempt {attempt + 1}/20)")
                time.sleep(delay)
        
        logger.debug("No checkpoint to load after 20 attempts, starting fresh")

    def increment_iteration(self):
        """Track iteration count without creating a checkpoint.
        
        Called by train.report() to track the current training iteration.
        No checkpoint is created - that only happens on SIGTERM.
        This is true JIT checkpointing - checkpoints are saved just-in-time
        when SIGTERM is received, not on every training iteration.
        
        The iteration count is tracked in memory and will be saved when
        on_sigterm_received() creates the checkpoint.
        """
        self.training_state['iteration'] += 1
        logger.debug(f"Tracked iteration {self.training_state['iteration']} (no checkpoint, JIT mode)")

    def on_sigterm_received(self) -> Optional[Checkpoint]:
        """Called by JIT checkpoint handler when SIGTERM is received.
        
        This is invoked by the existing JITCheckpointHandler when a SIGTERM
        signal is received. It creates an immediate checkpoint with the
        current training state.
        
        Returns:
            Checkpoint object or None if checkpoint creation fails
        """
        logger.info("SIGTERM received, creating emergency JIT checkpoint")
        try:
            return self._create_jit_checkpoint()
        except Exception as e:
            logger.error(f"Failed to create SIGTERM JIT checkpoint: {e}")
            return None

    def _load_checkpoint_state(self, checkpoint: Checkpoint):
        """Load model/optimizer state from checkpoint automatically.
        
        This method extracts and loads:
        - Model state_dict
        - Optimizer state_dict (if available)
        - Training metadata (iteration, time, etc.)
        
        Args:
            checkpoint: The checkpoint to load from
        """
        try:
            with checkpoint.as_directory() as checkpoint_dir:
                # Load model state
                model_path = os.path.join(checkpoint_dir, "model.pt")
                if os.path.exists(model_path) and 'model' in self.registered_objects:
                    model = self.registered_objects['model']
                    state_dict = torch.load(model_path, weights_only=True)
                    
                    # Handle DDP wrapper
                    if hasattr(model, "module"):
                        model.module.load_state_dict(state_dict)
                    else:
                        model.load_state_dict(state_dict)
                    
                    logger.info("✓ Auto-loaded model state from JIT checkpoint")
                else:
                    logger.debug("No model state found in checkpoint")
                
                # Load optimizer state
                optimizer_path = os.path.join(checkpoint_dir, "optimizer.pt")
                if os.path.exists(optimizer_path) and 'optimizer' in self.registered_objects:
                    optimizer = self.registered_objects['optimizer']
                    state_dict = torch.load(optimizer_path, weights_only=True)
                    
                    # Handle wrapped optimizer
                    if hasattr(optimizer, "optimizer"):
                        optimizer.optimizer.load_state_dict(state_dict)
                    else:
                        optimizer.load_state_dict(state_dict)
                    
                    logger.info("✓ Auto-loaded optimizer state from JIT checkpoint")
                else:
                    logger.debug("No optimizer state found in checkpoint")
                
                # Load training metadata (iteration tracking)
                metadata_path = os.path.join(checkpoint_dir, "training_metadata.pt")
                if os.path.exists(metadata_path):
                    metadata = torch.load(metadata_path, weights_only=True)
                    self.training_state['iteration'] = metadata.get('iteration', 0)
                    logger.info(f"✓ Resuming from iteration {self.training_state['iteration']}")
                else:
                    logger.debug("No training metadata found in checkpoint")
                    
        except Exception as e:
            logger.error(f"Failed to load checkpoint state: {e}", exc_info=True)
            # Don't raise - allow training to continue from scratch if load fails

    def _create_jit_checkpoint(self) -> Optional[Checkpoint]:
        """Create checkpoint with current model/optimizer/metadata state.
        
        This creates a checkpoint containing:
        - model.pt: Model state_dict
        - optimizer.pt: Optimizer state_dict (if registered)
        - training_metadata.pt: Iteration and timestamp info
        
        Returns:
            Checkpoint object or None if no objects are registered
        """
        if not self.registered_objects:
            logger.debug("No registered objects, skipping checkpoint creation")
            return None
        
        tmpdir = tempfile.mkdtemp(prefix="auto_jit_checkpoint_")
        
        try:
            # Save model state
            if 'model' in self.registered_objects:
                model = self.registered_objects['model']
                model_path = os.path.join(tmpdir, "model.pt")
                
                # Handle DDP wrapper
                if hasattr(model, "module"):
                    torch.save(model.module.state_dict(), model_path)
                else:
                    torch.save(model.state_dict(), model_path)
                
                logger.debug("Saved model state to checkpoint")
            
            # Save optimizer state
            if 'optimizer' in self.registered_objects:
                optimizer = self.registered_objects['optimizer']
                optimizer_path = os.path.join(tmpdir, "optimizer.pt")
                
                # Handle wrapped optimizer
                if hasattr(optimizer, "optimizer"):
                    torch.save(optimizer.optimizer.state_dict(), optimizer_path)
                else:
                    torch.save(optimizer.state_dict(), optimizer_path)
                
                logger.debug("Saved optimizer state to checkpoint")
            
            # Save training metadata (automatic iteration tracking)
            training_metadata = {
                "iteration": self.training_state['iteration'],
                "timestamp": time.time(),
            }
            metadata_path = os.path.join(tmpdir, "training_metadata.pt")
            torch.save(training_metadata, metadata_path)
            
            logger.info(
                f"Created JIT checkpoint at iteration {self.training_state['iteration']}"
            )
            return Checkpoint.from_directory(tmpdir)
        
        except Exception as e:
            # Clean up temporary directory on error
            logger.error(f"Failed to create checkpoint: {e}", exc_info=True)
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise


# Global manager instance (one per worker process)
_auto_jit_manager = None


def _get_auto_jit_manager() -> AutoJITCheckpointManager:
    """Get or create the global auto JIT checkpoint manager.
    
    This ensures a single manager instance per worker process, maintaining
    consistent state across all checkpoint operations.
    
    Returns:
        The global AutoJITCheckpointManager instance
    """
    global _auto_jit_manager
    if _auto_jit_manager is None:
        _auto_jit_manager = AutoJITCheckpointManager()
        logger.debug("Created global AutoJITCheckpointManager")
    return _auto_jit_manager


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
                    torch.save(
                        optimizer.optimizer.state_dict(),
                        os.path.join(tmpdir, "optimizer.pt"),
                    )
                else:
                    torch.save(
                        optimizer.state_dict(), os.path.join(tmpdir, "optimizer.pt")
                    )

            # Save additional state if provided
            if additional_state is not None:
                torch.save(additional_state, os.path.join(tmpdir, "extra_state.pt"))

            logger.info(
                "Auto JIT checkpoint created with current model/optimizer state"
            )
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
                logger.warning(
                    "No training session found, auto JIT checkpoint not enabled"
                )
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
                    torch.save(
                        current_model.module.state_dict(),
                        os.path.join(tmpdir, "model.pt"),
                    )
                else:
                    torch.save(
                        current_model.state_dict(), os.path.join(tmpdir, "model.pt")
                    )
                logger.debug("Saved model state to JIT checkpoint")

            # Save optimizer state if available
            if current_optimizer is not None:
                if hasattr(current_optimizer, "optimizer"):
                    torch.save(
                        current_optimizer.optimizer.state_dict(),
                        os.path.join(tmpdir, "optimizer.pt"),
                    )
                else:
                    torch.save(
                        current_optimizer.state_dict(),
                        os.path.join(tmpdir, "optimizer.pt"),
                    )
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
            logger.info(
                f"Created JIT checkpoint with model, optimizer, and training metadata (iteration={training_metadata['iteration']})"
            )

            # Return checkpoint - persist_current_checkpoint will copy files before tmpdir is cleaned
            return Checkpoint.from_directory(tmpdir)
        except Exception:
            # Clean up tmpdir on error
            import shutil

            shutil.rmtree(tmpdir, ignore_errors=True)
            raise

    # Register the checkpoint function
    session.set_state("jit_checkpoint_fn", create_auto_jit_checkpoint)
    logger.info("Automatic JIT checkpointing enabled for model and optimizer")
