"""Checkpoint utilities for Ray Train.

This module provides simple, high-level APIs for checkpoint management.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _read_checkpoint_epoch_from_filesystem() -> Optional[int]:
    """Read checkpoint epoch from filesystem using multiple fallback strategies.
    
    This function tries to find the checkpoint epoch by:
    1. Reading our training_metadata.pt file from checkpoint directories
    2. Reading Ray's .metadata.json file from checkpoint directories
    3. Using Ray Train's auto-resume logic to find the latest checkpoint
    
    Returns:
        Epoch number if found, None otherwise
    """
    try:
        import torch
    except ImportError:
        logger.debug("PyTorch not available for reading checkpoint metadata")
        return None
    
    # Try to get storage path and experiment name from environment or context
    storage_path = os.getenv("RAY_TRAIN_CHECKPOINT_STORAGE_PATH")
    if not storage_path:
        # Try alternative environment variable names
        storage_path = os.getenv("RAY_AIR_CUSTOM_TAGS_DIR") or "/tmp/ray_results"
    
    # Try to get experiment name from session (RunConfig.name)
    experiment_name = None
    try:
        from ray.train._internal.session import get_session
        session = get_session()
        if session and hasattr(session, "experiment_name"):
            experiment_name = session.experiment_name
            logger.debug(f"Got experiment_name from session: {experiment_name}")
    except Exception as e:
        logger.debug(f"Failed to get experiment_name from session: {e}")
    
    # Try to get experiment name from context
    if not experiment_name:
        try:
            from ray.train import get_context
            context = get_context()
            if hasattr(context, "experiment_name"):
                experiment_name = context.experiment_name
                logger.debug(f"Got experiment_name from context: {experiment_name}")
        except Exception as e:
            logger.debug(f"Failed to get experiment_name from context: {e}")
    
    # If no experiment name from context, try environment
    if not experiment_name:
        experiment_name = os.getenv("RAY_AIR_EXPERIMENT_NAME") or os.getenv("RAY_TRAIN_EXPERIMENT_NAME")
        logger.debug(f"Got experiment_name from env: {experiment_name}")
    
    if not experiment_name:
        experiment_name = "default"
        logger.debug("Using default experiment name: default")
    
    logger.info(f"Searching for checkpoint in storage_path={storage_path}, experiment_name={experiment_name}")
    
    try:
        # Use Ray Train's auto-resume logic to find latest checkpoint
        from ray.train.torch.auto_resume import get_latest_checkpoint_for_auto_resume
        
        checkpoint_path = get_latest_checkpoint_for_auto_resume(storage_path, experiment_name)
        
        if not checkpoint_path:
            logger.debug("No checkpoint path found via auto-resume logic")
            return None
        
        checkpoint_dir = Path(checkpoint_path)
        
        # Try 1: Read our training_metadata.pt file
        training_metadata_path = checkpoint_dir / "training_metadata.pt"
        if training_metadata_path.exists():
            try:
                import torch
                metadata = torch.load(str(training_metadata_path), weights_only=True)
                epoch = metadata.get('iteration', 0)
                logger.debug(f"Found epoch {epoch} in training_metadata.pt")
                return epoch
            except Exception as e:
                logger.debug(f"Failed to read training_metadata.pt: {e}")
        
        # Try 2: Read Ray's .metadata.json file
        metadata_json_path = checkpoint_dir / ".metadata.json"
        if metadata_json_path.exists():
            try:
                with open(metadata_json_path, 'r') as f:
                    metadata = json.load(f)
                    # Look for epoch/iteration in metadata
                    epoch = metadata.get('epoch') or metadata.get('iteration') or metadata.get('epoch_num')
                    if epoch is not None:
                        logger.debug(f"Found epoch {epoch} in .metadata.json")
                        return epoch
            except Exception as e:
                logger.debug(f"Failed to read .metadata.json: {e}")
        
        logger.debug("No usable metadata found in checkpoint files")
        return None
        
    except Exception as e:
        logger.debug(f"Failed to read checkpoint epoch from filesystem: {e}")
        return None


def get_checkpoint_start_epoch() -> int:
    """Get the starting epoch for training with automatic checkpoint resume.
    
    This function returns the epoch number to start training from:
    - Returns 0 if no checkpoint exists (fresh training)
    - Returns N if resuming from a checkpoint at epoch N
    
    Call this AFTER prepare_model() and prepare_optimizer() to get the
    epoch to start your training loop from. The prepare_* functions will
    have already loaded the model and optimizer state from the checkpoint.
    
    This function uses a multi-source approach to determine the resume epoch:
    1. First, tries to get epoch from AutoJITCheckpointManager
    2. Then, tries to directly read from train.get_checkpoint() with retries
    3. Finally, tries to read from filesystem using auto-resume logic
    
    Example:
        def train_func(config):
            model = prepare_model(model)          # Loads model weights automatically
            optimizer = prepare_optimizer(optim) # Loads optimizer state automatically
            
            # Get the epoch to start from
            start_epoch = train.get_checkpoint_start_epoch()
            
            for epoch in range(start_epoch, config["num_epochs"]):
                train.report({"epoch": epoch, "loss": loss})
    
    Returns:
        Starting epoch number (0 for fresh training, N for resume from epoch N)
    """
    # Strategy 1: Try AutoJITCheckpointManager first (most reliable if it worked)
    try:
        from ray.train.torch.jit_checkpoint_utils import _get_auto_jit_manager
        
        manager = _get_auto_jit_manager()
        start_epoch = manager.training_state.get('iteration', 0)
        
        if start_epoch > 0:
            logger.info(f"Resuming training from epoch {start_epoch} (from AutoJITCheckpointManager)")
            return start_epoch
        else:
            logger.debug("AutoJITCheckpointManager shows epoch 0, trying other sources...")
    except Exception as e:
        logger.debug(f"AutoJITCheckpointManager not available: {e}")
    
    # Strategy 2: Try to get checkpoint directly from Ray Train session with retries
    import time
    for attempt in range(10):
        try:
            checkpoint = __get_checkpoint_with_fallback()
            if checkpoint:
                logger.debug(f"Checkpoint found via train.get_checkpoint() on attempt {attempt + 1}")
                # Try to read metadata from checkpoint
                epoch = __read_epoch_from_checkpoint(checkpoint)
                if epoch is not None and epoch > 0:
                    logger.info(f"Resuming training from epoch {epoch} (from train.get_checkpoint())")
                    return epoch
                else:
                    logger.debug(f"No valid epoch found in checkpoint (got {epoch})")
            else:
                logger.debug(f"No checkpoint yet on attempt {attempt + 1}")
        except Exception as e:
            logger.debug(f"Attempt {attempt + 1} to get checkpoint failed: {e}")
        
        if attempt < 9:
            time.sleep(0.2)
    
    # Strategy 3: Try filesystem reading as final fallback
    try:
        epoch = _read_checkpoint_epoch_from_filesystem()
        if epoch is not None:
            logger.info(f"Resuming training from epoch {epoch} (from filesystem)")
            return epoch
    except Exception as e:
        logger.debug(f"Filesystem reading failed: {e}")
    
    # Fallback: Default to 0 with warning
    logger.warning(
        "Could not determine resume epoch from any source. Starting from epoch 0. "
        "This may be normal for a fresh training run."
    )
    return 0


def __get_checkpoint_with_fallback():
    """Internal helper to get checkpoint with proper imports."""
    from ray.train import get_checkpoint
    return get_checkpoint()


def __read_epoch_from_checkpoint(checkpoint):
    """Internal helper to read epoch from checkpoint object."""
    try:
        import torch
        
        with checkpoint.as_directory() as checkpoint_dir:
            # Try our training_metadata.pt file
            training_metadata_path = os.path.join(checkpoint_dir, "training_metadata.pt")
            if os.path.exists(training_metadata_path):
                metadata = torch.load(training_metadata_path, weights_only=True)
                return metadata.get('iteration', 0)
            
            # Try Ray's .metadata.json file
            metadata_json_path = os.path.join(checkpoint_dir, ".metadata.json")
            if os.path.exists(metadata_json_path):
                with open(metadata_json_path, 'r') as f:
                    metadata = json.load(f)
                    return metadata.get('epoch') or metadata.get('iteration')
            
            return None
    except Exception as e:
        logger.debug(f"Failed to read epoch from checkpoint: {e}")
        return None


def get_checkpoint_start_iteration() -> int:
    """Alias for get_checkpoint_start_epoch() for backwards compatibility.
    
    Returns:
        Starting iteration number (same as epoch in most cases)
    """
    return get_checkpoint_start_epoch()

