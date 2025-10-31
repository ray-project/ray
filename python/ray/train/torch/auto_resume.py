"""
Automatic checkpoint resumption utilities for Ray Train.

Enables automatic resume from the latest checkpoint when environment
variable is set, supporting UI-driven suspend/resume workflows.
"""

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def get_latest_checkpoint_for_auto_resume(
    storage_path: str,
    experiment_name: str
) -> Optional[str]:
    """Find the latest checkpoint directory for automatic resume.
    
    This searches the experiment directory for checkpoint directories
    and returns the path to the most recent one based on checkpoint index.
    
    Useful for UI-driven workflows where suspend/resume happens without
    code changes and the latest checkpoint should be automatically loaded.
    
    Args:
        storage_path: The storage path (e.g., "/mnt/ray-checkpoints")
        experiment_name: The experiment name (RunConfig.name)
    
    Returns:
        Path to the latest checkpoint directory, or None if no checkpoints found
    """
    try:
        experiment_path = Path(storage_path) / experiment_name
        
        if not experiment_path.exists():
            logger.debug(f"No experiment directory found at {experiment_path}")
            return None
        
        # Find all trial directories
        trial_dirs = [d for d in experiment_path.iterdir() if d.is_dir() and d.name.startswith("TorchTrainer_")]
        
        if not trial_dirs:
            logger.debug(f"No trial directories found in {experiment_path}")
            return None
        
        # Sort trials by mtime (newest first)
        trial_dirs_sorted = sorted(trial_dirs, key=lambda p: p.stat().st_mtime, reverse=True)
        
        # Find the most recent trial directory that actually has checkpoints
        # This handles the case where a new job creates an empty trial directory
        for trial_dir in trial_dirs_sorted:
            checkpoint_dirs = sorted([d for d in trial_dir.iterdir() if d.is_dir() and d.name.startswith("checkpoint_")])
            
            if checkpoint_dirs:
                # Return the highest checkpoint index from this trial
                latest_checkpoint = checkpoint_dirs[-1]
                logger.info(f"Found latest checkpoint for auto-resume in {trial_dir.name}: {latest_checkpoint}")
                return str(latest_checkpoint)
        
        # No checkpoints found in any trial
        logger.debug(f"No checkpoints found in any trial directory")
        return None
        
    except Exception as e:
        logger.warning(f"Failed to find latest checkpoint: {e}")
        return None


def should_auto_resume() -> bool:
    """Check if automatic checkpoint resume is enabled via environment variable.
    
    Returns:
        True if RAY_TRAIN_AUTO_RESUME=true
    """
    return os.getenv("RAY_TRAIN_AUTO_RESUME", "").lower() == "true"

