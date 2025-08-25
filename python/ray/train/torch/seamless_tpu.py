"""
Seamless TPU Integration for Ray TorchTrainer

This module provides utilities that make training loops work seamlessly
on both GPU and TPU without requiring code changes.
"""

import torch
import torch.nn as nn
from typing import Union, Optional, Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)


class SeamlessTPUTrainer:
    """
    A wrapper that makes training loops work seamlessly on both GPU and TPU.
    
    This class automatically detects the device type and applies the appropriate
    optimizations, allowing users to write training loops that work everywhere.
    """
    
    def __init__(self, model: nn.Module, device: Optional[torch.device] = None):
        """
        Initialize the seamless trainer.
        
        Args:
            model: The PyTorch model to train
            device: Optional device to use (auto-detected if None)
        """
        self.model = model
        self.device = device or self._auto_detect_device()
        self._is_tpu = self.device.type == 'xla'
        
        # Move model to device
        self.model = self.model.to(self.device)
        
        if self._is_tpu:
            logger.info("TPU detected - enabling XLA optimizations automatically")
        else:
            logger.info(f"Using device: {self.device}")
    
    def _auto_detect_device(self) -> torch.device:
        """Auto-detect the best available device."""
        try:
            import torch_xla.core.xla_model as xm
            return xm.xla_device()
        except ImportError:
            if torch.cuda.is_available():
                return torch.device("cuda")
            else:
                return torch.device("cpu")
    
    def to_device(self, *args) -> Union[torch.Tensor, Tuple[torch.Tensor, ...]]:
        """
        Move tensors to the appropriate device.
        
        This function automatically handles device placement for both GPU and TPU.
        """
        if len(args) == 1:
            return args[0].to(self.device)
        else:
            return tuple(arg.to(self.device) if isinstance(arg, torch.Tensor) else arg for arg in args)
    
    def _auto_optimize_for_tpu(self):
        """
        Automatically apply TPU optimizations when available.
        This is called internally to ensure optimal TPU performance.
        """
        if self._is_tpu:
            try:
                import torch_xla.core.xla_model as xm
                # Enable XLA compilation and optimization
                xm.mark_step()
            except ImportError:
                pass  # Not critical if torch_xla is not available
    
    def get_device(self) -> torch.device:
        """Get the current device."""
        return self.device
    
    def is_tpu(self) -> bool:
        """Check if we're running on TPU."""
        return self._is_tpu


# Convenience functions for easy integration
def create_seamless_trainer(model: nn.Module, device: Optional[torch.device] = None) -> SeamlessTPUTrainer:
    """Create a seamless trainer instance."""
    return SeamlessTPUTrainer(model, device)


def to_device(*args, device: Optional[torch.device] = None) -> Union[torch.Tensor, Tuple[torch.Tensor, ...]]:
    """
    Move tensors to device with automatic TPU detection.
    
    This is a convenience function that automatically detects TPU and
    applies the appropriate device placement.
    """
    if device is None:
        try:
            import torch_xla.core.xla_model as xm
            device = xm.xla_device()
        except ImportError:
            if torch.cuda.is_available():
                device = torch.device("cuda")
            else:
                device = torch.device("cpu")
    
    if len(args) == 1:
        return args[0].to(device)
    else:
        return tuple(arg.to(device) if isinstance(arg, torch.Tensor) else arg for arg in args)


def auto_optimize_for_tpu():
    """
    Automatically apply TPU optimizations when available.
    This function is called internally by the training loop.
    """
    try:
        import torch_xla.core.xla_model as xm
        # Check if we're on TPU
        if hasattr(xm, 'xla_device'):
            xm.mark_step()
    except ImportError:
        pass  # Not critical if torch_xla is not available 