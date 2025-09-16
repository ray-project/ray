"""
XLA utilities for Ray Train TorchTrainer.

This module provides utilities for working with XLA SPMD meshes
in Ray Train training loops.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import torch_xla.distributed.spmd as xs
    _XLA_AVAILABLE = True
except ImportError:
    _XLA_AVAILABLE = False


def get_xla_mesh() -> Optional[xs.Mesh]:
    """
    Get the XLA SPMD mesh that was created during worker bootstrap.
    
    This function returns the mesh that was automatically created
    during the XLA backend initialization. The mesh is configured
    based on the available TPU devices and process topology.
    
    For a 4x4 TPU slice with 4 workers (4 hosts, each with 4 TPU chips):
    - The mesh will have shape (4, 4) with axes ("data", "model")
    - This represents 4-way data parallelism and 4-way model parallelism
    
    Returns:
        The XLA SPMD mesh if XLA backend is being used, None otherwise.
        
    Raises:
        RuntimeError: If called outside of a Ray Train training context
            or if XLA backend is not being used.
            
    Example:
        .. testcode::
            :skipif: True
            
            def train_loop_per_worker(config):
                # Get the XLA mesh for SPMD operations
                mesh = ray.train.torch.xla.get_xla_mesh()
                if mesh is not None:
                    # Use the mesh for sharding operations
                    import torch_xla.distributed.spmd as xs
                    
                    # Shard your model across the mesh
                    model = xs.mark_sharding(model, mesh, xs.Shard(0))
                    
                    # Use the mesh for data parallelism
                    with xs.Mesh(mesh.devices, mesh.shape, mesh.axis_names):
                        # Your training code here
                        pass
    """
    if not _XLA_AVAILABLE:
        logger.warning("torch_xla is not available. get_xla_mesh() will return None.")
        return None
    
    try:
        import ray.train as train
        context = train.get_context()
        return context.get_xla_mesh()
    except Exception as e:
        logger.warning(f"Could not get XLA mesh from train context: {e}")
        return None


def is_xla_backend() -> bool:
    """
    Check if the current training is using XLA backend.
    
    Returns:
        True if XLA backend is being used, False otherwise.
    """
    if not _XLA_AVAILABLE:
        return False
    
    try:
        import torch_xla.runtime as xr
        return xr.is_initialized()
    except Exception:
        return False
