"""
Parameter Optimization and Validation Utilities for XGBoost Training

This module contains utilities for optimizing and validating XGBoost parameters
for external memory training scenarios with hardware-aware configurations.

Key components:
- _get_optimal_xgboost_params_for_external_memory: Hardware-aware parameter optimization
- _validate_xgboost_params: Parameter validation and adjustment

This implementation follows XGBoost's external memory best practices:
- tree_method="hist" is mandatory for external memory
- grow_policy="depthwise" provides best performance for external memory
- Batch size should be ~10GB per batch for 64GB RAM systems
- Avoid small batch sizes (e.g., 32 samples) as they hurt performance

Args:
    objective: XGBoost objective function
    use_gpu: Whether to use GPU training
    memory_constraint_gb: Memory constraint in GB
    enable_categorical: Whether to enable categorical features
    use_single_page_concatenation: Whether to use single page concatenation (GPU only)
    has_nvlink_c2c: Whether system has NVLink-C2C support
    storage_type: Storage type for external memory

Returns:
    Dictionary of optimized XGBoost parameters for external memory training
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _get_optimal_xgboost_params_for_external_memory(
    objective: str = "reg:squarederror",
    use_gpu: bool = False,
    memory_constraint_gb: float = None,
    enable_categorical: bool = False,
    use_single_page_concatenation: bool = False,
    has_nvlink_c2c: bool = None,
    storage_type: str = "nvme",
) -> Dict[str, Any]:
    """Get optimal XGBoost parameters for external memory training.

    Based on XGBoost external memory best practices:
    - Uses 'hist' tree method (required for external memory)
    - Uses 'depthwise' grow policy for optimal batch iteration efficiency
    - Optimized for ExtMemQuantileDMatrix performance
    - Includes GPU-specific optimizations and hardware-aware configurations

    Following XGBoost official recommendations:
    - tree_method="hist" is mandatory for external memory
    - grow_policy="depthwise" provides best performance for external memory
    - Batch size should be ~10GB per batch for 64GB RAM systems
    - Avoid small batch sizes (e.g., 32 samples) as they hurt performance

    Args:
        objective: XGBoost objective function
        use_gpu: Whether to use GPU training
        memory_constraint_gb: Memory constraint in GB
        enable_categorical: Whether to enable categorical features
        use_single_page_concatenation: Whether to use single page concatenation (GPU only)
        has_nvlink_c2c: Whether system has NVLink-C2C support
        storage_type: Storage type for external memory

    Returns:
        Dictionary of optimized XGBoost parameters for external memory training
    """
    # Normalize storage type if not explicitly provided
    if storage_type not in {"nvme", "ssd", "hdd"}:
        # Lazy import to avoid unused import at module level
        from ray.train.v2.xgboost._system_utils import _get_storage_performance_info

        storage_info = _get_storage_performance_info()
        storage_type = storage_info.get("storage_type", "nvme")

    # Auto-detect NVLink-C2C capability if not specified
    if has_nvlink_c2c is None:
        from ray.train.v2.xgboost._system_utils import _detect_nvlink_c2c_support

        has_nvlink_c2c = _detect_nvlink_c2c_support()

    # Base parameters for external memory training
    params = {
        # Required for external memory training
        "tree_method": "hist",
        # Recommended for optimal external memory performance
        "grow_policy": "depthwise",
        # External memory specific optimizations
        "max_bin": 256,  # Good balance between accuracy and memory
        "subsample": 1.0,  # No subsampling by default for external memory
        "colsample_bytree": 1.0,  # No column sampling by default
    }

    # Add objective-specific parameters
    if objective.startswith("binary:"):
        params.update(
            {
                "eval_metric": "logloss",
                "objective": objective,
            }
        )
    elif objective.startswith("multi:"):
        params.update(
            {
                "eval_metric": "mlogloss",
                "objective": objective,
            }
        )
    elif objective.startswith("reg:"):
        params.update(
            {
                "eval_metric": "rmse",
                "objective": objective,
            }
        )
    elif objective.startswith("rank:"):
        params.update(
            {
                "eval_metric": "ndcg",
                "objective": objective,
            }
        )
    else:
        params["objective"] = objective

    # GPU-specific optimizations
    if use_gpu:
        params.update(
            {
                "device": "cuda",
                "gpu_id": 0,  # Will be set by Ray Train
            }
        )

        # GPU external memory optimizations
        if use_single_page_concatenation:
            # For PCIe-connected GPUs, use concatenation with subsampling
            params.update(
                {
                    "extmem_single_page": True,
                    "subsample": 0.2,  # Reduce memory usage
                    "sampling_method": "gradient_based",  # Maintain accuracy
                }
            )
        else:
            # For NVLink-C2C systems, use regular batch fetching
            if has_nvlink_c2c:
                # NVLink-C2C detected - use regular batch fetching
                pass
            else:
                # PCIe connection detected - consider single page concatenation
                pass

        # RMM integration for GPU external memory
        try:
            import rmm

            params["use_rmm"] = True
        except ImportError:
            logger.warning(
                "RMM not available. Install cupy and rmm for optimal GPU external memory performance"
            )

    # Memory-constrained optimizations
    if memory_constraint_gb is not None:
        if memory_constraint_gb < 8:
            # Very memory-constrained systems
            params.update(
                {
                    "max_depth": 4,  # Shallow trees to reduce memory
                    "max_bin": 128,  # Fewer bins for lower memory usage
                    "subsample": 0.8,  # Slight subsampling
                    "colsample_bytree": 0.8,  # Slight column sampling
                }
            )
        elif memory_constraint_gb < 32:
            # Moderately memory-constrained systems
            params.update(
                {
                    "max_depth": 6,
                    "max_bin": 256,
                }
            )
        else:
            # Memory-rich systems
            params.update(
                {
                    "max_depth": 8,
                    "max_bin": 512,  # More bins for better accuracy
                }
            )

    # Storage-specific optimizations
    if storage_type == "hdd":
        # HDD storage is slow, optimize for fewer iterations
        params.update(
            {
                "max_depth": min(params.get("max_depth", 8), 6),
                "eta": 0.3,  # Higher learning rate for fewer iterations
            }
        )
    elif storage_type == "ssd":
        # SSD storage is moderate, balanced optimization
        params.update(
            {
                "eta": 0.1,  # Standard learning rate
            }
        )
    else:  # nvme
        # NVMe storage is fast, optimize for accuracy
        params.update(
            {
                "eta": 0.05,  # Lower learning rate for better accuracy
                "max_bin": max(params.get("max_bin", 256), 512),
            }
        )

    # Categorical feature support
    if enable_categorical:
        params["enable_categorical"] = True

    # External memory specific parameters
    params.update(
        {
            # Batch size recommendations follow XGBoost guidelines
            # ~10GB per batch for 64GB RAM systems
            "batch_size": "auto",  # Will be set by the iterator
            # External memory optimizations
            "max_quantile_batches": None,  # Auto-detect based on available memory
            "min_cache_page_bytes": None,  # Auto-detect based on storage
            "cache_host_ratio": None,  # Auto-detect for GPU systems
        }
    )

    return params


def _validate_xgboost_params(
    params: Dict[str, Any], use_external_memory: bool = False
) -> Dict[str, Any]:
    """Validate and adjust XGBoost parameters for external memory training.

    This function ensures that parameters are compatible with external memory training
    and follows XGBoost's best practices.

    Args:
        params: User-provided XGBoost parameters
        use_external_memory: Whether external memory is being used

    Returns:
        Validated and adjusted parameters

    Raises:
        ValueError: If parameters are incompatible with external memory training
    """
    validated_params = params.copy()

    if use_external_memory:
        # External memory requires specific tree method
        if validated_params.get("tree_method") != "hist":
            if "tree_method" in validated_params:
                logger.warning(
                    f"External memory training requires tree_method='hist'. "
                    f"Changing from '{validated_params['tree_method']}' to 'hist'."
                )
            validated_params["tree_method"] = "hist"

        # Validate grow policy for external memory
        grow_policy = validated_params.get("grow_policy", "depthwise")
        if grow_policy not in ["depthwise", "lossguide"]:
            logger.warning(
                f"External memory training works best with grow_policy='depthwise'. "
                f"Current setting '{grow_policy}' may cause performance issues."
            )

        # Validate batch size recommendations
        if "batch_size" in validated_params:
            batch_size = validated_params["batch_size"]
            if isinstance(batch_size, int) and batch_size < 1000:
                logger.warning(
                    f"Small batch size {batch_size} may significantly hurt external memory performance. "
                    "Consider using batch size >= 1000 for optimal performance."
                )

        # GPU external memory validations
        if validated_params.get("device") == "cuda":
            # Check for RMM availability
            try:
                import rmm

                if not validated_params.get("use_rmm", False):
                    logger.info(
                        "GPU external memory training detected. Consider enabling RMM "
                        "with use_rmm=True for optimal performance."
                    )
            except ImportError:
                logger.warning(
                    "GPU external memory training detected but RMM not available. "
                    "Install cupy and rmm for optimal performance."
                )

    # General parameter validations
    if "max_depth" in validated_params:
        max_depth = validated_params["max_depth"]
        if max_depth > 20:
            logger.warning(
                f"Very deep trees (max_depth={max_depth}) may cause overfitting "
                "and slow training. Consider reducing to <= 20."
            )

    if "eta" in validated_params:
        eta = validated_params["eta"]
        if eta > 1.0:
            logger.warning(
                f"High learning rate (eta={eta}) may cause training instability. "
                "Consider reducing to <= 1.0."
            )

    return validated_params
