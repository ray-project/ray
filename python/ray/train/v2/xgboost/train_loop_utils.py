"""
XGBoost Training Loop Utilities

This module provides high-level utilities for XGBoost training that automatically
handle external memory optimization, hardware detection, and parameter tuning.
These functions are designed to be used within train_loop_per_worker functions
to provide seamless external memory training with optimal performance.
"""

import logging
from typing import Dict, Any, Union, List, Optional

from ray.train.v2.xgboost._external_memory_utils import (
    _create_smart_dmatrix,
    _create_external_memory_dmatrix,
)
from ray.train.v2.xgboost._param_utils import (
    _get_optimal_xgboost_params_for_external_memory,
    _validate_xgboost_params,
)
from ray.train.v2.xgboost._system_utils import (
    _detect_numa_configuration,
    _get_storage_performance_info,
)

logger = logging.getLogger(__name__)


def prepare_dataset(
    dataset_shard,
    label_column: Union[str, List[str]],
    force_external_memory: bool = False,
    feature_types: Optional[List[str]] = None,
    missing: Optional[float] = None,
    memory_limit_gb: Optional[float] = None,
) -> "xgboost.DMatrix":
    """Prepare an XGBoost DMatrix with automatic memory optimization.

    This function automatically analyzes the dataset size and available cluster memory
    to choose the optimal strategy (materialization vs external memory) and handles
    all the complexity internally.

    Args:
        dataset_shard: Ray Data DataIterator from ray.train.get_dataset_shard()
        label_column: Name of the label column(s) in the dataset
        force_external_memory: If True, always use external memory regardless of size
        feature_types: List of feature types for XGBoost (e.g., ['int', 'float', 'categorical'])
        missing: Value to be treated as missing (default: NaN)
        memory_limit_gb: Optional memory limit in GB. If None, automatically calculated

    Returns:
        XGBoost DMatrix optimized for the dataset size and available memory

    Example:
        def train_fn_per_worker(config: dict):
            train_ds = ray.train.get_dataset_shard("train")
            eval_ds = ray.train.get_dataset_shard("validation")

            # Automatic optimization - no manual configuration needed
            dtrain = prepare_dataset(train_ds, label_column="target")
            deval = prepare_dataset(eval_ds, label_column="target")

            # Use with any XGBoost parameters
            bst = xgboost.train(params, dtrain, evals=[(deval, "validation")])
    """
    return _create_smart_dmatrix(
        dataset_shard=dataset_shard,
        label_column=label_column,
        force_external_memory=force_external_memory,
        feature_types=feature_types,
        missing=missing,
        memory_limit_gb=memory_limit_gb,
    )


def get_recommended_params(
    objective: str = "reg:squarederror",
    use_gpu: bool = False,
    memory_constraint_gb: Optional[float] = None,
    enable_categorical: bool = False,
    **user_params,
) -> Dict[str, Any]:
    """Get recommended XGBoost parameters with hardware-aware optimization.

    This function automatically detects the system configuration (storage type,
    NUMA topology, GPU capabilities) and returns optimized parameters for
    external memory training.

    Args:
        objective: XGBoost objective function
        use_gpu: Whether to use GPU training
        memory_constraint_gb: Available memory in GB for optimization
        enable_categorical: Whether to enable categorical feature support
        **user_params: Additional user-specified parameters (will override defaults)

    Returns:
        Dictionary of optimized XGBoost parameters

    Example:
        def train_fn_per_worker(config: dict):
            # Get hardware-optimized parameters automatically
            params = get_recommended_params(
                objective="binary:logistic",
                use_gpu=True,
                eta=0.1,  # User parameters override defaults
                max_depth=6
            )

            bst = xgboost.train(params, dtrain, ...)
    """
    # Detect system configuration
    storage_info = _get_storage_performance_info()
    numa_info = _detect_numa_configuration()

    # Log system detection results
    if numa_info["performance_impact"] == "high":
        logger.info(
            "Multi-socket system detected. For optimal performance, consider NUMA affinity configuration. "
            f"Recommendations: {numa_info['recommendations'][:2]}"
        )

    if storage_info["performance_rating"] == "poor":
        logger.warning(
            f"Storage type '{storage_info['storage_type']}' may limit external memory performance. "
            "Consider using NVMe SSD for optimal training speed."
        )
    elif storage_info["performance_rating"] == "excellent":
        logger.info(f"Excellent storage detected: {storage_info['storage_type']}")

    # Get hardware-optimized parameters
    recommended_params = _get_optimal_xgboost_params_for_external_memory(
        objective=objective,
        use_gpu=use_gpu,
        memory_constraint_gb=memory_constraint_gb,
        enable_categorical=enable_categorical,
        storage_type=storage_info.get("storage_type", "nvme"),
        has_nvlink_c2c=None,  # Auto-detect
        use_single_page_concatenation=False,  # Conservative default
    )

    # Override with user parameters
    recommended_params.update(user_params)

    # Validate the final parameters
    validated_params = _validate_xgboost_params(
        recommended_params, use_external_memory=True
    )

    return validated_params


def prepare_datasets_and_params(
    train_dataset_shard,
    label_column: Union[str, List[str]],
    eval_dataset_shard=None,
    objective: str = "reg:squarederror",
    use_gpu: bool = False,
    enable_categorical: bool = False,
    **user_params,
) -> tuple:
    """One-stop function to prepare datasets and parameters for XGBoost training.

    This is the highest-level utility that handles everything automatically:
    - Dataset preparation with memory optimization
    - Hardware detection and parameter optimization
    - Validation dataset handling

    Args:
        train_dataset_shard: Training dataset from ray.train.get_dataset_shard()
        label_column: Name of the label column(s)
        eval_dataset_shard: Optional evaluation dataset
        objective: XGBoost objective function
        use_gpu: Whether to use GPU training
        enable_categorical: Whether to enable categorical feature support
        **user_params: Additional user-specified parameters

    Returns:
        Tuple of (dtrain, deval, params) where deval is None if no eval dataset provided

    Example:
        def train_fn_per_worker(config: dict):
            train_ds = ray.train.get_dataset_shard("train")
            eval_ds = ray.train.get_dataset_shard("validation")

            # Everything optimized automatically
            dtrain, deval, params = prepare_datasets_and_params(
                train_ds,
                label_column="target",
                eval_dataset_shard=eval_ds,
                objective="binary:logistic",
                use_gpu=True,
                eta=0.1  # Custom parameters
            )

            bst = xgboost.train(params, dtrain, evals=[(deval, "validation")])
    """
    # Prepare training dataset
    dtrain = prepare_dataset(train_dataset_shard, label_column=label_column)

    # Prepare evaluation dataset if provided
    deval = None
    if eval_dataset_shard is not None:
        deval = prepare_dataset(eval_dataset_shard, label_column=label_column)

    # Get optimized parameters
    params = get_recommended_params(
        objective=objective,
        use_gpu=use_gpu,
        enable_categorical=enable_categorical,
        **user_params,
    )

    return dtrain, deval, params
