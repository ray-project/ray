"""
Training Loop Utilities for XGBoost with Ray Train

This module provides utilities for preparing datasets and parameters for XGBoost training
with Ray Train, following XGBoost's external memory best practices.

Key components:
- prepare_dataset: Prepare XGBoost DMatrix with automatic memory optimization
- prepare_datasets_and_params: Prepare both training and validation datasets with optimized parameters
- get_recommended_params: Get hardware-aware XGBoost parameters for external memory training

This implementation follows XGBoost's external memory best practices:
- Uses ExtMemQuantileDMatrix for hist tree method (required for external memory)
- Implements streaming iteration with minimal memory footprint
- Supports GPU training with RMM integration
- Optimized for depthwise grow policy performance
- Follows XGBoost 3.0+ external memory recommendations
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    import xgboost

from ray.train.v2.xgboost._external_memory_utils import (
    _create_smart_dmatrix,
)
from ray.train.v2.xgboost._param_utils import (
    _get_optimal_xgboost_params_for_external_memory,
    _validate_xgboost_params,
)
from ray.train.v2.xgboost._system_utils import (
    _get_node_memory_limit_gb,
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
    all the complexity internally. It follows XGBoost's external memory best practices.

    Following XGBoost official recommendations:
    - Uses ExtMemQuantileDMatrix for external memory training (required for hist tree method)
    - Implements streaming iteration with minimal memory footprint
    - Supports GPU training with RMM integration
    - Optimized for depthwise grow policy performance

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
    use_single_page_concatenation: bool = False,
    **user_params,
) -> Dict[str, Any]:
    """Get hardware-aware XGBoost parameters optimized for external memory training.

    This function generates optimal XGBoost parameters based on your hardware configuration
    and training requirements, following XGBoost's external memory best practices.

    Following XGBoost official recommendations:
    - tree_method="hist" is mandatory for external memory training
    - grow_policy="depthwise" provides best performance for external memory
    - Batch size should be ~10GB per batch for 64GB RAM systems
    - Avoid small batch sizes (e.g., 32 samples) as they hurt performance

    Args:
        objective: XGBoost objective function (e.g., "binary:logistic", "reg:squarederror")
        use_gpu: Whether to use GPU training
        memory_constraint_gb: Memory constraint in GB (if None, auto-detected)
        enable_categorical: Whether to enable categorical features
        use_single_page_concatenation: Whether to use single page concatenation (GPU only)
        **user_params: Additional user-specified parameters

    Returns:
        Dictionary of optimized XGBoost parameters for external memory training

    Example:
        # Get GPU-optimized parameters for binary classification
        params = get_recommended_params(
            objective="binary:logistic",
            use_gpu=True,
            enable_categorical=True
        )

        # Add custom parameters
        params.update({
            "eta": 0.1,
            "max_depth": 6,
            "num_boost_round": 100
        })
    """
    # Auto-detect memory constraint if not provided
    if memory_constraint_gb is None:
        memory_constraint_gb = _get_node_memory_limit_gb()

    # Get storage performance info for optimization
    storage_info = _get_storage_performance_info()
    storage_type = storage_info.get("storage_type", "nvme")

    # Get optimal parameters for external memory training
    params = _get_optimal_xgboost_params_for_external_memory(
        objective=objective,
        use_gpu=use_gpu,
        memory_constraint_gb=memory_constraint_gb,
        enable_categorical=enable_categorical,
        use_single_page_concatenation=use_single_page_concatenation,
        storage_type=storage_type,
    )

    # Override with user parameters
    params.update(user_params)

    # Validate parameters for external memory training
    params = _validate_xgboost_params(params, use_external_memory=True)

    return params


def prepare_datasets_and_params(
    train_dataset_shard,
    label_column: Union[str, List[str]],
    eval_dataset_shard=None,
    objective: str = "reg:squarederror",
    use_gpu: bool = False,
    memory_constraint_gb: Optional[float] = None,
    enable_categorical: bool = False,
    use_single_page_concatenation: bool = False,
    force_external_memory: bool = False,
    **user_params,
) -> tuple:
    """Prepare both training and validation datasets with optimized parameters.

    This is a convenience function that combines dataset preparation and parameter
    optimization in a single call, following XGBoost's external memory best practices.

    Args:
        train_dataset_shard: Training dataset shard from ray.train.get_dataset_shard()
        label_column: Name of the label column(s) in the dataset
        eval_dataset_shard: Validation dataset shard (optional)
        objective: XGBoost objective function
        use_gpu: Whether to use GPU training
        memory_constraint_gb: Memory constraint in GB (if None, auto-detected)
        enable_categorical: Whether to enable categorical features
        use_single_page_concatenation: Whether to use single page concatenation (GPU only)
        force_external_memory: If True, always use external memory regardless of size
        **user_params: Additional user-specified parameters

    Returns:
        Tuple of (training_dmatrix, validation_dmatrix, optimized_params)

    Example:
        def train_fn_per_worker(config: dict):
            train_ds = ray.train.get_dataset_shard("train")
            eval_ds = ray.train.get_dataset_shard("validation")

            # All optimization handled automatically - one line!
            dtrain, deval, params = prepare_datasets_and_params(
                train_ds,
                label_column="target",
                eval_dataset_shard=eval_ds,
                objective="binary:logistic",
                use_gpu=True,  # Automatic GPU optimization
                eta=0.1,       # Custom parameters as needed
                max_depth=6
            )

            # Standard XGBoost training - all complexity hidden
            bst = xgboost.train(
                params,
                dtrain=dtrain,
                evals=[(deval, "validation")],
                num_boost_round=100,
                callbacks=[RayTrainReportCallback()],
            )
    """
    # Prepare training dataset
    dtrain = prepare_dataset(
        train_dataset_shard,
        label_column=label_column,
        force_external_memory=force_external_memory,
        enable_categorical=enable_categorical,
    )

    # Prepare validation dataset if provided
    deval = None
    if eval_dataset_shard is not None:
        deval = prepare_dataset(
            eval_dataset_shard,
            label_column=label_column,
            force_external_memory=force_external_memory,
            enable_categorical=enable_categorical,
        )

    # Get optimized parameters
    params = get_recommended_params(
        objective=objective,
        use_gpu=use_gpu,
        memory_constraint_gb=memory_constraint_gb,
        enable_categorical=enable_categorical,
        use_single_page_concatenation=use_single_page_concatenation,
        **user_params,
    )

    return dtrain, deval, params
