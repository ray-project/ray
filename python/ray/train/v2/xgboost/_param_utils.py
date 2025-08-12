"""
Parameter Optimization and Validation Utilities for XGBoost Training

This module contains utilities for optimizing and validating XGBoost parameters
for external memory training scenarios with hardware-aware configurations.

Key components:
- _get_optimal_xgboost_params_for_external_memory: Hardware-aware parameter optimization
- _validate_xgboost_params: Parameter validation and adjustment
- _validate_external_memory_config: Comprehensive external memory configuration validation
"""

import logging
from typing import Dict, Any, Union, List, Optional
import warnings

from ray.train.v2.xgboost._system_utils import (
    _get_storage_performance_info,
    _detect_numa_configuration,
)

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
    """
    # Auto-detect NVLink-C2C capability if not specified
    if has_nvlink_c2c is None and use_gpu:
        try:
            import pynvml

            pynvml.nvmlInit()
            # Try to detect Grace-Hopper or similar architecture
            # This is a simplified detection - in practice, you'd check specific GPU models
            device_count = pynvml.nvmlDeviceGetCount()
            if device_count > 0:
                handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                name = pynvml.nvmlDeviceGetName(handle).decode("utf-8")
                # Grace-Hopper and similar high-bandwidth interconnect systems
                has_nvlink_c2c = any(
                    arch in name.lower() for arch in ["grace", "hopper", "gh200"]
                )
            else:
                has_nvlink_c2c = False
        except ImportError:
            # Default to False if pynvml not available
            has_nvlink_c2c = False

    params = {
        "tree_method": "hist",  # Required for external memory and ExtMemQuantileDMatrix
        "grow_policy": "depthwise",  # CRITICAL: Allows building entire tree layers with few batch iterations
        "objective": objective,
        "max_bin": 256,  # Balance between accuracy and memory usage for histogram construction
    }

    # Handle categorical features (if preprocessed by Ray Data)
    if enable_categorical:
        params["enable_categorical"] = True
        # Use optimal parameters for categorical features
        params["max_cat_to_onehot"] = 4  # Threshold for one-hot vs partitioning

    if use_gpu:
        params.update(
            {
                "device": "cuda",
                "sampling_method": "gradient_based",  # More efficient for GPU and enables subsampling
                "subsample": 0.8,  # Reduce GPU memory usage, works well with gradient_based sampling
            }
        )

        # GPU-specific categorical handling
        if enable_categorical:
            params["max_cat_to_onehot"] = 8  # Higher threshold for GPU

        # Handle single page concatenation for PCIe systems
        if use_single_page_concatenation:
            params.update(
                {
                    "extmem_single_page": True,  # Concatenate batches for PCIe performance
                    "subsample": 0.2,  # Aggressive subsampling to fit in memory
                    "sampling_method": "gradient_based",  # Essential for low subsample rates
                }
            )
            # Lower max_bin for concatenated pages to save memory
            params["max_bin"] = min(params["max_bin"], 128)

        # NVLink-C2C optimizations
        if has_nvlink_c2c:
            # Can use higher bins and less aggressive subsampling on C2C systems
            params["max_bin"] = 512
            if not use_single_page_concatenation:
                params["subsample"] = 0.9  # Less aggressive subsampling
    else:
        # CPU-specific optimizations based on storage type
        if storage_type == "nvme":
            # NVMe can handle larger batches and higher bins
            params["max_bin"] = 512
        elif storage_type == "ssd":
            # Standard SSD - moderate settings
            params["max_bin"] = 256
        elif storage_type == "hdd":
            # HDD - conservative settings to reduce I/O
            params["max_bin"] = 128
            warnings.warn(
                "HDD storage detected for CPU external memory training. "
                "Performance will be severely limited by disk I/O. "
                "Consider using NVMe SSD for practical training speeds."
            )

    # Adjust parameters based on memory constraints
    if memory_constraint_gb:
        if memory_constraint_gb < 16:  # Low memory system
            params.update(
                {
                    "max_bin": 128,
                    "subsample": 0.7,
                    "max_depth": 4,
                }
            )
            if use_gpu and not use_single_page_concatenation:
                # Enable single page concatenation for very low memory GPU systems
                params.update(
                    {
                        "extmem_single_page": True,
                        "subsample": 0.15,  # Very aggressive subsampling
                        "sampling_method": "gradient_based",
                    }
                )
        elif memory_constraint_gb > 64:  # High memory system
            base_bins = 512 if not use_gpu or has_nvlink_c2c else 256
            params.update(
                {
                    "max_bin": base_bins,
                    "max_depth": 8,
                }
            )
            if use_gpu and not has_nvlink_c2c:
                # Even high memory PCIe systems benefit from moderate subsampling
                params["subsample"] = 0.9

    # Objective-specific optimizations
    if "binary:" in objective:
        params["eval_metric"] = ["logloss", "error"]
        # Set base_score for binary classification to avoid XGBoost error
        params["base_score"] = 0.5
    elif "multi:" in objective:
        params["eval_metric"] = ["mlogloss", "merror"]
    elif "reg:" in objective:
        params["eval_metric"] = ["rmse"]
    elif "rank:" in objective:
        params["eval_metric"] = ["ndcg"]
        # Ranking often benefits from more conservative settings
        if use_gpu:
            params["subsample"] = min(params.get("subsample", 0.8), 0.7)

    # Performance warnings and recommendations
    if use_gpu and not has_nvlink_c2c and not use_single_page_concatenation:
        warnings.warn(
            "GPU training on PCIe system without single page concatenation detected. "
            "Performance may be 5x slower than in-core training. "
            "Consider setting use_single_page_concatenation=True with appropriate subsampling."
        )

    if not use_gpu and storage_type not in ["nvme", "ssd"]:
        warnings.warn(
            f"CPU external memory training with {storage_type} storage may be impractically slow. "
            "XGBoost external memory is I/O bound - consider NVMe SSD for practical performance."
        )

    return params


def _validate_xgboost_params(
    params: Dict[str, Any], use_external_memory: bool = True
) -> Dict[str, Any]:
    """Validate and adjust XGBoost parameters for robustness.

    Args:
        params: Original XGBoost parameters
        use_external_memory: Whether external memory is being used

    Returns:
        Validated and adjusted parameters
    """
    validated_params = params.copy()

    # Ensure tree_method is compatible with external memory
    if use_external_memory:
        if "tree_method" not in validated_params:
            validated_params["tree_method"] = "hist"
        elif validated_params["tree_method"] not in ["hist", "gpu_hist"]:
            logger.warning(
                f"Tree method '{validated_params['tree_method']}' may not work well with external memory. "
                "Consider using 'hist' or 'gpu_hist'."
            )

        # Validate grow_policy for external memory performance
        if "grow_policy" not in validated_params:
            validated_params["grow_policy"] = "depthwise"
        elif validated_params["grow_policy"] != "depthwise":
            logger.warning(
                f"Grow policy '{validated_params['grow_policy']}' is not optimal for external memory. "
                "Using 'depthwise' allows building entire tree layers with minimal batch iterations, "
                "significantly improving performance over 'lossguide' which iterates per tree node."
            )

        # Validate extmem_single_page configuration
        if (
            "extmem_single_page" in validated_params
            and validated_params["extmem_single_page"]
        ):
            if (
                "subsample" not in validated_params
                or validated_params["subsample"] >= 0.5
            ):
                logger.warning(
                    "extmem_single_page=True requires aggressive subsampling (≤0.5) to fit in memory. "
                    "Consider setting subsample=0.2 and sampling_method='gradient_based'."
                )
            if (
                "sampling_method" not in validated_params
                or validated_params["sampling_method"] != "gradient_based"
            ):
                validated_params["sampling_method"] = "gradient_based"
                logger.info(
                    "Set sampling_method='gradient_based' for extmem_single_page compatibility."
                )

    # Validate device and GPU-related parameters
    if "device" in validated_params and "cuda" in str(validated_params["device"]):
        # GPU training validation
        if "sampling_method" not in validated_params:
            validated_params["sampling_method"] = "gradient_based"

        # Validate GPU memory parameters
        if (
            "extmem_single_page" in validated_params
            and validated_params["extmem_single_page"]
        ):
            if "subsample" not in validated_params:
                validated_params["subsample"] = 0.2
            elif validated_params["subsample"] > 0.5:
                logger.warning(
                    f"GPU single page concatenation with subsample={validated_params['subsample']} "
                    "may cause out-of-memory errors. Consider reducing to ≤0.2."
                )

    # Validate objective function
    valid_objectives = [
        "reg:squarederror",
        "reg:squaredlogerror",
        "reg:logistic",
        "reg:pseudohubererror",
        "binary:logistic",
        "binary:logitraw",
        "binary:hinge",
        "multi:softmax",
        "multi:softprob",
        "rank:pairwise",
        "rank:ndcg",
        "rank:map",
        "survival:cox",
        "survival:aft",
    ]

    if "objective" in validated_params:
        obj = validated_params["objective"]
        if not any(obj.startswith(prefix.split(":")[0]) for prefix in valid_objectives):
            logger.warning(
                f"Objective '{obj}' may not be a standard XGBoost objective."
            )

        # Validate base_score for binary classification
        if "binary:" in obj and "base_score" not in validated_params:
            validated_params["base_score"] = 0.5
            logger.info(
                "Set base_score=0.5 for binary classification to avoid XGBoost errors."
            )

    # Set default eval_metric if not provided
    if "eval_metric" not in validated_params and "objective" in validated_params:
        obj = validated_params["objective"]
        if "binary:" in obj:
            validated_params["eval_metric"] = ["logloss", "error"]
        elif "multi:" in obj:
            validated_params["eval_metric"] = ["mlogloss", "merror"]
        elif "reg:" in obj:
            validated_params["eval_metric"] = ["rmse"]
        elif "rank:" in obj:
            validated_params["eval_metric"] = ["ndcg"]

    # Validate max_bin for external memory
    if use_external_memory and "max_bin" in validated_params:
        max_bin = validated_params["max_bin"]
        if max_bin < 32:
            logger.warning(
                f"max_bin={max_bin} is very low and may hurt accuracy. Consider ≥128."
            )
        elif max_bin > 1024:
            logger.warning(
                f"max_bin={max_bin} is very high and may increase memory usage significantly."
            )

    return validated_params
