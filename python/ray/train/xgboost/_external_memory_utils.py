"""
Shared utility functions for XGBoost external memory support.

This module provides utility functions for creating external memory DMatrix objects
that work with both V1 and V2 XGBoost trainers in Ray Train.

Key Features:
- External memory DMatrix creation for large datasets
- GPU memory optimization with RMM
- Automatic batch size selection
- Cache directory management
- Performance recommendations

Examples:
    Basic usage:
        >>> from ray.train.xgboost._external_memory_utils import (
        ...     create_external_memory_dmatrix
        ... )
        >>> dmatrix = create_external_memory_dmatrix(
        ...     dataset_shard=dataset,
        ...     label_column="target",
        ... )
"""

import logging
import os
import tempfile
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# Constants for default configuration
DEFAULT_CPU_BATCH_SIZE = 10000
DEFAULT_GPU_BATCH_SIZE = 5000
DEFAULT_MAX_BIN = 256
MIN_BATCH_SIZE = 100
MAX_BATCH_SIZE = 100000

# XGBoost version requirements
MIN_XGBOOST_VERSION = "2.0.0"


def create_external_memory_dmatrix(
    dataset_shard,
    label_column: Union[str, List[str]],
    feature_columns: Optional[List[str]] = None,
    batch_size: Optional[int] = None,
    cache_dir: Optional[str] = None,
    device: str = "cpu",
    max_bin: Optional[int] = None,
    enable_categorical: bool = False,
    missing: Optional[float] = None,
    **kwargs,
):
    """Create an XGBoost DMatrix with external memory optimization.

    This function creates an XGBoost DMatrix that uses external memory for
    training on large datasets that don't fit in memory. It follows XGBoost's
    official external memory API using QuantileDMatrix.

    Performance Tips:
    - Use larger batch sizes for better I/O efficiency
    - Store cache_dir on fast SSD storage
    - Use GPU (device="cuda") for faster histogram computation
    - Adjust max_bin based on feature cardinality

    Args:
        dataset_shard: Ray dataset shard to convert to DMatrix.
        label_column: Name(s) of the label column(s).
        feature_columns: Names of feature columns. If None, all non-label
            columns are used.
        batch_size: Batch size for iteration. If None, uses optimal default
            (10000 for CPU, 5000 for GPU). Valid range: 100-100000.
        cache_dir: Directory for caching external memory files. If None,
            uses temp directory. Should be on fast storage with sufficient space.
        device: Device to use ("cpu" or "cuda"). GPU requires CUDA-enabled
            XGBoost build.
        max_bin: Maximum number of bins for histogram construction. If None,
            uses XGBoost default (256). Higher values increase accuracy but
            slow down training.
        enable_categorical: Enable categorical feature support. Requires
            XGBoost >= 1.6.0.
        missing: Value to recognize as missing. If None, uses NaN.
        **kwargs: Additional arguments passed to QuantileDMatrix constructor.

    Returns:
        XGBoost QuantileDMatrix object optimized for external memory training.

    Raises:
        ImportError: If XGBoost is not properly installed or version is too old.
        ValueError: If parameters are invalid (e.g., batch_size out of range).
        RuntimeError: If DMatrix creation fails due to data issues.

    Examples:
        Basic CPU training:
            >>> train_ds_iter = ray.train.get_dataset_shard("train")
            >>> dtrain = create_external_memory_dmatrix(
            ...     train_ds_iter,
            ...     label_column="target",
            ... )

        GPU training with custom settings:
            >>> dtrain = create_external_memory_dmatrix(
            ...     train_ds_iter,
            ...     label_column="target",
            ...     batch_size=5000,
            ...     cache_dir="/mnt/nvme/xgboost_cache",
            ...     device="cuda",
            ...     max_bin=512,
            ... )

        Categorical features:
            >>> dtrain = create_external_memory_dmatrix(
            ...     train_ds_iter,
            ...     label_column="target",
            ...     enable_categorical=True,
            ... )

    Note:
        This function requires XGBoost >= 2.0.0 for optimal external memory
        support. Earlier versions may have limited functionality or bugs.
    """
    # Validate and import XGBoost
    try:
        import xgboost as xgb
        from packaging import version
    except ImportError as e:
        raise ImportError(
            "XGBoost >= 2.0.0 is required for external memory DMatrix creation. "
            f"Install with: pip install 'xgboost>={MIN_XGBOOST_VERSION}'"
        ) from e

    # Validate XGBoost version
    try:
        xgb_version = version.parse(xgb.__version__)
        min_version = version.parse(MIN_XGBOOST_VERSION)
        if xgb_version < min_version:
            logger.warning(
                f"XGBoost version {xgb.__version__} is older than "
                f"recommended {MIN_XGBOOST_VERSION}. "
                "External memory support may be limited or buggy. "
                "Please upgrade: pip install --upgrade xgboost"
            )
    except Exception as e:
        logger.warning(f"Could not verify XGBoost version: {e}")

    # Validate device parameter
    if device not in ("cpu", "cuda"):
        raise ValueError(
            f"Invalid device '{device}'. Must be 'cpu' or 'cuda'. "
            f"For GPU training, ensure CUDA-enabled XGBoost is installed."
        )

    # Set and validate batch size
    if batch_size is None:
        batch_size = DEFAULT_GPU_BATCH_SIZE if device == "cuda" else (
            DEFAULT_CPU_BATCH_SIZE
        )
        logger.debug(
            f"Auto-selected batch_size={batch_size} for device={device}"
        )
    else:
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError(
                f"batch_size must be a positive integer, got {batch_size}"
            )
        if batch_size < MIN_BATCH_SIZE:
            logger.warning(
                f"batch_size={batch_size} is very small (< {MIN_BATCH_SIZE}). "
                "This may cause poor I/O performance. Consider increasing it."
            )
        if batch_size > MAX_BATCH_SIZE:
            logger.warning(
                f"batch_size={batch_size} is very large (> {MAX_BATCH_SIZE}). "
                "This may cause high memory usage. Consider decreasing it."
            )

    # Set and validate cache directory
    if cache_dir is None:
        cache_dir = tempfile.mkdtemp(prefix="xgboost_external_memory_")
        logger.info(
            f"No cache_dir specified. Using temporary directory: {cache_dir}"
        )
        logger.info(
            "For production use, specify a persistent cache_dir on fast storage."
        )
    else:
        if not isinstance(cache_dir, str):
            raise TypeError(
                f"cache_dir must be a string path, got {type(cache_dir)}"
            )
        try:
            os.makedirs(cache_dir, exist_ok=True)
            # Check if directory is writable
            test_file = os.path.join(cache_dir, ".write_test")
            with open(test_file, "w") as f:
                f.write("test")
            os.remove(test_file)
            logger.debug(f"Using cache directory: {cache_dir}")
        except (OSError, PermissionError) as e:
            raise RuntimeError(
                f"Cannot write to cache_dir '{cache_dir}': {e}. "
                "Ensure the directory exists and is writable."
            ) from e

    # Validate max_bin parameter
    if max_bin is not None:
        if not isinstance(max_bin, int) or max_bin <= 0:
            raise ValueError(
                f"max_bin must be a positive integer, got {max_bin}"
            )
        if max_bin < 16:
            logger.warning(
                f"max_bin={max_bin} is very low. This may reduce model quality. "
                "Consider using at least 32."
            )
        if max_bin > 1024:
            logger.warning(
                f"max_bin={max_bin} is very high. This may slow down training. "
                "Consider using 256-512 for most cases."
            )
    else:
        max_bin = DEFAULT_MAX_BIN

    # Create a custom DataIter for Ray datasets
    class RayDatasetIterator(xgb.DataIter):
        """Iterator for Ray datasets that works with XGBoost external memory.

        This iterator implements the XGBoost DataIter interface to stream
        data from Ray datasets in batches, enabling training on datasets
        that don't fit in memory.

        Attributes:
            dataset_shard: Ray dataset shard to iterate over.
            label_column: Name(s) of the label column(s).
            feature_columns: Names of feature columns to use.
            batch_size: Number of samples per batch.
        """

        def __init__(
            self,
            dataset_shard,
            label_column,
            feature_columns,
            batch_size,
            missing_value,
        ):
            self.dataset_shard = dataset_shard
            self.label_column = label_column
            self.feature_columns = feature_columns
            self.batch_size = batch_size
            self.missing_value = missing_value
            self._iterator = None
            self._batch_index = 0
            self._total_batches = 0
            self._error_count = 0
            super().__init__(cache_prefix=cache_dir)

        def next(self, input_data):
            """Advance the iterator by one batch and return the data.

            Args:
                input_data: XGBoost input data callback function.

            Returns:
                1 if data was successfully loaded, 0 if iteration is complete.

            Raises:
                RuntimeError: If too many consecutive errors occur during iteration.
            """
            if self._iterator is None:
                # Initialize iterator on first call
                logger.debug(
                    f"Initializing batch iterator with batch_size={self.batch_size}"
                )
                try:
                    self._iterator = self.dataset_shard.iter_batches(
                        batch_size=self.batch_size,
                        batch_format="pandas",
                    )
                    self._batch_index = 0
                    self._error_count = 0
                except Exception as e:
                    logger.error(f"Failed to initialize batch iterator: {e}")
                    raise RuntimeError(
                        f"Cannot create batch iterator from dataset: {e}. "
                        "Ensure the dataset is properly configured."
                    ) from e

            try:
                batch_df = next(self._iterator)
                self._batch_index += 1

                # Validate batch data
                if batch_df.empty:
                    logger.warning(
                        f"Batch {self._batch_index} is empty. Skipping."
                    )
                    return self.next(input_data)  # Skip to next batch

                # Separate features and labels
                try:
                    if isinstance(self.label_column, str):
                        if self.label_column not in batch_df.columns:
                            raise KeyError(
                                f"Label column '{self.label_column}' not found "
                                f"in dataset. Available columns: {list(batch_df.columns)}"
                            )
                        labels = batch_df[self.label_column].values
                        features = batch_df.drop(columns=[self.label_column])
                    else:
                        # Multiple label columns
                        missing_labels = [
                            col
                            for col in self.label_column
                            if col not in batch_df.columns
                        ]
                        if missing_labels:
                            raise KeyError(
                                f"Label columns {missing_labels} not found "
                                f"in dataset. Available: {list(batch_df.columns)}"
                            )
                        labels = batch_df[self.label_column].values
                        features = batch_df.drop(columns=self.label_column)

                    # Handle feature columns selection
                    if self.feature_columns is not None:
                        missing_features = [
                            col
                            for col in self.feature_columns
                            if col not in features.columns
                        ]
                        if missing_features:
                            raise KeyError(
                                f"Feature columns {missing_features} not found. "
                                f"Available: {list(features.columns)}"
                            )
                        features = features[self.feature_columns]

                    # Validate data types
                    if not all(features.dtypes.apply(lambda x: x.kind in "biufc")):
                        logger.warning(
                            "Some feature columns have non-numeric types. "
                            "This may cause training errors. "
                            "Consider converting to numeric types."
                        )

                    # Log progress periodically
                    if self._batch_index % 100 == 0:
                        logger.info(
                            f"Processed {self._batch_index} batches "
                            f"({self._batch_index * self.batch_size} samples)"
                        )

                    # Return data to XGBoost
                    input_data(data=features.values, label=labels)
                    self._error_count = 0  # Reset error count on success
                    return 1

                except KeyError as e:
                    logger.error(f"Column error in batch {self._batch_index}: {e}")
                    raise RuntimeError(
                        f"Data schema error: {e}. "
                        "Ensure label_column and feature_columns are correct."
                    ) from e

            except StopIteration:
                # End of iteration
                logger.info(
                    f"Completed iteration over {self._batch_index} batches "
                    f"({self._batch_index * self.batch_size} total samples)"
                )
                return 0
            except Exception as e:
                self._error_count += 1
                logger.error(
                    f"Error in batch {self._batch_index}: {e} "
                    f"(error count: {self._error_count})"
                )
                if self._error_count > 5:
                    raise RuntimeError(
                        f"Too many consecutive errors ({self._error_count}). "
                        f"Last error: {e}. Check data format and quality."
                    ) from e
                # Try to continue with next batch
                return self.next(input_data)

        def reset(self):
            """Reset the iterator to the beginning."""
            logger.debug("Resetting batch iterator")
            self._iterator = None
            self._batch_index = 0
            self._error_count = 0

    # Create the iterator
    try:
        data_iter = RayDatasetIterator(
            dataset_shard=dataset_shard,
            label_column=label_column,
            feature_columns=feature_columns,
            batch_size=batch_size,
            missing_value=missing,
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create data iterator: {e}. "
            "Check dataset_shard and column specifications."
        ) from e

    # Create QuantileDMatrix with external memory
    # QuantileDMatrix is optimized for hist tree method
    logger.info(
        f"Creating QuantileDMatrix with: batch_size={batch_size}, "
        f"max_bin={max_bin}, device={device}, cache_dir={cache_dir}"
    )

    try:
        dmatrix_kwargs = {
            "max_bin": max_bin,
            **kwargs,
        }

        # Add categorical feature support if enabled
        if enable_categorical:
            logger.debug("Categorical features enabled")
            dmatrix_kwargs["enable_categorical"] = True

        # Add missing value if specified
        if missing is not None:
            logger.debug(f"Using missing value: {missing}")
            dmatrix_kwargs["missing"] = missing

        dmatrix = xgb.QuantileDMatrix(
            data_iter,
            **dmatrix_kwargs,
        )

        logger.info(
            f"Successfully created external memory QuantileDMatrix "
            f"(batch_size={batch_size}, max_bin={max_bin}, device={device})"
        )

        return dmatrix

    except Exception as e:
        logger.error(f"Failed to create QuantileDMatrix: {e}")
        raise RuntimeError(
            f"QuantileDMatrix creation failed: {e}. "
            "Common issues:\n"
            "  - Incompatible data types (ensure numeric features)\n"
            "  - Memory constraints (try reducing batch_size or max_bin)\n"
            "  - Corrupt or malformed data\n"
            "  - Missing dependencies (for GPU: ensure CUDA-enabled XGBoost)"
        ) from e


def setup_gpu_external_memory() -> bool:
    """Setup GPU external memory training with RMM optimization.

    This function configures RAPIDS Memory Manager (RMM) for optimal GPU external
    memory performance. It should be called before creating external memory DMatrix
    objects for GPU training.

    RMM provides:
    - Better GPU memory allocation performance
    - Memory pooling for reduced allocation overhead
    - Integration with CuPy for NumPy-like GPU arrays

    Returns:
        True if GPU setup was successful, False otherwise.

    Examples:
        Basic GPU setup:
            >>> if setup_gpu_external_memory():
            ...     print("GPU ready for training")

        Check before GPU training:
            >>> import ray.train
            >>> if setup_gpu_external_memory():
            ...     # Proceed with GPU external memory training
            ...     trainer = XGBoostTrainer(
            ...         use_external_memory=True,
            ...         external_memory_device="cuda",
            ...     )
            ... else:
            ...     # Fallback to CPU
            ...     trainer = XGBoostTrainer(
            ...         use_external_memory=True,
            ...         external_memory_device="cpu",
            ...     )

    Note:
        Requirements for GPU external memory:
        - CUDA-enabled XGBoost build
        - RAPIDS Memory Manager (RMM): pip install rmm-cu11
        - CuPy: pip install cupy-cuda11x

        For CPU training, this function is not required.
    """
    try:
        import xgboost as xgb

        # Check if GPU is available
        if not xgb.build_info()["USE_CUDA"]:
            logger.warning("XGBoost was not built with CUDA support")
            return False

        # Try to configure RMM for GPU memory management
        try:
            import rmm  # noqa: F401
            from rmm.allocators.cupy import rmm_cupy_allocator

            import cupy  # noqa: F401

            cupy.cuda.set_allocator(rmm_cupy_allocator)
            logger.info("Successfully configured RMM for GPU external memory training")
            return True
        except ImportError:
            logger.warning(
                "RMM and CuPy are required for optimal GPU external memory performance. "
                "Install with: pip install rmm-cu11 cupy-cuda11x"
            )
            return False

    except ImportError:
        logger.warning("XGBoost is not installed")
        return False
    except Exception as e:
        logger.warning(f"Failed to setup GPU external memory: {e}")
        return False


def get_external_memory_recommendations() -> Dict[str, Any]:
    """Get recommendations for external memory training configuration.

    Returns:
        Dictionary containing recommended configuration settings and best practices.

    Examples:
        .. code-block:: python

            recommendations = get_external_memory_recommendations()
            print("Recommended parameters:", recommendations["parameters"])
            print("Best practices:", recommendations["best_practices"])
    """
    return {
        "parameters": {
            "tree_method": "hist",
            "grow_policy": "depthwise",
            "max_bin": 256,
        },
        "best_practices": [
            "Use hist tree method (required for QuantileDMatrix)",
            "Use depthwise grow policy for better performance",
            "Set appropriate batch_size based on available memory",
            "Use shared storage for cache_dir in distributed training",
            "Monitor disk I/O and adjust batch size accordingly",
        ],
        "cache_directories": {
            "local": "/tmp/xgboost_cache",
            "shared": "/mnt/cluster_storage/xgboost_cache",
            "cloud": "s3://bucket/xgboost_cache",
        },
        "batch_size_recommendations": {
            "cpu": {"small": 5000, "medium": 10000, "large": 20000},
            "gpu": {"small": 2500, "medium": 5000, "large": 10000},
        },
        "documentation": (
            "https://xgboost.readthedocs.io/en/"
            "stable/tutorials/external_memory.html"
        ),
    }
