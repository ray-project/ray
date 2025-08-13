"""
External Memory Utilities for XGBoost Training

This module contains utilities for creating XGBoost DMatrix objects using external memory
with Ray Data's streaming iteration capabilities. This avoids full dataset materialization
for large datasets.

Key components:
- _RayDataExternalMemoryIterator: Custom iterator for XGBoost external memory
- _create_external_memory_dmatrix: Creates ExtMemQuantileDMatrix for optimal performance
- _create_smart_dmatrix: Automatically chooses between materialization and external memory
- _extract_features_and_labels: Helper for data preprocessing
"""

import logging
import tempfile
import os
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union
import warnings

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class _RayDataExternalMemoryIterator:
    """Custom external memory iterator for XGBoost that uses Ray Data's iter_batches.

    This avoids full dataset materialization while maintaining distributed data sharding
    and preprocessing capabilities. Based on XGBoost's DataIter interface for external memory.

    .. warning::
        To support multiple epochs of training, this iterator caches all data batches
        in memory on the first pass. For very large datasets, this can lead to high
        memory usage and potential out-of-memory errors. Ensure that worker nodes
        have enough RAM to hold all batches of the dataset shard, or reduce
        batch size accordingly.
    """

    def __init__(
        self, dataset_shard, label_column: Union[str, List[str]], batch_size: int = None
    ):
        """Initialize the external memory iterator.

        Args:
            dataset_shard: Ray Data DataIterator from ray.train.get_dataset_shard()
                          or an already created batch iterator
            label_column: Name of the label column(s) in the dataset
            batch_size: Number of rows per batch. If None, uses optimal batch size
                       based on available memory (recommended: ~10GB per batch for 64GB RAM)
        """
        self.dataset_shard = dataset_shard
        self.label_column = label_column
        self.is_multi_label = isinstance(label_column, list)

        # Calculate optimal batch size if not provided
        if batch_size is None:
            from ray.train.v2.xgboost._system_utils import (
                _estimate_dataset_memory_usage,
            )

            memory_estimates = _estimate_dataset_memory_usage(dataset_shard)
            batch_size = memory_estimates["recommended_batch_size"]

        self.batch_size = batch_size
        self._batches = None
        self._current_batch_idx = 0

    def _initialize_batches(self):
        """Lazily initialize the batch iterator to avoid early materialization."""
        if self._batches is None:
            # Check if dataset_shard is already an iterator or needs to be converted
            if hasattr(self.dataset_shard, "iter_batches"):
                # dataset_shard is a DataIterator, use iter_batches
                batch_iterator = self.dataset_shard.iter_batches(
                    batch_size=self.batch_size,
                    batch_format="pandas",  # Pandas format for XGBoost compatibility
                    prefetch_batches=1,  # Minimal prefetching to reduce memory usage
                )
            else:
                # dataset_shard might already be an iterable
                batch_iterator = self.dataset_shard

            # Convert to list for multiple iterations (required by XGBoost external memory)
            self._batches = list(batch_iterator)

    def __iter__(self):
        """Make the iterator iterable for XGBoost external memory interface."""
        self._initialize_batches()
        self._current_batch_idx = 0
        return self

    def __next__(self):
        """Get the next batch for XGBoost external memory training."""
        if self._current_batch_idx >= len(self._batches):
            raise StopIteration

        batch = self._batches[self._current_batch_idx]
        self._current_batch_idx += 1

        # Separate features and labels with robust handling
        X, y = _extract_features_and_labels(batch, self.label_column)

        return X, y

    def reset(self):
        """Reset the iterator to the beginning."""
        self._current_batch_idx = 0


def _extract_features_and_labels(
    batch: "pd.DataFrame", label_column: Union[str, List[str]]
):
    """Extract features and labels from a preprocessed batch.

    Note: This function assumes the data has already been preprocessed by Ray Data,
    including categorical encoding, missing value handling, and data type conversions.
    """
    import pandas as pd

    if isinstance(label_column, str):
        # Single label column
        if label_column not in batch.columns:
            raise ValueError(
                f"Label column '{label_column}' not found in batch columns: {batch.columns.tolist()}"
            )

        X = batch.drop(columns=[label_column])
        y = batch[label_column]
    else:
        # Multiple label columns (for multi-output tasks)
        missing_labels = [col for col in label_column if col not in batch.columns]
        if missing_labels:
            raise ValueError(
                f"Label columns {missing_labels} not found in batch columns: {batch.columns.tolist()}"
            )

        X = batch.drop(columns=label_column)
        y = batch[label_column]

    # Validate labels for critical issues only
    if isinstance(y, pd.Series):
        if y.isnull().any():
            warnings.warn(
                "Found missing values in labels. Consider preprocessing labels before training."
            )
    elif isinstance(y, pd.DataFrame):
        if y.isnull().any().any():
            warnings.warn(
                "Found missing values in multi-label targets. Consider preprocessing labels before training."
            )

    return X, y


def _create_external_memory_dmatrix(
    dataset_shard,
    label_column: Union[str, List[str]],
    batch_size: int = None,
    feature_types: Optional[List[str]] = None,
    missing: Optional[float] = None,
    max_bin: int = 256,
    max_quantile_batches: Optional[int] = None,
    min_cache_page_bytes: Optional[int] = None,
    cache_host_ratio: Optional[float] = None,
    on_host: bool = True,
    use_rmm: bool = None,
    ref: Optional["xgboost.ExtMemQuantileDMatrix"] = None,
):
    """Create XGBoost DMatrix using external memory with Ray Data iterator.

    This function creates a memory-efficient DMatrix that doesn't require
    full dataset materialization, making it suitable for large datasets.
    Optimized for XGBoost 2.0+ with ExtMemQuantileDMatrix support.
    """
    import xgboost

    # Auto-detect GPU usage
    is_gpu = False
    try:
        import cupy

        # Check if we're in a GPU context or have GPU data
        if hasattr(dataset_shard, "to_pandas"):
            # Try a small sample to detect GPU arrays
            sample = next(
                iter(dataset_shard.iter_batches(batch_size=1, batch_format="pandas"))
            )
            if any(
                hasattr(col, "device") and "cuda" in str(col.device)
                for col in sample.values
            ):
                is_gpu = True
    except (ImportError, StopIteration):
        pass

    # Configure RMM for GPU training
    if is_gpu and use_rmm is not False:
        try:
            import rmm
            import cupy as cp
            from rmm.allocators.cupy import rmm_cupy_allocator

            # Set up RMM if not already configured
            current_mr = rmm.mr.get_current_device_resource()
            if not isinstance(
                current_mr, (rmm.mr.PoolMemoryResource, rmm.mr.ArenaMemoryResource)
            ):
                if use_rmm is None:
                    # Auto-configure RMM with pool memory resource
                    mr = rmm.mr.PoolMemoryResource(rmm.mr.CudaAsyncMemoryResource())
                    rmm.mr.set_current_device_resource(mr)
                    cp.cuda.set_allocator(rmm_cupy_allocator)
                    use_rmm = True
                    logger.info(
                        "Configured RMM with PoolMemoryResource for optimal GPU external memory performance"
                    )
                elif use_rmm:
                    # User explicitly requested RMM
                    mr = rmm.mr.PoolMemoryResource(rmm.mr.CudaAsyncMemoryResource())
                    rmm.mr.set_current_device_resource(mr)
                    cp.cuda.set_allocator(rmm_cupy_allocator)
                    logger.info(
                        "Configured RMM as requested for GPU external memory training"
                    )
            else:
                use_rmm = True  # Already configured

        except ImportError:
            if use_rmm:
                warnings.warn(
                    "RMM requested but not available. Install cupy and rmm for optimal GPU external memory performance. "
                    "Performance will be significantly degraded without RMM."
                )
            use_rmm = False

    # Create a custom XGBoost DataIter for external memory
    class _XGBoostExternalMemoryIter(xgboost.DataIter):
        def __init__(
            self, ray_data_iterator, feature_types=None, missing=None, on_host=True
        ):
            self.ray_iterator = ray_data_iterator
            self.iterator = None
            self.feature_types = feature_types
            self.missing = missing
            self.on_host = on_host
            # Use temporary directory for XGBoost cache files
            self.temp_dir = tempfile.mkdtemp(prefix="xgb_external_")
            super().__init__(
                cache_prefix=os.path.join(self.temp_dir, "cache"), on_host=on_host
            )

        def next(self, input_data: Callable) -> bool:
            """XGBoost calls this method to get the next batch of data."""
            if self.iterator is None:
                self.iterator = iter(self.ray_iterator)

            try:
                X, y = next(self.iterator)

                # Convert to appropriate arrays for XGBoost
                if is_gpu:
                    # Ensure data is on GPU for ExtMemQuantileDMatrix
                    try:
                        import cupy as cp

                        if hasattr(X, "values"):
                            X_array = cp.asarray(X.values)
                        else:
                            X_array = cp.asarray(X)

                        if hasattr(y, "values"):
                            y_array = cp.asarray(y.values)
                        else:
                            y_array = cp.asarray(y)
                    except ImportError:
                        # Fallback to numpy if cupy not available
                        if hasattr(X, "values"):
                            X_array = X.values
                        else:
                            X_array = X

                        if hasattr(y, "values"):
                            y_array = y.values
                        else:
                            y_array = y
                else:
                    # CPU training
                    if hasattr(X, "values"):
                        X_array = X.values
                    else:
                        X_array = X

                    if hasattr(y, "values"):
                        y_array = y.values
                    else:
                        y_array = y

                # Pass data to XGBoost using the input_data callback
                input_data(
                    data=X_array,
                    label=y_array,
                    feature_types=self.feature_types,
                    missing=self.missing,
                )
                return True
            except StopIteration:
                return False

        def reset(self) -> None:
            """Reset the iterator to the beginning."""
            self.ray_iterator.reset()
            self.iterator = None

        def __del__(self):
            """Clean up temporary directory.

            Note: __del__ is not guaranteed to run; this is a best-effort cleanup. Any
            exceptions during cleanup are logged as warnings.
            """
            try:
                import shutil

                if hasattr(self, "temp_dir") and os.path.exists(self.temp_dir):
                    shutil.rmtree(self.temp_dir)
            except Exception as e:
                logger.warning(
                    "Failed to clean up temporary directory %s: %s",
                    getattr(self, "temp_dir", "<unknown>"),
                    e,
                )

    # Create Ray Data iterator
    ray_iterator = _RayDataExternalMemoryIterator(
        dataset_shard, label_column, batch_size
    )

    # Create XGBoost external memory iterator
    xgb_iterator = _XGBoostExternalMemoryIter(
        ray_iterator, feature_types, missing, on_host
    )

    # Build ExtMemQuantileDMatrix parameters
    dmatrix_kwargs = {"max_bin": max_bin}

    if max_quantile_batches is not None:
        dmatrix_kwargs["max_quantile_batches"] = max_quantile_batches

    if ref is not None:
        dmatrix_kwargs["ref"] = ref

    # GPU-specific parameters
    if is_gpu:
        if min_cache_page_bytes is not None:
            dmatrix_kwargs["min_cache_page_bytes"] = min_cache_page_bytes
        if cache_host_ratio is not None:
            dmatrix_kwargs["cache_host_ratio"] = cache_host_ratio

    # Use ExtMemQuantileDMatrix for optimal external memory performance
    try:
        if use_rmm and is_gpu:
            # Use RMM context for GPU training
            with xgboost.config_context(use_rmm=True):
                dmatrix = xgboost.ExtMemQuantileDMatrix(xgb_iterator, **dmatrix_kwargs)
        else:
            dmatrix = xgboost.ExtMemQuantileDMatrix(xgb_iterator, **dmatrix_kwargs)

    except (AttributeError, ImportError) as e:
        # Fallback to regular DMatrix with external memory if ExtMemQuantileDMatrix not available
        fallback_warning = (
            "ExtMemQuantileDMatrix not available, falling back to regular external memory DMatrix. "
            "Performance will be significantly slower. Consider upgrading XGBoost to version 2.0+."
        )
        if "ExtMemQuantileDMatrix" not in str(e):
            fallback_warning += f" Error: {e}"
        warnings.warn(fallback_warning)

        try:
            if use_rmm and is_gpu:
                with xgboost.config_context(use_rmm=True):
                    dmatrix = xgboost.DMatrix(xgb_iterator)
            else:
                dmatrix = xgboost.DMatrix(xgb_iterator)
        except Exception as fallback_error:
            raise RuntimeError(
                f"Failed to create both ExtMemQuantileDMatrix and fallback DMatrix. "
                f"ExtMemQuantileDMatrix error: {e}. Fallback error: {fallback_error}"
            )
    except Exception as e:
        # Handle other potential errors
        if "out of memory" in str(e).lower() or "insufficient memory" in str(e).lower():
            raise RuntimeError(
                f"Out of memory during DMatrix construction. Consider: "
                f"1. Reducing batch_size, 2. Increasing max_quantile_batches, "
                f"3. For GPU: adjusting cache_host_ratio or min_cache_page_bytes. "
                f"Original error: {e}"
            )
        else:
            raise RuntimeError(f"Failed to create ExtMemQuantileDMatrix: {e}")

    return dmatrix


def _create_smart_dmatrix(
    dataset_shard,
    label_column: Union[str, List[str]],
    force_external_memory: bool = False,
    feature_types: Optional[List[str]] = None,
    missing: Optional[float] = None,
    memory_limit_gb: Optional[float] = None,
):
    """Smart DMatrix creation that chooses between materialization and external memory.

    Automatically determines whether to use materialization or external memory based on:
    1. Dataset size relative to available memory per worker node
    2. User-specified memory limit (if provided)
    3. Force external memory flag
    """
    import xgboost
    import pandas as pd
    import numpy as np
    import ray

    # Calculate memory threshold for external memory decision
    if memory_limit_gb is None:
        from ray.train.v2.xgboost._system_utils import _get_node_memory_limit_gb

        memory_limit_gb = _get_node_memory_limit_gb()

    # Check dataset size to decide on strategy
    stats = dataset_shard.stats()
    estimated_size_gb = 0

    if stats and stats.total_bytes:
        estimated_size_gb = stats.total_bytes / (1024**3)

    # Use external memory for large datasets or when forced
    # Reserve 20% of memory for other operations, use 80% as threshold
    memory_threshold_gb = memory_limit_gb * 0.8

    if force_external_memory or estimated_size_gb > memory_threshold_gb:
        return _create_external_memory_dmatrix(
            dataset_shard, label_column, feature_types=feature_types, missing=missing
        )
    else:
        # For small datasets, materialization is more efficient
        # Check if we already have a DataIterator vs other formats
        if hasattr(dataset_shard, "materialize"):
            # DataIterator case
            dataset = dataset_shard.materialize()
            df = dataset.to_pandas()
        elif hasattr(dataset_shard, "to_pandas"):
            # Already materialized dataset case
            df = dataset_shard.to_pandas()
        else:
            # Assume it's already a pandas DataFrame or similar
            df = dataset_shard

        # Extract features and labels with robust handling
        X, y = _extract_features_and_labels(df, label_column)

        # Convert to numpy arrays
        if hasattr(X, "values"):
            X_array = X.values
        else:
            X_array = X

        if hasattr(y, "values"):
            y_array = y.values
        else:
            y_array = y

        return xgboost.DMatrix(
            X_array, label=y_array, feature_types=feature_types, missing=missing
        )
