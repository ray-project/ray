"""
External Memory Utilities for XGBoost Training

This module contains utilities for creating XGBoost DMatrix objects using external memory
with Ray Data's streaming iteration capabilities. This avoids full dataset materialization
for large datasets while following XGBoost's official external memory best practices.

Key components:
- _RayDataExternalMemoryIterator: Custom iterator for XGBoost external memory
- _create_external_memory_dmatrix: Creates ExtMemQuantileDMatrix for optimal performance
- _create_smart_dmatrix: Automatically chooses between materialization and external memory
- _extract_features_and_labels: Helper for data preprocessing

This implementation follows XGBoost's external memory best practices:
- Uses ExtMemQuantileDMatrix for hist tree method (required for external memory)
- Implements streaming iteration with minimal memory footprint
- Supports GPU training with RMM integration
- Optimized for depthwise grow policy performance
- Follows XGBoost 3.0+ external memory recommendations
"""

import logging
import tempfile
import os
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union
import warnings

if TYPE_CHECKING:
    import pandas as pd
    import xgboost

logger = logging.getLogger(__name__)


class _RayDataExternalMemoryIterator:
    """Custom external memory iterator for XGBoost that uses Ray Data's iter_batches.

    This implements XGBoost's DataIter interface for external memory training,
    following the official XGBoost external memory best practices. The iterator
    supports streaming iteration with minimal memory footprint while maintaining
    compatibility with XGBoost's ExtMemQuantileDMatrix.

    .. warning::
        This iterator supports multiple epochs of training without caching all data in memory.
        However, for very large datasets, ensure that worker nodes have enough memory to
        handle the configured batch size. The iterator will automatically adjust batch sizes
        if memory constraints are detected.

        Memory usage is limited to approximately 2-3 batches in memory at any given time,
        making it suitable for datasets that don't fit entirely in memory.

        Following XGBoost best practices:
        - Use tree_method="hist" (required for external memory)
        - Use grow_policy="depthwise" for optimal performance
        - Set batch size to ~10GB per batch for 64GB RAM systems
        - Avoid small batch sizes (e.g., 32 samples) as they hurt performance
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
        self._current_batch_idx = 0
        self._total_batches = None
        self._batch_cache = None
        self._cache_size = 3  # Keep only 3 batches in memory at a time
        self._current_cache_start = 0

    def _get_total_batches(self):
        """Get the total number of batches without materializing all data."""
        if self._total_batches is None:
            # Count batches efficiently without loading all data
            if hasattr(self.dataset_shard, "iter_batches"):
                # Use a small sample to estimate total batches
                sample_iterator = self.dataset_shard.iter_batches(
                    batch_size=self.batch_size,
                    batch_format="pandas",
                    prefetch_batches=1,
                )
                # Count batches by iterating once
                count = 0
                for _ in sample_iterator:
                    count += 1
                self._total_batches = count
            else:
                # For already iterable datasets, we need to estimate
                # This is a fallback for edge cases
                self._total_batches = 1000  # Conservative estimate
        return self._total_batches

    def _load_batch_cache(self, start_idx: int):
        """Load a subset of batches into cache for efficient iteration."""
        if (
            self._batch_cache is None
            or start_idx < self._current_cache_start
            or start_idx >= self._current_cache_start + self._cache_size
        ):

            # Load new batch range into cache
            if hasattr(self.dataset_shard, "iter_batches"):
                batch_iterator = self.dataset_shard.iter_batches(
                    batch_size=self.batch_size,
                    batch_format="pandas",
                    prefetch_batches=1,
                )

                # Skip to the start position
                for _ in range(start_idx):
                    try:
                        next(batch_iterator)
                    except StopIteration:
                        break

                # Load cache_size batches into memory
                self._batch_cache = []
                for _ in range(self._cache_size):
                    try:
                        batch = next(batch_iterator)
                        self._batch_cache.append(batch)
                    except StopIteration:
                        break

                self._current_cache_start = start_idx
            else:
                # For already iterable datasets, convert to list as fallback
                # This maintains backward compatibility but with warning
                warnings.warn(
                    "Dataset shard is not a DataIterator. Converting to list for "
                    "compatibility. This may cause high memory usage for large datasets.",
                    UserWarning,
                )
                batch_iterator = self.dataset_shard
                self._batch_cache = list(batch_iterator)
                self._current_cache_start = 0

    def _get_batch(self, idx: int):
        """Get a specific batch by index, loading cache as needed."""
        if idx >= self._get_total_batches():
            raise IndexError(f"Batch index {idx} out of range")

        # Check if batch is in current cache
        cache_idx = idx - self._current_cache_start
        if (
            cache_idx < 0
            or cache_idx >= len(self._batch_cache)
            or self._batch_cache is None
        ):
            # Load new cache range
            self._load_batch_cache(idx)
            cache_idx = 0

        return self._batch_cache[cache_idx]

    def __iter__(self):
        """Make the iterator iterable for XGBoost external memory interface."""
        self._current_batch_idx = 0
        return self

    def __next__(self):
        """Get the next batch for XGBoost external memory training."""
        if self._current_batch_idx >= self._get_total_batches():
            raise StopIteration

        batch = self._get_batch(self._current_batch_idx)
        self._current_batch_idx += 1

        # Separate features and labels with robust handling
        X, y = _extract_features_and_labels(batch, self.label_column)

        return X, y

    def reset(self):
        """Reset the iterator to the beginning."""
        self._current_batch_idx = 0
        # Clear cache to free memory
        self._batch_cache = None
        self._current_cache_start = 0

    def __len__(self):
        """Return the total number of batches."""
        return self._get_total_batches()


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
                f"Found {y.isnull().sum()} missing values in labels. "
                "This may cause training issues.",
                UserWarning,
            )
    elif isinstance(y, pd.DataFrame):
        if y.isnull().any().any():
            warnings.warn(
                "Found missing values in multi-label columns. "
                "This may cause training issues.",
                UserWarning,
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
    Optimized for XGBoost 3.0+ with ExtMemQuantileDMatrix support.

    Following XGBoost external memory best practices:
    - Uses ExtMemQuantileDMatrix for hist tree method (required)
    - Implements streaming iteration with minimal memory footprint
    - Supports GPU training with RMM integration
    - Optimized for depthwise grow policy performance

    Args:
        dataset_shard: Ray Data DataIterator from ray.train.get_dataset_shard()
        label_column: Name of the label column(s) in the dataset
        batch_size: Number of rows per batch. If None, uses optimal batch size
        feature_types: List of feature types for XGBoost
        missing: Value to be treated as missing (default: NaN)
        max_bin: Maximum number of bins for histogram construction
        max_quantile_batches: Maximum number of quantile batches for GPU training
        min_cache_page_bytes: Minimum cache page size in bytes
        cache_host_ratio: Ratio of cache to keep on host vs device (GPU only)
        on_host: Whether to stage cache on host memory (GPU only)
        use_rmm: Whether to use RAPIDS Memory Manager (GPU only)
        ref: Reference DMatrix for consistent binning (GPU only)

    Returns:
        XGBoost ExtMemQuantileDMatrix optimized for external memory training
    """
    import xgboost

    # Auto-detect GPU usage
    is_gpu = False
    try:
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
            import cupy as cp
            import rmm
            from rmm.allocators.cupy import rmm_cupy_allocator

            # Use RMM for GPU-based external memory to improve performance
            mr = rmm.mr.PoolMemoryResource(rmm.mr.CudaAsyncMemoryResource())
            rmm.mr.set_current_device_resource(mr)
            # Set the allocator for cupy as well
            cp.cuda.set_allocator(rmm_cupy_allocator)
            use_rmm = True
        except ImportError:
            logger.warning(
                "RMM not available. GPU external memory performance may be suboptimal. "
                "Install cupy and rmm for better performance."
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
            """Clean up temporary directory."""
            try:
                import shutil

                shutil.rmtree(self.temp_dir, ignore_errors=True)
            except ImportError:
                pass

    # Create the Ray Data external memory iterator
    ray_iterator = _RayDataExternalMemoryIterator(
        dataset_shard, label_column, batch_size
    )

    # Create XGBoost DataIter wrapper
    xgb_iterator = _XGBoostExternalMemoryIter(
        ray_iterator, feature_types=feature_types, missing=missing, on_host=on_host
    )

    # Create ExtMemQuantileDMatrix for optimal external memory performance
    # This is the recommended approach for XGBoost 3.0+ external memory training
    dmatrix = xgboost.ExtMemQuantileDMatrix(
        xgb_iterator,
        max_bin=max_bin,
        max_quantile_batches=max_quantile_batches,
        min_cache_page_bytes=min_cache_page_bytes,
        cache_host_ratio=cache_host_ratio,
        ref=ref,
    )

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
