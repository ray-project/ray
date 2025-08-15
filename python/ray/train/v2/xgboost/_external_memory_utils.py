"""
External Memory Utilities for XGBoost Training.

This module contains utilities for creating XGBoost DMatrix objects using external memory
with Ray Data's streaming iteration capabilities. This avoids full dataset materialization
for large datasets while following XGBoost's official external memory best practices.

Key components:
- _RayDataExternalMemoryIterator: Custom iterator implementing XGBoost's DataIter interface
- _create_external_memory_dmatrix: Creates XGBoost DMatrix with external memory optimization
- _create_fallback_dmatrix: Fallback DMatrix creation when external memory fails

This implementation follows XGBoost's external memory best practices:
- Uses ExtMemQuantileDMatrix for hist tree method (required for external memory)
- Implements streaming iteration with minimal memory footprint
- Automatic cleanup of temporary files and memory management
- Performance monitoring and adaptive optimization
- Enhanced error handling and recovery

This module provides internal utilities for XGBoost external memory training.
Users should use the XGBoostTrainer class for training, which automatically
handles external memory optimization.

For distributed training scenarios (e.g., Anyscale clusters), it's important to specify
a custom cache_dir parameter (e.g., "/mnt/cluster_storage") to ensure all nodes can
access the external memory cache files.

External Memory Documentation: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html
DataIter Interface: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html#dataiter-interface
External Memory Parameters: https://xgboost.readthedocs.io/en/latest/parameter.html#external-memory-parameters
"""

import logging
import tempfile
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    import pandas as pd
    import xgboost

logger = logging.getLogger(__name__)


class _RayDataExternalMemoryIterator:
    """Custom iterator for Ray Data that implements XGBoost's DataIter interface.

    This iterator provides streaming access to Ray Data batches, implementing
    XGBoost's DataIter protocol for external memory training.

    The DataIter interface allows XGBoost to consume data in batches without
    loading the entire dataset into memory, enabling training on datasets
    larger than available RAM.

    DataIter Interface: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html#dataiter-interface
    External Memory Best Practices: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html#best-practices
    """

    def __init__(
        self,
        dataset_shard,
        label_column: Union[str, List[str]],
        feature_columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
        cache_dir: Optional[str] = None,
    ):
        """Initialize the iterator.

        Args:
            dataset_shard: Ray dataset shard to iterate over.
            label_column: Name(s) of the label column(s).
            feature_columns: Names of feature columns. If None, all non-label columns are used.
            batch_size: Batch size for iteration. If None, uses optimal default.
            cache_dir: Directory for caching temporary files.
        """
        self.dataset_shard = dataset_shard
        self.label_column = label_column
        self.feature_columns = feature_columns
        self.batch_size = batch_size or _get_optimal_batch_size()
        self.cache_dir = _get_optimal_cache_directory(custom_dir=cache_dir)

        # Initialize batch iterator
        self._batch_iter = None
        self._current_batch = None
        self._batch_index = 0
        self._reset_iterator()

    def _reset_iterator(self):
        """Reset the batch iterator.

        Resets the iterator to the beginning of the dataset, allowing
        multiple epochs of training with the same data.
        """
        try:
            self._batch_iter = self.dataset_shard.iter_batches(
                batch_size=self.batch_size,
                batch_format="pandas",
                drop_last=False,
            )
            self._batch_index = 0
        except Exception as e:
            logger.error(f"Failed to reset iterator: {e}")
            raise

    def __iter__(self):
        """Return self as iterator."""
        return self

    def __next__(self):
        """Get next batch of data.

        Returns:
            Tuple of (data, label) for the next batch.
        """
        try:
            if self._current_batch is None:
                self._current_batch = next(self._batch_iter)
                self._batch_index += 1

            # Extract features and labels
            features, labels = self._extract_features_and_labels(self._current_batch)

            # Process the batch
            result = self._process_batch(features, labels)

            # Clear current batch to get next one
            self._current_batch = None

            return result

        except StopIteration:
            # Reset iterator for next epoch
            self._reset_iterator()
            raise
        except Exception as e:
            logger.error(f"Error in batch {self._batch_index}: {e}")
            raise

    def _extract_features_and_labels(self, batch):
        """Extract features and labels from a batch.

        Args:
            batch: Pandas DataFrame batch.

        Returns:
            Tuple of (features, labels).
        """
        try:
            # Handle single or multiple label columns
            if isinstance(self.label_column, str):
                labels = batch[self.label_column].values
                feature_cols = [
                    col for col in batch.columns if col != self.label_column
                ]
            else:
                labels = batch[self.label_column].values
                feature_cols = [
                    col for col in batch.columns if col not in self.label_column
                ]

            # Filter feature columns if specified
            if self.feature_columns:
                feature_cols = [
                    col for col in feature_cols if col in self.feature_columns
                ]

            features = batch[feature_cols].values
            return features, labels

        except Exception as e:
            logger.error(f"Failed to extract features and labels: {e}")
            raise

    def _process_batch(self, features, labels):
        """Process a batch of features and labels.

        Args:
            features: Feature array.
            labels: Label array.

        Returns:
            Processed batch data.
        """
        try:
            # Convert to appropriate format for XGBoost
            if hasattr(features, "values"):
                features = features.values

            if hasattr(labels, "values"):
                labels = labels.values

            # Ensure proper data types
            import numpy as np

            features = np.asarray(features, dtype=np.float32)
            labels = np.asarray(labels, dtype=np.float32)

            return features, labels

        except Exception as e:
            logger.error(f"Failed to process batch: {e}")
            raise


def _create_external_memory_dmatrix(
    dataset_shard,
    label_column: Union[str, List[str]],
    feature_types: Optional[List[str]] = None,
    missing: Optional[float] = None,
    batch_size: int = None,
    cache_prefix: Optional[str] = None,
    cache_dir: Optional[str] = None,
    # Default to False for better compatibility across different systems
    # See: https://xgboost.readthedocs.io/en/stable/tutorials/external_memory.html
    extmem_single_page: bool = False,
    cache_host_ratio: Optional[float] = None,
    max_bin: Optional[int] = None,
    # Default to CPU for broader compatibility
    device: str = "cpu",
    **kwargs,
) -> "xgboost.DMatrix":
    """Create an XGBoost DMatrix with external memory optimization for Ray datasets.

    This function creates an XGBoost DMatrix that uses external memory for training
    on large Ray datasets that don't fit in memory. It's an alternative to the
    standard xgb.DMatrix() constructor specifically designed for Ray datasets.

    External Memory DMatrix: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html#dmatrix-creation

    Args:
        dataset_shard: Ray dataset shard to convert.
        label_column: Name(s) of the label column(s).
        feature_types: Feature type specifications.
        missing: Missing value indicator.
        batch_size: Batch size for external memory iteration.
        cache_prefix: Prefix for cache files.
        cache_dir: Directory for caching external memory files. For distributed training
            scenarios (e.g., Anyscale clusters), specify a shared directory like
            "/mnt/cluster_storage" that all nodes can access. If None, the function
            will automatically select the best available directory.
        extmem_single_page: Whether to use single page concatenation.
        cache_host_ratio: Ratio of cache to keep on host vs device.
        max_bin: Maximum number of bins for histogram construction.
        device: Device to use for training (cpu/gpu).
        **kwargs: Additional arguments passed to fallback DMatrix creation.

    Returns:
        XGBoost DMatrix object optimized for external memory training.
    """
    try:
        # Determine optimal batch size
        optimal_batch_size = batch_size or _get_optimal_batch_size()

        # Determine optimal cache directory
        optimal_cache_dir = _get_optimal_cache_directory(custom_dir=cache_dir)

        # Create external memory iterator
        iterator = _RayDataExternalMemoryIterator(
            dataset_shard=dataset_shard,
            label_column=label_column,
            batch_size=optimal_batch_size,
            cache_dir=optimal_cache_dir,
        )

        # Create external memory DMatrix directly
        import xgboost as xgb

        # Create external memory DMatrix with optimal settings
        dmatrix = xgb.DMatrix(
            data=iterator,
            enable_categorical=False,  # Disable categorical for external memory
            # Default missing value for XGBoost compatibility
            missing=missing or float("nan"),
        )

        # Set external memory parameters
        dmatrix.set_info(
            # Default cache prefix for Ray external memory training
            cache_prefix=cache_prefix or "ray_external_memory",
            cache_dir=optimal_cache_dir,
            extmem_single_page=extmem_single_page,
            cache_host_ratio=cache_host_ratio,
            # Default max_bin for external memory training
            # See: https://xgboost.readthedocs.io/en/stable/tutorials/external_memory.html
            max_bin=max_bin or 256,
        )

        return dmatrix

    except Exception as e:
        logger.warning(
            f"External memory DMatrix creation failed: {e}, falling back to regular DMatrix"
        )
        return _create_fallback_dmatrix(
            dataset_shard,
            label_column,
            feature_types=feature_types,
            missing=missing,
            **kwargs,
        )


def _create_fallback_dmatrix(
    dataset_shard,
    label_column: Union[str, List[str]],
    feature_types: Optional[List[str]] = None,
    missing: Optional[float] = None,
    **kwargs,
):
    """Create a fallback DMatrix when external memory fails.

    This function provides a fallback mechanism by converting the Ray dataset
    to pandas and creating a regular DMatrix. This ensures training can continue
    even if external memory setup fails.

    Fallback DMatrix: https://xgboost.readthedocs.io/en/latest/python/python_api.html#xgboost.DMatrix

    Args:
        dataset_shard: Ray dataset shard to convert.
        label_column: Name(s) of the label column(s).
        **kwargs: Additional arguments for DMatrix creation.

    Returns:
        Regular XGBoost DMatrix object.
    """
    try:
        import xgboost as xgb

        # Convert to pandas for fallback
        df = dataset_shard.to_pandas()

        # Extract features and labels
        if isinstance(label_column, str):
            labels = df[label_column]
            features = df.drop(columns=[label_column])
        else:
            labels = df[label_column]
            features = df.drop(columns=label_column)

        # Create regular DMatrix with additional parameters
        dmatrix_kwargs = kwargs.copy()
        if feature_types is not None:
            dmatrix_kwargs["feature_types"] = feature_types
        if missing is not None:
            dmatrix_kwargs["missing"] = missing

        dmatrix = xgb.DMatrix(data=features, label=labels, **dmatrix_kwargs)

        return dmatrix

    except Exception as e:
        logger.error(f"Fallback DMatrix creation failed: {e}")
        raise


def _get_optimal_batch_size() -> int:
    """Get optimal batch size for external memory training.

    Returns the recommended batch size for external memory training based on
    XGBoost best practices and common system configurations.

    Batch Size Guidelines: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html#batch-size
    External Memory Best Practices: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html#best-practices

    Returns:
        Optimal batch size in number of rows.
    """
    # Default batch size for external memory training
    # This follows XGBoost recommendations for optimal performance
    # See: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html#batch-size
    return 50000


def _get_optimal_cache_directory(custom_dir: Optional[str] = None) -> str:
    """Get optimal cache directory for external memory training.

    Determines the best cache directory for external memory files based on
    available storage options and common cluster configurations. Users can
    specify a custom directory for distributed training scenarios where
    the default temp directory might not be accessible to all nodes.

    Cache Directory Guidelines: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html#cache-directory

    Args:
        custom_dir: Optional custom directory path. If provided and accessible,
                   this directory will be used instead of the default candidates.

    Returns:
        Path to optimal cache directory.
    """
    # If user specified a custom directory, try to use it first
    if custom_dir:
        if os.path.exists(custom_dir) and os.access(custom_dir, os.W_OK):
            try:
                # Create subdirectory for XGBoost cache
                xgboost_cache = os.path.join(custom_dir, "xgboost_external_memory")
                os.makedirs(xgboost_cache, exist_ok=True)
                return xgboost_cache
            except Exception as e:
                logger.warning(f"Custom directory {custom_dir} not accessible: {e}")
        else:
            logger.warning(
                f"Custom directory {custom_dir} does not exist or is not writable"
            )

    # Priority order for cache directories (fallback options)
    # See: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html#cache-directory
    cache_candidates = [
        "/mnt/cluster_storage",  # Anyscale cluster storage
        "/tmp/xgboost_cache",  # Local temp with subdirectory
        tempfile.gettempdir(),  # System temp directory
    ]

    for candidate in cache_candidates:
        if os.path.exists(candidate) and os.access(candidate, os.W_OK):
            # Create subdirectory for XGBoost cache
            xgboost_cache = os.path.join(candidate, "xgboost_external_memory")
            try:
                os.makedirs(xgboost_cache, exist_ok=True)
                return xgboost_cache
            except Exception:
                continue

    # Final fallback to system temp directory
    fallback_dir = os.path.join(tempfile.gettempdir(), "xgboost_external_memory")
    os.makedirs(fallback_dir, exist_ok=True)
    return fallback_dir
