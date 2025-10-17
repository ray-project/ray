"""
Configuration constants for DataFusion optimizer.

This module defines all tunable parameters for the DataFusion integration,
including adaptive sampling strategies, size thresholds, and performance tuning.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class DataFusionSamplingConfig:
    """
    Configuration for DataFusion's adaptive sampling strategy.

    DataFusion uses table statistics for cost-based optimization. Since materializing
    full Ray Datasets is expensive, we use intelligent sampling to gather statistics
    without reading all data.

    Attributes:
        min_sample_size: Minimum sample size for any dataset (ensures basic statistics).
        max_sample_size: Maximum sample size for any dataset (caps memory usage).
        default_sample_size: Default when no metadata is available.

        tiny_threshold: Row threshold for tiny datasets (< 1K rows: use all).
        small_threshold: Row threshold for small datasets (1K-10K: 50% sample).
        medium_threshold: Row threshold for medium datasets (10K-100K: 10% sample).
        large_threshold: Row threshold for large datasets (100K-1M: 1% sample).

        tiny_sample_fraction: Sampling fraction for tiny datasets (1.0 = 100%).
        small_sample_fraction: Sampling fraction for small datasets (0.5 = 50%).
        medium_sample_fraction: Sampling fraction for medium datasets (0.1 = 10%).
        large_sample_fraction: Sampling fraction for large datasets (0.01 = 1%).

        read_operation_multiplier: Multiplier for datasets from read operations.
        block_metadata_multiplier: Multiplier for datasets with block metadata.
    """

    # Sample size bounds
    min_sample_size: int = 100
    max_sample_size: int = 10_000
    default_sample_size: int = 1_000

    # Dataset size tier thresholds (in rows)
    tiny_threshold: int = 1_000  # < 1K rows
    small_threshold: int = 10_000  # 1K-10K rows
    medium_threshold: int = 100_000  # 10K-100K rows
    large_threshold: int = 1_000_000  # 100K-1M rows
    # > 1M rows: use fixed max sample

    # Sampling fractions for each tier
    tiny_sample_fraction: float = 1.0  # 100% - use all rows
    small_sample_fraction: float = 0.5  # 50%
    medium_sample_fraction: float = 0.1  # 10%
    large_sample_fraction: float = 0.01  # 1%

    # Multipliers for special cases
    read_operation_multiplier: int = 5  # Datasets from read_parquet, read_csv, etc.
    block_metadata_multiplier: int = 2  # Datasets with block metadata


# Global default configuration instance
DEFAULT_SAMPLING_CONFIG = DataFusionSamplingConfig()


def get_sampling_config() -> DataFusionSamplingConfig:
    """
    Get the current DataFusion sampling configuration.

    Returns:
        DataFusionSamplingConfig with current settings.

    Examples:
        >>> config = get_sampling_config()
        >>> print(f"Max sample size: {config.max_sample_size}")
        Max sample size: 10000
    """
    # Future: This could check DataContext for user overrides
    return DEFAULT_SAMPLING_CONFIG
