import numpy as np

from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.in_memory_size_estimator import (
    InMemorySizeEstimator,
)
from ray.util.annotations import DeveloperAPI

# Default Parquet encoding ratio: in-memory is ~5x on-disk size.
# Parquet uses columnar compression and encoding, so Arrow in-memory
# representation is significantly larger than the on-disk format.
PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT = 5


@DeveloperAPI
class ParquetInMemorySizeEstimator(InMemorySizeEstimator):
    """Estimates in-memory sizes for Parquet files using a fixed encoding ratio.

    Parquet files are typically much smaller on disk than in memory due to
    columnar compression and encoding. This estimator applies a constant
    ratio (default 5x) to avoid the overhead of reading file metadata or
    sampling data, which can be slow for Parquet files and hurt startup time.
    """

    def __init__(self, encoding_ratio: float = PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT):
        self._encoding_ratio = encoding_ratio

    def estimate_in_memory_sizes(self, manifest: FileManifest) -> np.ndarray:
        return self._encoding_ratio * manifest.file_sizes
