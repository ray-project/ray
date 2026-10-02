from abc import ABC, abstractmethod

import numpy as np

from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class InMemorySizeEstimator(ABC):
    """Sizes listing rows for ``RoundRobinPartitioner``.

    ``SamplingInMemorySizeEstimator`` reads one file per listing task and
    scales by its ratio. Implement this instead when decoded size is a known
    function of file size, so no sample read is needed.
    """

    @abstractmethod
    def estimate_in_memory_sizes(self, manifest: FileManifest) -> np.ndarray:
        """Estimate the in-memory sizes of the paths in the given manifest.

        Some `FilePartitioner` implementations use this method to ensure that each
        read task receives an appropriate amount of data. To ensure that file listing
        is efficient, this method must be cheap to call, on average.

        Args:
            manifest: A manifest containing the paths and on-disk sizes of the files.

        Returns:
            The estimated in-memory sizes of the data in bytes.
        """
        ...
