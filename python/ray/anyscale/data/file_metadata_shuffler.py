from typing import Any, Dict, List, Tuple

import numpy as np

from ray.data.datasource.file_metadata_shuffler import FileMetadataShuffler


class RandomFileMetadataShuffler(FileMetadataShuffler):
    """Random shuffle file metadata when the reader argument enables it.
    Otherwise returns file metadata in its original order.
    """

    def __init__(self, reader_args: Dict[str, Any]):
        super().__init__(reader_args)
        self._is_shuffle_enabled = reader_args.get("shuffle", False)
        if self._is_shuffle_enabled:
            seed = reader_args.get("shuffle_seed", None)
            self._generator = np.random.default_rng(seed)

    def shuffle_files(
        self,
        paths_and_sizes: List[Tuple[str, int]],
    ) -> List[Tuple[str, int]]:
        if self._is_shuffle_enabled:
            return [
                paths_and_sizes[i]
                for i in self._generator.permutation(len(paths_and_sizes))
            ]
        else:
            return paths_and_sizes
