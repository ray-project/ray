from typing import Any, List

import numpy as np

from ray.data.context import DataContext


class FileMetadataShuffler:
    """Random shuffle file metadata when the `DataContext` enables it.
    Otherwise returns file metadata in its original order.
    """

    def __init__(self):
        ctx = DataContext.get_current()
        shuffle_input = ctx.execution_options.shuffle_input
        is_shuffle_enabled = False
        shuffle_seed = None
        if isinstance(shuffle_input, bool):
            is_shuffle_enabled = shuffle_input
        else:
            assert isinstance(shuffle_input, tuple) and len(shuffle_input) == 2
            is_shuffle_enabled, shuffle_seed = shuffle_input

        self._is_shuffle_enabled = is_shuffle_enabled
        if self._is_shuffle_enabled:
            self._generator = np.random.default_rng(shuffle_seed)

    def shuffle_files(
        self,
        files_metadata: List[Any],
    ) -> List[Any]:
        if self._is_shuffle_enabled:
            return [
                files_metadata[i]
                for i in self._generator.permutation(len(files_metadata))
            ]
        else:
            return files_metadata
