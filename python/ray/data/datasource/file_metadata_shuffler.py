from typing import Any, List, Union

import numpy as np

from typing import Literal


class FileMetadataShuffler:
    """Random shuffle file metadata when the `shuffle` parameter enables it.
    Otherwise returns file metadata in its original order.
    """

    def __init__(self, shuffle: Union[Literal["files"], None]):
        self._is_shuffle_enabled = False
        if shuffle == "files":
            self._is_shuffle_enabled = True
            self._generator = np.random.default_rng()

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
