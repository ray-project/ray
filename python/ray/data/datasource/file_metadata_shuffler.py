from typing import Any, Dict, List, Tuple


class FileMetadataShuffler:
    """Abstract class for file metadata shuffler.

    Shufflers live on the driver side of the Dataset only.
    """

    def __init__(self, reader_args: Dict[str, Any]):
        self._reader_args = reader_args

    def shuffle_files(
        self,
        paths_and_sizes: List[Tuple[str, int]],
    ) -> List[Tuple[str, int]]:
        """Shuffle files in the given paths and sizes.

        Args:
            paths_and_sizes: The file paths and file sizes to shuffle.

        Returns:
            The file paths and their sizes after shuffling.
        """
        raise NotImplementedError


class SequentialFileMetadataShuffler(FileMetadataShuffler):
    def shuffle_files(
        self,
        paths_and_sizes: List[Tuple[str, int]],
    ) -> List[Tuple[str, int]]:
        """Return files in the given paths and sizes sequentially."""
        return paths_and_sizes
