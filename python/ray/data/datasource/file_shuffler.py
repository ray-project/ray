from typing import Any, Dict, List, Tuple

class FileShuffler:
    """Abstract class for file shuffler.

    Shufflers live on the driver side of the Dataset only.
    """
    def __init__(self, reader_args: Dict[str, Any]):
        self._reader_args = reader_args

    def shuffle_files(
        self,
        paths: List[str],
        file_sizes: List[int],
    ) -> Tuple[List[str], List[int]]:
        """Shuffle files in the given paths and sizes.

        Args:
            paths: The file paths to shuffle.
            file_sizes: The size of file paths, corresponding to `paths`.

        Returns:
            The file paths and their size after shuffling.
        """
        raise NotImplementedError


class SequentialFileShuffler(FileShuffler):
    def shuffle_files(
        self,
        paths: List[str],
        file_sizes: List[int],
    ) -> Tuple[List[str], List[int]]:
        """Return files in the given paths and sizes sequentially."""
        return (paths, file_sizes)