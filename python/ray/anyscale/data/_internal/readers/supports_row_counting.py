import abc
from typing import List, Optional

import pyarrow.fs

from .file_reader import FileReader


class SupportsRowCounting(abc.ABC):
    """A mix-in to implement row counting logic.

    The `PushdownCountFiles` rule uses this interface to optimize row counting.
    """

    @abc.abstractmethod
    def count_rows(
        self: FileReader, paths: List[str], *, filesystem: pyarrow.fs.FileSystem
    ) -> int:
        """Count the number of rows in the files at the given paths.

        This method is used by the `PushdownCountFiles` rule to avoid reading the entire
        file when only the number of rows is needed.

        Args:
            path: A list of file paths to count rows from.
            filesystem: The filesystem to read from.

        Returns:
            The number of rows in the files.
        """
        ...

    @abc.abstractmethod
    def can_count_rows(self: FileReader) -> bool:
        """Return whether `count_rows` can be called on this reader instance."""
        ...

    @abc.abstractmethod
    def count_rows_batch_size(self: FileReader) -> Optional[int]:
        """Return the number of paths to pass to `count_rows` at a time.

        Under-the-hood, the count pushdown rule uses the `MapBatches` logical operator.
        The semantics for the batch size are the same.
        """
        ...
