import abc
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional

from ray.data.block import DataBatch

if TYPE_CHECKING:
    import pyarrow


# TODO(@bveeramani): Consolidate this with `FileBasedDatasource` so that there aren't
# two divergent code paths.
class FileReader(abc.ABC):
    """Base class for reading files.

    The `ReadFiles` operator uses implementations of this interface to read data from
    files.
    """

    @abc.abstractmethod
    def read_paths(
        self,
        paths: List[str],
        *,
        filter_expr: "pyarrow.dataset.Expression",
        columns: Optional[List[str]],
        columns_rename: Optional[Dict[str, str]],
        filesystem: "pyarrow.fs.FileSystem"
    ) -> Iterable[DataBatch]:
        """Read batches of data from the given file paths.

        Args:
            paths: A list of file paths to read.
            filter_expr: pyarrow.dataset.Expression for predicate pushdown.
            columns: The columns that will be read. If None, all columns will be read.
            filesystem: The filesystem to read from.

        Returns:
            An iterable of data batches. Batches can be any size.
        """
        ...

    @abc.abstractmethod
    def estimate_in_memory_size(self, path: str, file_size: int, *, filesystem) -> int:
        """Estimate the in-memory size of the data at the given path.

        This method is used by the `PartitionFiles` operator to ensure that each read
        task receives an appropriate amount of data.

        Args:
            path: The path to the file.
            file_size: The on-disk size of the file in bytes.
            filesystem: The filesystem to read from.

        Returns:
            The estimated in-memory size of the data in bytes.
        """
        ...

    def count_rows(self, paths: List[str], *, filesystem) -> int:
        """Count the number of rows in the files at the given paths.

        This method is used by the `PushdownCountFiles` rule to avoid reading the entire
        file when only the number of rows is needed.

        Overriding this method is optional. If you do override it, also override
        `supports_count_rows` to return `True`.

        Args:
            path: A list of file paths to count rows from.
            filesystem: The filesystem to read from.

        Returns:
            The number of rows in the files.
        """
        raise NotImplementedError

    def supports_count_rows(self) -> bool:
        """Return whether other objects can call `count_rows`."""
        return False

    def supports_predicate_pushdown(self) -> bool:
        """Whether expressions can be handled upon reading"""
        return False
