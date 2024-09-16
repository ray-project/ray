import abc
from typing import TYPE_CHECKING, Iterable, List

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
        self, paths: List[str], *, filesystem: "pyarrow.fs.FileSystem"
    ) -> Iterable[DataBatch]:
        """Read batches of data from the given file paths.

        Args:
            paths: A list of file paths to read.
            filesystem: The filesystem to read from.

        Returns:
            An iterable of data batches. Batches can be any size.
        """
        ...
