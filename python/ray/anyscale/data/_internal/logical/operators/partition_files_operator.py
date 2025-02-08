from ray.anyscale.data._internal.readers import FileReader
from ray.data._internal.logical.interfaces import LogicalOperator


class PartitionFiles(LogicalOperator):
    """Partition file paths for reading.

    This operator ensures that each read task reads an appropriate amount of data.

    Physical operators that implement this logical operator should receive input blocks
    that contain two columns named `PATH_COLUMN_NAME` and `FILE_SIZE_COLUMN_NAME`. The
    physical operator should output blocks with a single column named
    `PATH_COLUMN_NAME`.
    """

    def __init__(
        self,
        input_dependency: LogicalOperator,
        *,
        reader: FileReader,
        filesystem,
    ):
        super().__init__(name="PartitionFiles", input_dependencies=[input_dependency])

        self.reader = reader
        self.filesystem = filesystem
