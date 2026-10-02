import pyarrow as pa
import pyarrow.dataset as pds
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileReader
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class OrcFileReader(FileReader):
    """Read ORC files in batches using PyArrow Dataset fragments.

    Each fragment covers a whole file. PyArrow applies row filters to the
    scanned batches; this reader does not provide ORC-native stripe pruning.
    """

    @override
    def _make_format(self) -> pds.OrcFileFormat:
        return pds.OrcFileFormat()

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        super()._on_batch_read(table)
        raise_on_pickle_object_columns(table)
