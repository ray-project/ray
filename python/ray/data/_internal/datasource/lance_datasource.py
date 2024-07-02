import logging
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

import numpy as np

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)


class LanceDatasource(Datasource):
    """Lance datasource, for reading Lance dataset."""

    def __init__(
        self,
        uri: str,
        columns: Optional[List[str]] = None,
        filter: Optional[str] = None,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        _check_import(self, module="lance", package="pylance")

        import lance

        self.uri = uri
        self.columns = columns
        self.filter = filter
        self.storage_options = storage_options
        self.lance_ds = lance.dataset(uri=uri, storage_options=storage_options)

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        read_tasks = []
        for fragments in np.array_split(self.lance_ds.get_fragments(), parallelism):
            if len(fragments) <= 0:
                continue

            fragment_ids = [f.metadata.id for f in fragments]
            num_rows = sum(f.count_rows() for f in fragments)
            input_files = [
                data_file.path() for f in fragments for data_file in f.data_files()
            ]

            # TODO(chengsu): Take column projection into consideration for schema.
            metadata = BlockMetadata(
                num_rows=num_rows,
                schema=fragments[0].schema,
                input_files=input_files,
                size_bytes=None,
                exec_stats=None,
            )
            columns = self.columns
            row_filter = self.filter
            lance_ds = self.lance_ds

            read_task = ReadTask(
                lambda f=fragment_ids: _read_fragments(
                    f, lance_ds, columns, row_filter
                ),
                metadata,
            )
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO(chengsu): Add memory size estimation to improve auto-tune of parallelism.
        return None


def _read_fragments(
    fragment_ids, lance_ds, columns, row_filter
) -> Iterator["pyarrow.Table"]:
    """Read Lance fragments in batches.

    NOTE: Use fragment ids, instead of fragments as parameter, because pickling
    LanceFragment is expensive.
    """
    import pyarrow

    fragments = [lance_ds.get_fragment(id) for id in fragment_ids]
    scanner = lance_ds.scanner(columns, filter=row_filter, fragments=fragments)
    for batch in scanner.to_reader():
        yield pyarrow.Table.from_batches([batch])
