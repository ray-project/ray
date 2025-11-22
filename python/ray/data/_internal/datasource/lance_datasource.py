import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import numpy as np

from ray._common.retry import call_with_retry
from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)


class LanceDatasource(Datasource):
    """Lance datasource, for reading Lance dataset."""

    # Errors to retry when reading Lance fragments.
    READ_FRAGMENTS_ERRORS_TO_RETRY = ["LanceError(IO)"]
    # Maximum number of attempts to read Lance fragments.
    READ_FRAGMENTS_MAX_ATTEMPTS = 10
    # Maximum backoff seconds between attempts to read Lance fragments.
    READ_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS = 32

    def __init__(
        self,
        uri: str,
        version: Optional[Union[int, str]] = None,
        columns: Optional[List[str]] = None,
        filter: Optional[str] = None,
        storage_options: Optional[Dict[str, str]] = None,
        scanner_options: Optional[Dict[str, Any]] = None,
    ):
        _check_import(self, module="lance", package="pylance")

        import lance

        self.uri = uri
        self.scanner_options = scanner_options or {}
        if columns is not None:
            self.scanner_options["columns"] = columns
        if filter is not None:
            self.scanner_options["filter"] = filter
        self.storage_options = storage_options
        self.lance_ds = lance.dataset(
            uri=uri, version=version, storage_options=storage_options
        )

        match = []
        match.extend(self.READ_FRAGMENTS_ERRORS_TO_RETRY)
        match.extend(DataContext.get_current().retried_io_errors)
        self._retry_params = {
            "description": "read lance fragments",
            "match": match,
            "max_attempts": self.READ_FRAGMENTS_MAX_ATTEMPTS,
            "max_backoff_s": self.READ_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS,
        }

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        read_tasks = []
        ds_fragments = self.scanner_options.get("fragments")
        if ds_fragments is None:
            ds_fragments = self.lance_ds.get_fragments()

        for fragments in np.array_split(ds_fragments, parallelism):
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
                size_bytes=None,
                input_files=input_files,
                exec_stats=None,
            )
            scanner_options = self.scanner_options
            lance_ds = self.lance_ds
            retry_params = self._retry_params

            read_task = ReadTask(
                lambda f=fragment_ids: _read_fragments_with_retry(
                    f,
                    lance_ds,
                    scanner_options,
                    retry_params,
                ),
                metadata,
                schema=fragments[0].schema,
                per_task_row_limit=per_task_row_limit,
            )
            read_tasks.append(read_task)
        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO(chengsu): Add memory size estimation to improve auto-tune of parallelism.
        return None


def _read_fragments_with_retry(
    fragment_ids,
    lance_ds,
    scanner_options,
    retry_params,
) -> Iterator["pyarrow.Table"]:
    return call_with_retry(
        lambda: _read_fragments(fragment_ids, lance_ds, scanner_options),
        **retry_params,
    )


def _read_fragments(
    fragment_ids,
    lance_ds,
    scanner_options,
) -> Iterator["pyarrow.Table"]:
    """Read Lance fragments in batches.

    NOTE: Use fragment ids, instead of fragments as parameter, because pickling
    LanceFragment is expensive.
    """
    import pyarrow

    fragments = [lance_ds.get_fragment(id) for id in fragment_ids]
    scanner_options["fragments"] = fragments
    scanner = lance_ds.scanner(**scanner_options)
    for batch in scanner.to_reader():
        yield pyarrow.Table.from_batches([batch])
