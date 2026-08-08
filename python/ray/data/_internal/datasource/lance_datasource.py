from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union

import numpy as np

from ray._common.retry import call_with_retry
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    import pyarrow


class LanceDatasource(Datasource):
    """Lance datasource, for reading Lance dataset."""

    def __init__(
        self,
        uri: Union[str, List[str]],
        version: Optional[Union[int, str]] = None,
        columns: Optional[List[str]] = None,
        filter: Optional[str] = None,
        storage_options: Optional[Dict[str, str]] = None,
        scanner_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__()
        _check_import(self, module="lance", package="pylance")

        import lance

        self._projection_map = None
        if isinstance(uri, str):
            self.uris = [uri]
        else:
            self.uris = list(uri)
        if len(self.uris) == 0:
            raise ValueError("`uri` must not be empty.")
        self.scanner_options = scanner_options or {}
        if columns is not None:
            self.scanner_options["columns"] = columns
        if filter is not None:
            self.scanner_options["filter"] = filter
        self.storage_options = storage_options
        self.lance_datasets = [
            lance.dataset(uri=u, version=version, storage_options=storage_options)
            for u in self.uris
        ]
        # Use the dataset-level (not fragment-level) schema: Lance fills nulls
        # for columns missing from older fragments under schema evolution, so
        # only the dataset schema is a correct output contract.
        if len(self.lance_datasets) > 1:
            from ray.data._internal.util import unify_schemas_with_validation

            self._schema = unify_schemas_with_validation(
                [ds.schema for ds in self.lance_datasets]
            )
        else:
            self._schema = self.lance_datasets[0].schema

        data_context = DataContext.get_current()
        lance_config = data_context.lance_config
        match = []
        match.extend(lance_config.read_fragments_errors_to_retry)
        match.extend(data_context.retried_io_errors)
        self._retry_params = {
            "description": "read lance fragments",
            "match": match,
            "max_attempts": lance_config.read_fragments_max_attempts,
            "max_backoff_s": lance_config.read_fragments_retry_max_backoff_s,
        }

    def supports_predicate_pushdown(self) -> bool:
        return True

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        read_tasks = []

        # Lance scanner's filter attr accepts only a string (SQL).
        # See: https://github.com/lance-format/lance/blob/aac74b441cdb6df7d78700dbba33c521e6379ca5/python/python/lance/lance/__init__.pyi#L230
        filter_expr = (
            str(self._predicate_expr.to_pyarrow())
            if self._predicate_expr is not None
            else None
        )
        filter_from_arg = self.scanner_options.get("filter")
        if filter_from_arg is not None:
            filter_expr = (
                filter_from_arg
                if filter_expr is None
                else f"({filter_expr}) AND ({filter_from_arg})"
            )

        fragments_override = self.scanner_options.get("fragments")
        if fragments_override is not None and len(self.lance_datasets) > 1:
            raise ValueError(
                "scanner_options['fragments'] is not supported when reading "
                "multiple Lance datasets."
            )
        if fragments_override is not None:
            fragments_per_dataset = [fragments_override]
        else:
            fragments_per_dataset = [
                lance_ds.get_fragments() for lance_ds in self.lance_datasets
            ]

        total_fragments = sum(len(fragments) for fragments in fragments_per_dataset)
        if total_fragments == 0:
            return read_tasks

        fragment_entries = [
            (lance_ds, fragment)
            for lance_ds, fragments in zip(self.lance_datasets, fragments_per_dataset)
            for fragment in fragments
        ]

        def _make_read_fn(groups, opts, retry):
            return lambda: _read_fragments_with_retry(groups, opts, retry)

        # Cap at the fragment count, since we can't create more non-empty read tasks
        # than fragments. Guard against parallelism <= 0 to avoid ZeroDivisionError
        # in np.array_split.
        num_read_tasks = max(1, min(parallelism, total_fragments))
        for chunk_indices in np.array_split(range(total_fragments), num_read_tasks):
            if len(chunk_indices) <= 0:
                continue

            dataset_fragment_groups = []
            current_lance_ds = None
            current_fragment_ids = []
            chunk_fragments = []
            for index in chunk_indices:
                lance_ds, fragment = fragment_entries[index]
                if current_lance_ds is not None and lance_ds is not current_lance_ds:
                    dataset_fragment_groups.append(
                        (current_lance_ds, current_fragment_ids)
                    )
                    current_fragment_ids = []
                current_lance_ds = lance_ds
                current_fragment_ids.append(fragment.metadata.id)
                chunk_fragments.append(fragment)
            if current_lance_ds is not None:
                dataset_fragment_groups.append((current_lance_ds, current_fragment_ids))

            num_rows = sum(fragment.count_rows() for fragment in chunk_fragments)
            input_files = [
                data_file.path()
                for fragment in chunk_fragments
                for data_file in fragment.data_files()
            ]

            # TODO(chengsu): Take column projection into consideration for schema.
            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                input_files=input_files,
                exec_stats=None,
            )
            # Use a copy per task to avoid mutation races when tasks run in parallel.
            task_scanner_options = dict(self.scanner_options)
            if filter_expr is not None:
                task_scanner_options["filter"] = filter_expr
            retry_params = self._retry_params

            read_task = ReadTask(
                _make_read_fn(
                    dataset_fragment_groups, task_scanner_options, retry_params
                ),
                metadata,
                schema=self._schema,
                per_task_row_limit=per_task_row_limit,
            )
            read_tasks.append(read_task)
        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO(chengsu): Add memory size estimation to improve auto-tune of parallelism.
        return None


def _read_fragments_with_retry(
    dataset_fragment_groups,
    scanner_options,
    retry_params,
) -> Iterator["pyarrow.Table"]:
    return call_with_retry(
        lambda: _read_fragments(dataset_fragment_groups, scanner_options),
        **retry_params,
    )


def _read_fragments(
    dataset_fragment_groups: List[Tuple[Any, List[int]]],
    scanner_options,
) -> Iterator["pyarrow.Table"]:
    """Read Lance fragments in batches.

    NOTE: Use fragment ids, instead of fragments as parameter, because pickling
    LanceFragment is expensive.
    """
    import pyarrow

    for lance_ds, fragment_ids in dataset_fragment_groups:
        fragments = [lance_ds.get_fragment(id) for id in fragment_ids]
        task_scanner_options = dict(scanner_options)
        task_scanner_options["fragments"] = fragments
        scanner = lance_ds.scanner(**task_scanner_options)
        for batch in scanner.to_reader():
            table = pyarrow.Table.from_batches([batch])
            # When you unpickle untrusted data, attackers can execute arbitrary code. To
            # avoid exposing our users, raise unless the user has explicitly opted in.
            raise_on_pickle_object_columns(table)
            yield table
