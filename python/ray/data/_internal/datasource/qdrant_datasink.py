import datetime
import math
import os
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Literal, Optional, Set

import numpy as np
import pyarrow as pa

from ray._common.retry import call_with_retry
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

if TYPE_CHECKING:
    from qdrant_client import QdrantClient

QDRANT_API_KEY_ENV_VAR = "QDRANT_API_KEY"

_DISTANCES = ("Cosine", "Euclid", "Dot", "Manhattan")

_RETRYABLE_ERRORS = [
    "ResponseHandlingException",
    r"Unexpected Response: (408|429|5\d\d)",
    r"StatusCode\.(UNAVAILABLE|DEADLINE_EXCEEDED|RESOURCE_EXHAUSTED|ABORTED|INTERNAL)",
]


class QdrantDatasink(Datasink[None]):
    def __init__(
        self,
        collection: Optional[str] = None,
        *,
        collection_column: Optional[str] = None,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        id_column: str = "id",
        vector_column: str = "vector",
        vector_name: Optional[str] = None,
        batch_size: int = 256,
        distance: Literal["Cosine", "Euclid", "Dot", "Manhattan"] = "Cosine",
        client_kwargs: Optional[Dict[str, Any]] = None,
    ):
        _check_import(self, module="qdrant_client", package="qdrant-client")

        if collection and collection_column:
            raise ValueError(
                "Specify exactly one of 'collection' or 'collection_column', "
                "not both."
            )
        if not collection and not collection_column:
            raise ValueError(
                "Either 'collection' or 'collection_column' must be provided."
            )
        if not url:
            raise ValueError("'url' must be provided.")
        if batch_size < 1:
            raise ValueError(f"batch_size must be positive, got {batch_size}.")
        if distance not in _DISTANCES:
            raise ValueError(
                f"distance must be one of {list(_DISTANCES)}, got {distance!r}."
            )
        if id_column == vector_column:
            raise ValueError(
                "id_column and vector_column refer to the same column "
                f"'{id_column}'. They must be distinct."
            )
        if collection_column in (id_column, vector_column):
            raise ValueError(
                f"collection_column '{collection_column}' must not be the same "
                f"as id_column ('{id_column}') or vector_column "
                f"('{vector_column}')."
            )

        self.collection = collection
        self.collection_column = collection_column
        self.url = url
        self.api_key = api_key or os.getenv(QDRANT_API_KEY_ENV_VAR)
        self.id_column = id_column
        self.vector_column = vector_column
        self.vector_name = vector_name
        self.batch_size = batch_size
        self.distance = distance
        self.client_kwargs = client_kwargs or {}
        self._ready_collections: Set[str] = set()

    def _get_client(self) -> "QdrantClient":
        import qdrant_client

        return qdrant_client.QdrantClient(
            url=self.url, api_key=self.api_key, **self.client_kwargs
        )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        client = self._get_client()
        try:
            for block in blocks:
                table = BlockAccessor.for_block(block).to_arrow()
                if table.num_rows == 0:
                    continue
                if self.collection_column:
                    self._write_multi_collection(client, table)
                else:
                    assert self.collection is not None
                    self._write_to_collection(client, table, self.collection)
        finally:
            client.close()

    def _write_multi_collection(self, client: "QdrantClient", table: pa.Table) -> None:
        name = self.collection_column
        if name not in table.column_names:
            raise ValueError(
                f"Collection column '{name}' not found in table. "
                f"Available columns: {table.column_names}"
            )
        column = table.column(name)
        if column.null_count > 0:
            raise ValueError(
                f"Collection column '{name}' contains null values; "
                "fill or drop them before writing with collection_column."
            )
        table = table.set_column(
            table.schema.get_field_index(name), name, column.cast(pa.string())
        )

        sort_key = SortKey(key=name, descending=False)
        block_accessor = BlockAccessor.for_block(
            BlockAccessor.for_block(table).sort(sort_key)
        )
        for (collection,), group in block_accessor._iter_groups_sorted(sort_key):
            self._write_to_collection(client, group.drop(name), collection)

    def _write_to_collection(
        self, client: "QdrantClient", table: pa.Table, collection: str
    ) -> None:
        from qdrant_client import models

        for column, kind in ((self.id_column, "ID"), (self.vector_column, "Vector")):
            if column not in table.column_names:
                raise ValueError(
                    f"{kind} column '{column}' not found in table. "
                    f"Available columns: {table.column_names}"
                )
        table = table.filter(table.column(self.id_column).is_valid())
        if table.column(self.vector_column).null_count > 0:
            raise ValueError(
                f"Vector column '{self.vector_column}' contains null values; "
                "fill or drop them before writing to Qdrant."
            )

        for offset in range(0, table.num_rows, self.batch_size):
            batch = table.slice(offset, self.batch_size)
            ids = self._to_point_ids(batch.column(self.id_column))
            vectors = self._to_vectors(batch)
            payloads = self._to_payloads(batch)
            self._ensure_collection(client, collection, len(vectors[0]))

            points = models.Batch(
                ids=ids,
                vectors={self.vector_name: vectors} if self.vector_name else vectors,
                payloads=payloads,
            )
            call_with_retry(
                lambda: client.upsert(
                    collection_name=collection, points=points, wait=True
                ),
                description=f"upsert batch to collection '{collection}'",
                match=_RETRYABLE_ERRORS,
                max_attempts=5,
            )

    def _to_point_ids(self, column: pa.ChunkedArray) -> List:
        """Qdrant only allows unsigned integers or UUIDs as point IDs.
        Ref: https://qdrant.tech/documentation/manage-data/points/#point-ids
        """
        if pa.types.is_integer(column.type):
            ids = column.to_pylist()
            if min(ids) < 0:
                raise ValueError(
                    f"ID column '{self.id_column}' contains negative values; "
                    "Qdrant point IDs must be unsigned integers or UUIDs."
                )
            return ids

        ids = []
        for value in column.to_pylist():
            if isinstance(value, bytes) and len(value) == 16:
                value = uuid.UUID(bytes=value)
            try:
                ids.append(str(uuid.UUID(str(value))))
            except ValueError:
                raise ValueError(
                    f"ID column '{self.id_column}' contains {value!r}; Qdrant "
                    "point IDs must be unsigned integers or UUIDs."
                ) from None
        return ids

    def _to_vectors(self, table: pa.Table) -> List[List[float]]:
        invalid = ValueError(
            f"Vector column '{self.vector_column}' must contain non-empty, "
            "one-dimensional numeric vectors of the same length."
        )
        column = BlockAccessor.for_block(table).to_numpy(self.vector_column)
        try:
            vectors = np.stack(list(column))
        except ValueError:
            raise invalid from None
        if (
            vectors.ndim != 2
            or vectors.shape[1] == 0
            or not np.issubdtype(vectors.dtype, np.number)
        ):
            raise invalid
        if not np.isfinite(vectors.astype(np.float32)).all():
            raise ValueError(
                f"Vector column '{self.vector_column}' contains NaN or infinite "
                "values, which Qdrant can't store."
            )
        return vectors.astype(np.float64).tolist()

    def _to_payloads(self, table: pa.Table) -> Optional[List[Dict[str, Any]]]:
        names = [
            name
            for name in table.column_names
            if name not in (self.id_column, self.vector_column)
        ]
        if not names:
            return None
        columns = [
            [self._to_payload_value(v, name) for v in table.column(name).to_pylist()]
            for name in names
        ]
        return [dict(zip(names, row)) for row in zip(*columns)]

    @staticmethod
    def _to_payload_value(value: Any, column: str) -> Any:
        """Convert a value to JSON."""
        if value is None or isinstance(value, (bool, int, str)):
            return value
        if isinstance(value, float):
            if math.isinf(value):
                raise ValueError(
                    f"Payload column '{column}' contains infinite values, which "
                    "Qdrant can't store."
                )
            return None if math.isnan(value) else value
        if isinstance(value, (datetime.date, datetime.time)):
            return value.isoformat()
        if isinstance(value, np.ndarray):
            return QdrantDatasink._to_payload_value(value.tolist(), column)
        if isinstance(value, dict):
            return {
                k: QdrantDatasink._to_payload_value(v, column) for k, v in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [QdrantDatasink._to_payload_value(v, column) for v in value]
        raise ValueError(
            f"Payload column '{column}' contains a value of type "
            f"{type(value).__name__}, which Qdrant can't store. Cast the column to "
            "a string or number type before writing."
        )

    def _ensure_collection(
        self, client: "QdrantClient", collection: str, vector_size: int
    ) -> None:
        if collection in self._ready_collections:
            return

        from qdrant_client import models

        params = models.VectorParams(
            size=vector_size, distance=models.Distance(self.distance)
        )

        def create_if_missing() -> None:
            if client.collection_exists(collection):
                return
            try:
                client.create_collection(
                    collection_name=collection,
                    vectors_config={self.vector_name: params}
                    if self.vector_name
                    else params,
                )
            except Exception:
                # It is possible that another write task created the
                # collection at the same time.
                if not client.collection_exists(collection):
                    raise

        call_with_retry(
            create_if_missing,
            description=f"create collection '{collection}'",
            match=_RETRYABLE_ERRORS,
            max_attempts=5,
        )
        self._ready_collections.add(collection)
