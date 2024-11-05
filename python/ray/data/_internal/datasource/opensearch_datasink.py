from functools import cached_property
from itertools import chain
from typing import Any, Iterable, Iterator, Literal, Optional, Union

from opensearchpy import OpenSearch, helpers
from pandas import DataFrame
from pyarrow import Table
from typing_extensions import TypeAlias

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block
from ray.data.datasource.datasink import Datasink

OpType: TypeAlias = Literal["index", "create", "update", "delete"]


class OpensearchDatasink(Datasink):
    _index: str
    _op_type: Optional[OpType]
    _chunk_size: int
    _max_chunk_bytes: int
    _max_retries: int
    _initial_backoff: Union[float, int]
    _max_backoff: Union[float, int]
    _client_kwargs: Optional[dict[str, Any]]

    def __init__(
        self,
        index: str,
        op_type: Optional[OpType] = None,
        chunk_size: int = 500,
        max_chunk_bytes: int = 100 * 1024 * 1024,
        max_retries: int = 0,
        initial_backoff: Union[float, int] = 2,
        max_backoff: Union[float, int] = 600,
        client_kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__()
        self._index = index
        self._op_type = op_type
        self._chunk_size = chunk_size
        self._max_chunk_bytes = max_chunk_bytes
        self._max_retries = max_retries
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._client_kwargs = client_kwargs

    @staticmethod
    def _iter_block_rows(block: Block) -> Iterator[dict]:
        if isinstance(block, Table):
            yield from block.to_pylist()
        elif isinstance(block, DataFrame):
            for _, row in block.iterrows():
                yield row.to_dict()
        else:
            raise RuntimeError(f"Unknown block type: {type(block)}")

    @cached_property
    def _opensearch(self) -> OpenSearch:
        return OpenSearch(**self._client_kwargs)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        rows: Iterable[dict] = chain.from_iterable(
            self._iter_block_rows(block) for block in blocks
        )
        print(f"rows: {rows}")
        rows = (
            {
                "_index": self._index,
                **row,
            }
            for row in rows
        )
        if self._op_type is not None:
            rows = (
                {
                    "_op_type": self._op_type,
                    **row,
                }
                for row in rows
            )
        results = helpers.streaming_bulk(
            client=self._opensearch,
            actions=rows,
            chunk_size=self._chunk_size,
            max_chunk_bytes=self._max_chunk_bytes,
            raise_on_error=True,
            raise_on_exception=True,
            max_retries=self._max_retries,
            initial_backoff=self._initial_backoff,
            max_backoff=self._max_backoff,
        )
        for _ in results:
            pass

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def num_rows_per_write(self) -> Optional[int]:
        return None
