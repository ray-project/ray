import math
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Callable, Generator, Iterable, List, NewType, Optional

import pyarrow as pa

from ray.data._internal.arrow_block import ArrowRow
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, Reader, ReadTask, WriteResult

# A Python DB API2-compliant `Connection` object.
Connection = Any


def cursor_to_block(cursor) -> Block:
    rows = cursor.fetchall()
    columns = [column_description[0] for column_description in cursor.description]
    pylist = [{c: v for c, v in zip(columns, row)} for row in rows]
    return pa.Table.from_pylist(pylist)


class DBAPI2Datasource(Datasource[ArrowRow]):
    def __init__(self, connection_factory: Callable[[], Connection]):
        self.connection_factory = connection_factory

    def create_reader(self, sql: str) -> "Reader":
        return DBAPI2Reader(sql, self.connection_factory)


@contextmanager
def connect(
    connection_factory: Callable[[], Connection]
) -> Generator[Connection, None, None]:
    connection = connection_factory()
    try:
        yield connection
    except:
        # `rollback` is optional since not all databases provide transaction support.
        if hasattr(connection, "rollback"):
            connection.rollback()
    finally:
        connection.close()


class DBAPI2Reader(Reader):

    NUM_SAMPLE_ROWS = 100

    def __init__(self, sql: str, connection_factory: Callable[[], Connection]):
        self.sql = sql
        self.connection_factory = connection_factory

    @property
    @lru_cache
    def sample_block(self) -> Block:
        with connect(self.connection_factory) as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT * FROM ({self.sql}) as T LIMIT {self.NUM_SAMPLE_ROWS}"
            )
            return cursor_to_block(cursor)

    @property
    @lru_cache
    def num_rows(self) -> int:
        with connect(self.connection_factory) as connection:
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM ({self.sql}) as T")
            return cursor.fetchone()[0]

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if self.num_rows == 0:
            return None

        accessor = BlockAccessor.for_block(self.sample_block)
        size_bytes_per_row = accessor.size_bytes() / accessor.num_rows()
        return math.ceil(size_bytes_per_row * self.num_rows)

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        if self.num_rows == 0:
            return []

        elif self.num_rows < parallelism:
            parallelism = self.num_rows

        num_rows_per_block = self.num_rows // parallelism
        num_blocks_with_extra_row = self.num_rows % parallelism

        sample_block_accessor = BlockAccessor.for_block(self.sample_block)
        sample_block_size_bytes = sample_block_accessor.size_bytes()
        sample_block_schema = sample_block_accessor.schema()

        tasks = []
        offset = 0
        for i in range(parallelism):
            num_rows = num_rows_per_block
            if i < num_blocks_with_extra_row:
                num_rows += 1

            def create_read_fn(num_rows: int, offset: int):
                def read_fn() -> Iterable[Block]:
                    with connect(self.connection_factory) as connection:
                        cursor = connection.cursor()
                        cursor.execute(
                            f"SELECT * FROM ({self.sql}) as T LIMIT {num_rows} OFFSET "
                            f"{offset}"
                        )
                        block = cursor_to_block(cursor)
                        return [block]

                return read_fn

            read_fn = create_read_fn(num_rows, offset)
            metadata = BlockMetadata(
                num_rows, sample_block_size_bytes, sample_block_schema, None, None
            )
            tasks.append(ReadTask(read_fn, metadata))

            offset += num_rows

        return tasks
