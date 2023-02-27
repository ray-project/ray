from abc import ABC, abstractmethod
from collections import defaultdict
import textwrap
from functools import cached_property
from math import ceil
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar
import logging
import pandas
import pyarrow
from ray.data._internal.execution.interfaces import TaskContext
import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, BlockAccessor
from ray.data.datasource import Reader, Datasource, ReadTask
from ray.util.annotations import DeveloperAPI, PublicAPI

DatabaseConnection = TypeVar("DatabaseConnection")

logger = logging.getLogger(__name__)
_PRINT_TO_CONSOLE = False


def _transpose_list(pylist: List[List[Any]]) -> List[List[Any]]:
    return list(map(list, zip(*pylist)))


def pylist_to_pydict_of_lists(
    pylist: List[List[Any]], columns: List[str]
) -> Dict[str, List[Any]]:
    return {c: d for c, d in zip(columns, _transpose_list(pylist))}


def pylist_to_pylist_of_dicts(
    pylist: List[List[Any]], columns: List[str]
) -> List[Dict[str, Any]]:
    return [{c: v for c, v in zip(columns, row)} for row in pylist]


def block_to_pylist(block: Block) -> List[List[Any]]:
    return [[*pydict.values()] for pydict in block_to_pylist_of_dicts(block)]


def block_to_pylist_of_dicts(block: Block) -> List[Dict[str, Any]]:
    return BlockAccessor.for_block(block).to_arrow().to_pylist()


def block_to_pydict_of_lists(block: Block) -> List[Dict[str, Any]]:
    return BlockAccessor.for_block(block).to_arrow().to_pydict()


def pylist_to_pandas(pylist: List[List[Any]], columns: List[str]) -> Block:
    return pandas.DataFrame(columns=columns, data=pylist)


def pylist_to_pyarrow(pylist: List[List[Any]], columns: List[str]) -> Block:
    return pyarrow.Table.from_pylist(pylist_to_pylist_of_dicts(pylist, columns))


def _get_column_names(block: Block) -> List[str]:
    return BlockAccessor.for_block(block).schema().names


def __flatten(obj):
    if isinstance(obj, list):
        for o in obj:
            for s in _flatten(o):
                yield s
    else:
        yield obj


def _flatten(obj):
    return list(__flatten(obj))


def _desc_val(v: Any) -> str:
    value_type = type(v)
    if value_type in (int, bool, float):
        desc = f"{v}"
    elif value_type == str:
        desc = f"'{v}'"
    elif value_type == list:
        desc = "[" + ",".join([_desc_val(d) for d in v]) + "]"
    elif value_type == dict:
        desc = (
            "dict(" + ",".join([v1 + "=" + _desc_val(v2) for v1, v2 in v.items()]) + ")"
        )
    else:
        desc = f"type({value_type})"

    # if len(desc) > 100:
    #    desc = desc[0:50] + ' ... ' + desc[-50:]

    return desc


def _desc_method(method: str, *args, **kwargs) -> str:
    desc = ""
    if len(args) > 0:
        desc = "\n".join([_desc_val(v) for v in args])
        if len(kwargs) > 0:
            desc = desc + "\n"
    if len(kwargs) > 0:
        desc = desc + "\n".join([k + "=" + _desc_val(v) for k, v in kwargs.items()])
    desc = textwrap.indent(desc, "  ")
    return method + ":\n" + desc


def _log(level: int, message: str, *args, **kwargs):
    if _PRINT_TO_CONSOLE or logger.isEnabledFor(level):
        if len(args) > 0:
            message = _desc_method(message, *args, **kwargs)

        if level == logging.ERROR:
            logger.error(message, exc_info=True)
        else:
            logger.log(level, message)

        if _PRINT_TO_CONSOLE:
            print(message)


def _debug(*args, **kwargs):
    _log(logging.DEBUG, *args, **kwargs)


def _warn(*args, **kwargs):
    _log(logging.WARN, *args, **kwargs)


def _error(*args, **kwargs):
    _log(logging.ERROR, *args, **kwargs)


@DeveloperAPI
class DefaultArgs(defaultdict):
    def __missing__(self, key):
        return "{" + str(key) + "}"


@DeveloperAPI
class QueryTemplates(dict):
    """Dictionary like class that provides functions for filtering
    and templating queries"""

    def templatize(
        self,
        columns: Optional[List[str]] = None,
        partition: Optional[int] = None,
        mode: Optional[str] = None,
        column_template: str = "{column}",
        partition_template: str = "{partition}",
        param_template: str = "?",
        **kwargs,
    ) -> "QueryTemplates":
        replacements = {}
        if columns:
            replacements["column_list"] = ",".join(
                [column_template.format(column=c) for c in columns]
            )
            replacements["param_list"] = ",".join(
                [param_template.format(column=c) for c in columns]
            )

        if partition is not None:
            replacements["partition"] = partition_template.format(
                partition=str(partition)
            )

        return self.filter(mode).replace_all(**{**replacements, **kwargs})

    def filter(self, mode: Optional[str] = None) -> "QueryTemplates":
        if mode is not None:
            suffix = f"_{mode}"
            filtered = {
                k.replace(suffix, ""): v for k, v in self.items() if k.endswith(suffix)
            }
            return type(self)(**filtered)
        else:
            return type(self)(**self)

    def replace(self, query_key: str, **replacements) -> Optional[str]:
        query = self.get(query_key, None)
        if not query:
            return None

        replacements = DefaultArgs(**replacements)
        formatted = query.format_map(replacements)
        return textwrap.dedent(formatted.strip("\n"))

    def replace_all(self, **replacements) -> "QueryTemplates":
        templated = {k: self.replace(k, **replacements) for k, _ in self.items()}
        return type(self)(**templated)


@DeveloperAPI
class DatabaseConnector(ABC):
    """Abstract class that implements the  DB operations used by readers and writers
    when interacting with the database.

    Attributes:
        connect_properties: The connection properties of the database connection.
        connection : The underlying connection object used to interact with the
            database.
    Current subclasses:
        DBAPI2Connector
    """

    def __init__(
        self,
        to_value_fn: Callable[[Any], Any],
        to_block_fn: Callable[[Any], Block],
        from_block_fn: Callable[[Block], Any],
        **connect_properties,
    ):
        self.to_value_fn = to_value_fn
        self.to_block_fn = to_block_fn
        self.from_block_fn = from_block_fn
        self.connect_properties = connect_properties
        self.connection: Optional[DatabaseConnection] = None
        self._ref_count: int = 0

    @abstractmethod
    def _open(self) -> DatabaseConnection:
        ...

    @abstractmethod
    def _commit(self) -> None:
        ...

    @abstractmethod
    def _rollback(self) -> None:
        ...

    @abstractmethod
    def _close(self) -> None:
        ...

    @abstractmethod
    def _execute(
        self, query: str, *args, block: Optional[Block] = None, **kwargs
    ) -> Any:
        ...

    def open(self) -> None:
        if self._ref_count == 0:
            _debug("opening database connection")
            try:
                self.connection = self._open()
            except Exception as e:
                self.connection = None
                _error("error opening database connection")
                raise e
        self._ref_count += 1

    def commit(self) -> None:
        if not self.connection:
            raise ValueError("database connection not open")

        _debug("committing database transaction")
        try:
            self._commit()
        except Exception as e:
            _error("committing database transaction")
            raise e

    def rollback(self) -> None:
        if not self.connection:
            raise ValueError("database connection not open")

        _debug("rolling back database transaction")
        try:
            self._rollback()
        except Exception as e:
            _error("error rolling back database transaction")
            raise e

    def close(self):
        if not self.connection:
            raise ValueError("database connection not open")

        self.commit()

        self._ref_count -= 1
        if self._ref_count == 0:
            _debug("closing database connection")
            try:
                self._close()
            except Exception as e:
                _error("error closing database connection")
                raise e
            finally:
                self.connection = None

    def query(self, query: str, **kwargs) -> None:
        self.execute(query, **kwargs)

    def query_value(self, query: str, **kwargs) -> Any:
        return self.to_value_fn(self.execute(query, **kwargs))

    def query_int(self, query: str, **kwargs) -> Optional[int]:
        value = self.query_value(query, **kwargs)
        return int(value) if value is not None else None

    def query_str(self, query: str, **kwargs) -> Optional[str]:
        value = self.query_value(query, **kwargs)
        return str(value) if value is not None else None

    def query_block(self, query: str, **kwargs) -> Block:
        return self.to_block_fn(self.execute(query, **kwargs))

    def insert_block(self, query: str, block: Block, **kwargs) -> None:
        data = self.from_block_fn(block)
        self.execute(query, block=data, **kwargs)

    def execute(
        self,
        query: str,
        block: Optional[Block] = None,
        warn_on_error: bool = False,
        query_args: List[Any] = None,
        **kwargs,
    ) -> Any:
        query_args = query_args or []
        _debug("executing", query, *query_args, **kwargs)
        try:
            if "call_fn(" == query[:8]:
                return self.call_fn(query, *query_args, block=block, **kwargs)
            else:
                return self._execute(query, *query_args, block=block, **kwargs)
        except Exception as e:
            if warn_on_error:
                _warn("error executing ", query, *query_args, **kwargs)
            else:
                _error("error executing ", query, *query_args, **kwargs)
                raise e

    def call_fn(self, query: str, *args, **kwargs) -> Any:
        argv = query[8:-1].split(",")
        fn_name, fn_args = argv[0], argv[1:]
        return self.__getattribute__(fn_name)(*fn_args, *args, **kwargs)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
            self.close()
            return False
        else:
            self.close()

    def __getstate__(self):
        state = self.__dict__.copy()
        # remove unserializable objects from state
        # so passing engines to tasks doesn't cause serialization error
        if "_connection" in state:
            del state["_connection"]

        # ref counting is per process
        if "_ref_count" in state:
            state["_ref_count"] = 0

        return state


@DeveloperAPI
class DatabaseReadTask(ReadTask):
    def __init__(
        self,
        connector: DatabaseConnector,
        metadata: BlockMetadata,
        queries: QueryTemplates,
        query_kwargs: Dict[str, Any] = None,
    ):
        self._metadata = metadata
        self.connector = connector
        self.queries = queries
        self.query_kwargs = query_kwargs or {}

    def _read_fn(self) -> Iterable[Block]:
        query = self.queries.get("read")
        if query:
            with self.connector:
                return [self.connector.query_block(query, **self.query_kwargs)]
        else:
            return []


@DeveloperAPI
class _DatabaseReader(Reader):
    def __init__(
        self,
        mode: str,
        connector: DatabaseConnector,
        queries: QueryTemplates,
        query_kwargs: Dict[str, Any] = None,
    ):
        self.mode = mode
        self.connector = connector
        self.queries = queries
        self.query_kwargs = query_kwargs or {}

    @cached_property
    def num_rows(self) -> int:
        query = self.queries.get("num_rows")
        if not query:
            raise ValueError("number of rows query not specified.")

        with self.connector:
            num_rows = self.connector.query_int(query, **self.query_kwargs)
            if num_rows is None:
                raise ValueError("number of rows query returns no value.")
            else:
                return num_rows

    @cached_property
    def sample(self) -> Block:
        query = self.queries.get("sample")
        if not query:
            raise ValueError("sample query not specified.")

        with self.connector:
            return self.connector.query_block(query, **self.query_kwargs)

    def estimate_inmemory_data_size(self):
        if self.num_rows and self.sample is not None:
            accessor = BlockAccessor.for_block(self.sample)
            return ceil(accessor.size_bytes() / accessor.num_rows() * self.num_rows)
        else:
            return None

    def get_read_tasks(self, parallelism: int) -> List[DatabaseReadTask]:
        """Gets the read tasks for reading from the database table in parallel.
        Will generate a task that will read divide reading from the table based
        on the number of rows and parallelism.

        Args:
            parallelism: The parallelism will be the same as the number of tasks
                returned.
        Returns:
            A list of read tasks.
        """
        if self.num_rows == 0:
            return []

        elif self.num_rows < parallelism:
            parallelism = self.num_rows

        normal_block_size = self.num_rows // parallelism
        n_blocks_with_extra_row = self.num_rows % parallelism

        accessor = BlockAccessor.for_block(self.sample)
        average_row_size = accessor.size_bytes() // accessor.num_rows()

        schema = accessor.schema()

        tasks = []
        row_start = 0
        for i in range(0, parallelism):
            num_rows = normal_block_size
            if i < n_blocks_with_extra_row:
                num_rows += 1

            size_bytes = average_row_size * num_rows
            metadata = BlockMetadata(num_rows, size_bytes, schema, None, None)

            queries = self.queries.templatize(
                columns=metadata.schema.names,
                partition=i,
                row_start=row_start,
                num_rows=num_rows,
            )

            tasks.append(
                DatabaseReadTask(self.connector, metadata, queries, self.query_kwargs)
            )

            row_start += num_rows

        return tasks


@DeveloperAPI
class DatabaseBlockWriter:
    def __init__(self, queries: QueryTemplates, query_kwargs: Dict[str, Any] = None):
        self.queries = queries
        self.query_kwargs = query_kwargs or {}

    def write(self, connector: DatabaseConnector, block: Block):
        query = self.queries.get("write")
        if query:
            connector.insert_block(query, block, **self.query_kwargs)

    def prepare(self, connector: DatabaseConnector) -> None:
        query = self.queries.get("prepare")
        if query:
            connector.query(query, **self.query_kwargs)

    def complete(self, connector: DatabaseConnector) -> None:
        query = self.queries.get("complete")
        if query:
            connector.query(query, **self.query_kwargs)

    def all_complete(self, connector: DatabaseConnector) -> None:
        query = self.queries.get("all_complete")
        if query:
            connector.query(query, **self.query_kwargs)
            connector.commit()

    def failed(self, connector: DatabaseConnector, error: Exception) -> None:
        query = self.queries.get("failed")
        if query:
            connector.query(query, warn_on_error=True, **self.query_kwargs)

    def cleanup(self, connector: DatabaseConnector) -> None:
        query = self.queries.get("cleanup")
        if query:
            connector.query(query, warn_on_error=True, **self.query_kwargs)


@PublicAPI
class DatabaseDatasource(Datasource, ABC):
    """An abstract Ray datasource for reading and writing to databases.

    Attributes:
        connector: The connector to use for accessing the database.
        read_queries : A dictionary of read query templates.
        write_queries : A dictionary of write query templates.
        read_modes: The available read modes.
        write_modes: The available write modes.
        template_keys: The keys to use for templates.

    Current subclasses:
        DBAPI2Datasource
    """

    def __init__(
        self,
        connector: DatabaseConnector,
        read_modes: List[str],
        write_modes: List[str],
        read_queries: Optional[Dict[str, str]] = None,
        write_queries: Optional[Dict[str, str]] = None,
        template_keys: Optional[List[str]] = None,
    ):
        self.connector = connector

        read_queries = read_queries or {}
        self.read_queries = QueryTemplates(**read_queries)
        write_queries = write_queries or {}
        self.write_queries = QueryTemplates(**write_queries)
        self.template_keys = [
            "column_template",
            "partition_template",
            "param_template",
        ]
        if template_keys:
            self.template_keys = self.template_keys + template_keys

        self.read_modes = read_modes
        self.write_modes = write_modes

    def _get_template_kwargs(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in kwargs.items() if k in self.template_keys}

    def _get_query_kwargs(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in kwargs.items() if k not in self.template_keys}

    def _get_read_mode(self, mode: Optional[str]) -> str:
        if mode is None:
            return self.read_modes[0]
        else:
            if mode not in self.read_modes:
                raise ValueError(f"{mode} is an unsupported read mode")
            return mode

    def _get_write_mode(self, mode: Optional[str]) -> str:
        if mode is None:
            return self.write_modes[0]
        else:
            if mode not in self.write_modes:
                raise ValueError(f"{mode} is an unsupported write mode")
            return mode

    def _create_reader(self, mode: str, *args, **kwargs) -> _DatabaseReader:
        return _DatabaseReader(mode, *args, **kwargs)

    def create_reader(self, mode: Optional[str] = None, **kwargs) -> _DatabaseReader:
        template_kwargs = self._get_template_kwargs(**kwargs)
        query_kwargs = self._get_query_kwargs(**kwargs)
        mode = self._get_read_mode(mode)
        queries = self.read_queries.templatize(mode=mode, **template_kwargs)
        return self._create_reader(mode, self.connector, queries, query_kwargs)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        mode: Optional[str] = None,
        cleanup: bool = True,
        **write_args: Dict[str, Any],
    ) -> List[DatabaseBlockWriter]:
        template_kwargs = self._get_template_kwargs(**write_args)
        query_kwargs = self._get_query_kwargs(**write_args)
        mode = self._get_write_mode(mode)

        # open connection and start the transaction
        writers: List[DatabaseBlockWriter] = []
        with self.connector as connection:
            try:
                for n, block in enumerate(blocks):
                    # get columns if this is the first block,
                    if n == 0:
                        columns = _get_column_names(block)

                    # templatize all the queries needed
                    queries = self.write_queries.templatize(
                        mode=mode,
                        columns=columns,
                        partition=ctx.task_idx,
                        **template_kwargs,
                    )
                    if not cleanup and "cleanup" in queries:
                        del queries["cleanup"]

                    # create the db block write operation
                    writer = DatabaseBlockWriter(queries, query_kwargs)
                    writers.append(writer)

                    # prepare for writing once per task
                    if n == 0:
                        writer.prepare(connection)

                    # write each block to the database
                    writer.write(connection, block)

            # on failure, run failed queries and cleanup queries
            except Exception as e:
                for writer in writers:
                    writer.failed(connection, e)
                for writer in writers:
                    writer.cleanup(connection)
                raise e

        return writers

    def on_write_complete(
        self, writers_lists: List[List[List[DatabaseBlockWriter]]]
    ) -> None:
        writers: List[DatabaseBlockWriter] = _flatten(writers_lists)
        if len(writers) > 0:
            with self.connector as connection:
                try:
                    for writer in writers:
                        writer.complete(connection)

                    writers[0].all_complete(connection)
                except Exception as e:
                    for writer in writers:
                        writer.failed(connection, e)
                    raise e
                finally:
                    for writer in writers:
                        writer.cleanup(connection)

    def on_write_failed(self, refs: List[ObjectRef], error: Exception) -> None:
        writers: List[DatabaseBlockWriter] = _flatten(ray.get(refs))
        if len(writers) > 0:
            with self.connector as connection:
                for writer in writers:
                    writer.failed(connection, error)

                for writer in writers:
                    writer.cleanup(connection)
