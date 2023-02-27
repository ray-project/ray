from typing import Any, Dict, Iterable, List, Optional
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.data.block import Block
from ray.data.datasource.database_datasource import (
    pylist_to_pylist_of_dicts,
    DatabaseConnection,
    DatabaseBlockWriter,
    _warn,
)
from ray.data.datasource.dbapi2_datasource import DBAPI2Datasource, DBAPI2Connector
from ray.data._internal.execution.interfaces import TaskContext


def cursor_to_pyarrow(cursor: Any) -> Block:
    return cursor.fetchall_arrow()


@DeveloperAPI
class DatabricksConnector(DBAPI2Connector):
    def __init__(self, **connect_properties):
        from databricks.sql import connect as databricks_connect_fn

        super().__init__(
            databricks_connect_fn,
            to_block_fn=cursor_to_pyarrow,
            from_block_fn=pylist_to_pylist_of_dicts,
            **connect_properties
        )

    def remove_stage(self, stage_uri, **kwargs) -> Any:
        from pyarrow.fs import FileSystem

        fs, path = FileSystem.from_uri(stage_uri)
        try:
            fs.delete_dir(path)
        finally:
            pass

    def _rollback(self) -> None:
        _warn("databricks does not support rollback.")
        pass  # rollback not supported


@PublicAPI
class DatabricksDatasource(DBAPI2Datasource):
    # default write queries
    WRITE_QUERIES = dict(
        prepare_copyinto="CREATE TABLE IF NOT EXISTS {table}",
        all_complete_copyinto="""
                COPY INTO {table}
                FROM '{stage_uri}'
                WITH ( {with_clause} )
                FILEFORMAT = PARQUET
                PATTERN = '*.parquet'
                FORMAT_OPTIONS (
                    'mergeSchema' = 'true'
                )
                COPY_OPTIONS ('mergeSchema' = 'true')
            """,
        cleanup_copyinto="call_fn(remove_stage,{stage_uri})",
    )

    def __init__(
        self,
        connector: DatabaseConnection,
        read_queries: Dict[str, str] = None,
        write_queries: Dict[str, str] = None,
        template_keys: List[str] = None,
    ):
        read_queries = read_queries or {}
        write_queries = write_queries or {}
        template_keys = template_keys or []
        super().__init__(
            connector,
            read_modes=["partition", "direct"],
            write_modes=["copyinto", "direct", "stage"],
            read_queries=read_queries,
            write_queries={**DatabricksDatasource.WRITE_QUERIES, **write_queries},
            template_keys=["stage_uri", "with_clause"] + template_keys,
        )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        table: str,
        mode: str = "copyinto",
        stage_uri: Optional[str] = None,
        with_clause: str = "",
        **kwargs
    ) -> List[DatabaseBlockWriter]:
        if mode == "copyinto":
            if not stage_uri:
                raise ValueError("copyinto mode requires a stage_uri")
        return super().write(
            blocks,
            ctx,
            table=table,
            mode=mode,
            stage_uri=stage_uri,
            with_clause=with_clause,
            **kwargs
        )
