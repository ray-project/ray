from functools import cached_property
from typing import Any, Iterator, Mapping, Optional, Union

from opensearchpy import OpenSearch
from pyarrow import Schema, Table

from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask


class OpenSearchDatasource(Datasource):
    _index: str
    _query: Optional[Mapping[str, Any]]
    _keep_alive: str
    _chunk_size: int
    _client_kwargs: Optional[dict[str, Any]]
    _schema: Optional[Union[type, Schema]]

    def __init__(
        self,
        index: str,
        query: Optional[Mapping[str, Any]] = None,
        keep_alive: str = "5m",
        chunk_size: int = 1000,
        client_kwargs: Optional[dict[str, Any]] = None,
        schema: Optional[Union[type, Schema]] = None,
    ) -> None:
        super().__init__()
        self._index = index
        self._query = query
        self._keep_alive = keep_alive
        self._chunk_size = chunk_size
        self._client_kwargs = client_kwargs
        self._schema = schema if schema is not None else self.get_index_mapping(index)

    @property
    def _opensearch(self) -> OpenSearch:
        return OpenSearch(**self._client_kwargs)

    def schema(self) -> Optional[Union[type, Schema]]:
        return self._schema

    @staticmethod
    def build_query(query):
        body = {}
        if query is not None and "query" not in list(query.keys()):
            body = {"query": query}
        elif query is not None and "query" in list(query.keys()):
            body = query
        return body

    @cached_property
    def _num_rows(self) -> int:
        body = OpenSearchDatasource.build_query(self._query)
        print(f"++++++ body is {body}")
        return self._opensearch.count(
            index=self._index,
            body=body,
        )["count"]

    def num_rows(self) -> int:
        return self._num_rows

    """
    index_body = {
            "settings": {
                "index": {
                    "knn": True,
                    "number_of_shards": 2,
                    "number_of_replicas": 1
                }
            },
            "mappings": {
                "properties": {
                    "dataset_name": {
                        "type": "text"
                    },
                    "data_item_uri": {
                        "type": "text"
                    },
                    "vector": {
                        "type": "knn_vector",
                        "dimension": 1024,
                        "space_type": "l2",
                        "method": {
                            "name": "hnsw",
                            "engine": "faiss"
                        }
                    },
                    "text_blob": {
                        "type": "text"
                    },
                    "attributes": {
                        "type": "text"
                    }
                }

            }
        }
        """

    def drop_index(self, index) -> None:
        return self._opensearch.indices.delete(index=index)

    def create_index(self, index: str, body: dict[str, Any] = None) -> None:
        body = body or {}
        return self._opensearch.indices.create(index=index, body=body)

    def exists_index(self, index: str) -> None:
        return self._opensearch.indices.exists(index=index)

    def get_index_mapping(self, index_name):
        return self._opensearch.indices.get_mapping(index=index_name)

    @cached_property
    def _estimated_inmemory_data_size(self) -> Optional[int]:
        stats = self._opensearch.indices.stats(
            index=self._index,
            metric="store",
        )
        if "store" not in stats["_all"]["total"]:
            return None
        return stats["_all"]["total"]["store"]["size_in_bytes"]

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._estimated_inmemory_data_size

    @staticmethod
    def _get_read_task(
        pit_id: str,
        query_predicate: Optional[Mapping[str, Any]],
        slice_id: int,
        slice_max: int,
        chunk_size: int,
        client_kwargs: dict[str, Any],
        schema: Optional[Union[type, Schema]],
    ) -> ReadTask:
        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=schema,
            input_files=None,
            exec_stats=None,
        )

        def iter_blocks() -> Iterator[Table]:
            opensearch = OpenSearch(**client_kwargs)
            search_after: Any = None
            query = OpenSearchDatasource.build_query(query_predicate)
            query["pit"] = {"id": pit_id}
            query["slice"] = {"id": slice_id, "max": slice_max}
            query["sort"] = ["_doc"]
            query["size"] = chunk_size

            while True:
                if search_after is not None:
                    query["search_after"] = search_after
                response = opensearch.search(
                    body=query,
                )
                hits = response["hits"]["hits"]
                if len(hits) == 0:
                    break
                yield Table.from_pylist(
                    mapping=hits,
                    schema=(
                        schema
                        if schema is not None and isinstance(schema, Schema)
                        else None
                    ),
                )
                search_after = max(hit["sort"] for hit in hits)

        return ReadTask(
            read_fn=iter_blocks,
            metadata=metadata,
        )

    def get_read_tasks(self, parallelism: int) -> list[ReadTask]:
        pit_id: str = self._opensearch.create_point_in_time(
            index=self._index,
            keep_alive=self._keep_alive,
        )["pit_id"]
        try:
            return [
                self._get_read_task(
                    pit_id=pit_id,
                    query_predicate=self._query,
                    slice_id=i,
                    slice_max=parallelism,
                    chunk_size=self._chunk_size,
                    client_kwargs=self._client_kwargs,
                    schema=self._schema,
                )
                for i in range(parallelism)
            ]
        except Exception as e:
            self._OpenSearch.close_point_in_time(body={"id": pit_id})
            raise e

    @staticmethod
    def query_hybrid_search_or(
        vector_mapping, vector, keyword_mapping, keyword, top_k=5
    ):
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {keyword_mapping: keyword}},
                        {"knn": {vector_mapping: {"vector": vector, "k": top_k}}},
                    ],
                }
            }
        }
        return query

    @staticmethod
    def query_all():
        query = {"query": {"match_all": {}}}
        return query

    @property
    def supports_distributed_reads(self) -> bool:
        return True
