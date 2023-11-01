import json
import logging
import time
from typing import List, Optional
from urllib.parse import urlencode, urljoin

import pyarrow
import requests

from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


_STATEMENT_EXEC_POLL_TIME_S = 1


@PublicAPI(stability="alpha")
class DatabricksUCDatasource(Datasource):
    def __init__(
        self,
        host: str,
        token: str,
        warehouse_id: str,
        catalog: str,
        schema: str,
        query: str,
    ):
        self.host = host
        self.token = token
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.query = query

        url_base = f"https://{self.host}/api/2.0/sql/statements/"

        payload = json.dumps(
            {
                "statement": self.query,
                "warehouse_id": self.warehouse_id,
                "wait_timeout": "0s",
                "disposition": "EXTERNAL_LINKS",
                "format": "ARROW_STREAM",
                "catalog": self.catalog,
                "schema": self.schema,
            }
        )

        req_auth = ("token", self.token)
        req_headers = {"Content-Type": "application/json"}

        response = requests.post(
            url_base,
            auth=("token", self.token),
            headers={"Content-Type": "application/json"},
            data=payload,
        )
        response.raise_for_status()
        statement_id = response.json()["statement_id"]

        state = response.json()["status"]["state"]

        logger.info(f"Waiting for query {query!r} execution result.")
        try:
            while state in ["PENDING", "RUNNING"]:
                time.sleep(_STATEMENT_EXEC_POLL_TIME_S)
                response = requests.get(
                    urljoin(url_base, statement_id) + "/",
                    auth=req_auth,
                    headers=req_headers,
                )
                response.raise_for_status()
                state = response.json()["status"]["state"]
        except KeyboardInterrupt:
            # User cancel the command, so we cancel query execution.
            requests.post(
                urljoin(url_base, f"{statement_id}/cancel"),
                auth=req_auth,
                headers=req_headers,
            )
            try:
                response.raise_for_status()
            except Exception as e:
                logger.warning(
                    f"Canceling query {query!r} execution failed, reason: {repr(e)}."
                )
            raise

        if state != "SUCCEEDED":
            raise RuntimeError(f"Query {self.query!r} execution failed.")

        manifest = response.json()["manifest"]
        is_truncated = manifest["truncated"]

        if is_truncated:
            logger.warning(
                f"The result dataset of '{query!r}' exceeding 100GiB and it is "
                "truncated."
            )

        chunks = manifest["chunks"]

        # Make chunks metadata are ordered by index.
        chunks = sorted(chunks, key=lambda x: x["chunk_index"])
        num_chunks = len(chunks)
        self.num_chunks = num_chunks
        self._estimate_inmemory_data_size = sum(chunk["byte_count"] for chunk in chunks)

        def get_read_task(task_index, parallelism):
            # 0 <= task_index < parallelism
            chunk_index_list = list(range(task_index, parallelism, num_chunks))

            num_rows = sum(
                chunks[chunk_index]["row_count"] for chunk_index in chunk_index_list
            )
            size_bytes = sum(
                chunks[chunk_index]["byte_count"] for chunk_index in chunk_index_list
            )

            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=size_bytes,
                schema=None,
                input_files=None,
                exec_stats=None,
            )

            def read_fn():
                for chunk_index in chunk_index_list:
                    chunk_info = chunks[chunk_index]
                    row_offset_param = urlencode(
                        {"row_offset": chunk_info["row_offset"]}
                    )
                    resolve_external_link_url = urljoin(
                        url_base,
                        f"{statement_id}/result/chunks/{chunk_index}?"
                        f"{row_offset_param}",
                    )

                    resolve_response = requests.get(
                        resolve_external_link_url, auth=req_auth, headers=req_headers
                    )
                    resolve_response.raise_for_status()
                    external_url = resolve_response.json()["external_links"][0][
                        "external_link"
                    ]
                    # NOTE: do _NOT_ send the authorization header to external urls
                    raw_response = requests.get(external_url, auth=None, headers=None)
                    raw_response.raise_for_status()

                    arrow_table = pyarrow.ipc.open_stream(
                        raw_response.content
                    ).read_all()
                    yield arrow_table

            return ReadTask(read_fn=read_fn, metadata=metadata)

        self._get_read_task = get_read_task

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._estimate_inmemory_data_size

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        assert parallelism > 0, f"Invalid parallelism {parallelism}"

        if parallelism > self.num_chunks:
            parallelism = self.num_chunks

        return [self._get_read_task(index, parallelism) for index in range(parallelism)]
