import json
import logging
import os
import time
from typing import List, Optional
from urllib.parse import urljoin

import numpy as np
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

        if not host.startswith(("http://", "https://")):
            self.host = f"https://{host}"

        url_base = f"{self.host}/api/2.0/sql/statements/"

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

        req_headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.token,
        }

        response = requests.post(
            url_base,
            headers=req_headers,
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
                    headers=req_headers,
                )
                response.raise_for_status()
                state = response.json()["status"]["state"]
        except KeyboardInterrupt:
            # User cancel the command, so we cancel query execution.
            requests.post(
                urljoin(url_base, f"{statement_id}/cancel"),
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
            raise RuntimeError(
                f"Query {self.query!r} execution failed.\n{response.json()}"
            )

        manifest = response.json()["manifest"]
        self.is_truncated = manifest.get("truncated", False)

        if self.is_truncated:
            logger.warning(
                f"The resulting size of the dataset of '{query!r}' exceeds "
                "100GiB and it is truncated."
            )

        chunks = manifest.get("chunks", [])

        # Make chunks metadata are ordered by index.
        chunks = sorted(chunks, key=lambda x: x["chunk_index"])
        num_chunks = len(chunks)
        self.num_chunks = num_chunks
        self._estimate_inmemory_data_size = sum(chunk["byte_count"] for chunk in chunks)

        def get_read_task(task_index, parallelism):
            # Handle empty chunk list by yielding an empty PyArrow table
            if num_chunks == 0:
                import pyarrow as pa

                metadata = BlockMetadata(
                    num_rows=0,
                    size_bytes=0,
                    input_files=None,
                    exec_stats=None,
                )

                def empty_read_fn():
                    yield pa.Table.from_pydict({})

                return ReadTask(read_fn=empty_read_fn, metadata=metadata)

            # get chunk list to be read in this task and preserve original chunk order
            chunk_index_list = list(
                np.array_split(range(num_chunks), parallelism)[task_index]
            )

            num_rows = sum(
                chunks[chunk_index]["row_count"] for chunk_index in chunk_index_list
            )
            size_bytes = sum(
                chunks[chunk_index]["byte_count"] for chunk_index in chunk_index_list
            )

            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=size_bytes,
                input_files=None,
                exec_stats=None,
            )

            def _read_fn():
                for chunk_index in chunk_index_list:
                    resolve_external_link_url = urljoin(
                        url_base, f"{statement_id}/result/chunks/{chunk_index}"
                    )

                    resolve_response = requests.get(
                        resolve_external_link_url, headers=req_headers
                    )
                    resolve_response.raise_for_status()
                    external_url = resolve_response.json()["external_links"][0][
                        "external_link"
                    ]
                    # NOTE: do _NOT_ send the authorization header to external urls
                    raw_response = requests.get(external_url, auth=None, headers=None)
                    raw_response.raise_for_status()

                    with pyarrow.ipc.open_stream(raw_response.content) as reader:
                        arrow_table = reader.read_all()

                    yield arrow_table

            def read_fn():
                if mock_setup_fn_path := os.environ.get(
                    "RAY_DATABRICKS_UC_DATASOURCE_READ_FN_MOCK_TEST_SETUP_FN_PATH"
                ):
                    import ray.cloudpickle as pickle

                    # This is for testing.
                    with open(mock_setup_fn_path, "rb") as f:
                        mock_setup = pickle.load(f)
                    with mock_setup():
                        yield from _read_fn()
                else:
                    yield from _read_fn()

            return ReadTask(read_fn=read_fn, metadata=metadata)

        self._get_read_task = get_read_task

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._estimate_inmemory_data_size

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # Handle empty dataset case
        if self.num_chunks == 0:
            return [self._get_read_task(0, 1)]

        assert parallelism > 0, f"Invalid parallelism {parallelism}"

        if parallelism > self.num_chunks:
            parallelism = self.num_chunks
            logger.info(
                "The parallelism is reduced to chunk number due to "
                "insufficient chunk parallelism."
            )

        return [self._get_read_task(index, parallelism) for index in range(parallelism)]
