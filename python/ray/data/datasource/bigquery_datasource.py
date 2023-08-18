import logging
import os
import tempfile
import time
import uuid
from typing import Any, Dict, List, Optional

import pyarrow.parquet as pq
from google.api_core import exceptions
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import types

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, Reader, ReadTask, WriteResult
from ray.types import ObjectRef

logger = logging.getLogger(__name__)

class _BigQueryDatasourceReader(Reader):
    def __init__(
        self,
        project_id: str,
        dataset: Optional[str] = None,
        query: Optional[str] = None,
        parallelism: Optional[int] = -1,
        **kwargs: Optional[Dict[str, Any]],
    ):
        self._project_id = project_id
        self._dataset = dataset
        self._query = query
        self._kwargs = kwargs

        if query is not None and dataset is not None:
            raise ValueError(
                "Query and dataset kwargs cannot both be provided"
                + " (must be mutually exclusive)."
            )

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # Executed by a worker node
        def _read_single_partition(stream) -> Block:
            client = bigquery_storage.BigQueryReadClient()
            reader = client.read_rows(stream.name)
            return reader.to_arrow()

        if self._query:
            query_client = bigquery.Client(project=self._project_id)
            query_job = query_client.query(self._query)
            query_job.result()
            destination = str(query_job.destination)
            dataset_id = destination.split(".")[-2]
            table_id = destination.split(".")[-1]
        else:
            self._validate_dataset_table_exist(self._project_id, self._dataset)
            dataset_id = self._dataset.split(".")[0]
            table_id = self._dataset.split(".")[1]

        bqs_client = bigquery_storage.BigQueryReadClient()
        table = f"projects/{self._project_id}/datasets/{dataset_id}/tables/{table_id}"

        if parallelism == -1:
            parallelism = None
        requested_session = types.ReadSession(
            table=table,
            data_format=types.DataFormat.ARROW,
        )
        read_session = bqs_client.create_read_session(
            parent=f"projects/{self._project_id}",
            read_session=requested_session,
            max_stream_count=parallelism,
        )

        read_tasks = []
        logger.info("Created streams: " + str(len(read_session.streams)))
        if len(read_session.streams) < parallelism:
            logger.info(
                "The number of streams created by the "
                + "BigQuery Storage Read API is less than the requested "
                + "parallelism due to the size of the dataset."
            )

        for stream in read_session.streams:
            # Create a metadata block object to store schema, etc.
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                schema=None,
                input_files=None,
                exec_stats=None,
            )

            # Create a no-arg wrapper read function which returns a block
            read_single_partition = lambda stream=stream: [
                _read_single_partition(stream)
            ]

            # Create the read task and pass the wrapper and metadata in
            read_task = ReadTask(read_single_partition, metadata)
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def _validate_dataset_table_exist(self, project_id: str, dataset: str) -> None:
        client = bigquery.Client(project=project_id)
        dataset_id = dataset.split(".")[0]
        try:
            client.get_dataset(dataset_id)
        except exceptions.NotFound:
            raise ValueError(
                "Dataset {} is not found. Please ensure that it exists.".format(
                    dataset_id
                )
            )

        try:
            client.get_table(dataset)
        except exceptions.NotFound:
            raise ValueError(
                "Table {} is not found. Please ensure that it exists.".format(dataset)
            )


class BigQueryDatasource(Datasource):
    def create_reader(self, **kwargs) -> Reader:
        return _BigQueryDatasourceReader(**kwargs)

    def write(
        self,
        blocks: List[ObjectRef[Block]],
        ctx: TaskContext,
        project_id: str,
        dataset: str,
    ) -> WriteResult:
        def _write_single_block(
            block: Block, project_id: str, dataset: str
        ):
            block = BlockAccessor.for_block(block).to_arrow()

            client = bigquery.Client(project=project_id)
            job_config = bigquery.LoadJobConfig(autodetect=True)
            job_config.source_format = bigquery.SourceFormat.PARQUET
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

            with tempfile.TemporaryDirectory() as temp_dir:
                fp = os.path.join(temp_dir, f"block_{uuid.uuid4()}.parquet")
                pq.write_table(block, fp, compression="SNAPPY")

                retry_cnt = 0
                while retry_cnt < 10:
                    with open(fp, "rb") as source_file:
                        job = client.load_table_from_file(
                            source_file, dataset, job_config=job_config
                        )
                    retry_cnt += 1
                    try:
                        logger.info(job.result())
                        break
                    except exceptions.Forbidden as e:
                        logger.info("Rate limit exceeded... Sleeping to try again")
                        logger.debug(e)
                        time.sleep(11)

        # Set up datasets to write
        client = bigquery.Client(project=project_id)
        dataset_id = dataset.split(".", 1)[0]
        try:
            client.create_dataset(f"{project_id}.{dataset_id}", timeout=30)
            logger.info("Created dataset " + dataset_id)
        except exceptions.Conflict:
            logger.info(
                "Dataset " +
                dataset_id +
                " already exists. The table will be overwritten if it already exists.",
            )

        # Delete table if it already exists
        client.delete_table(f"{project_id}.{dataset}", not_found_ok=True)

        for block in blocks:
            _write_single_block(
                block, project_id, dataset
            )
        return "ok"
