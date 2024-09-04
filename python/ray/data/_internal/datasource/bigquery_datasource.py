import logging
from typing import List, Optional

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)


class BigQueryDatasource(Datasource):
    def __init__(
        self,
        project_id: str,
        dataset: Optional[str] = None,
        query: Optional[str] = None,
    ):
        _check_import(self, module="google.cloud", package="bigquery")
        _check_import(self, module="google.cloud", package="bigquery_storage")
        _check_import(self, module="google.api_core", package="exceptions")

        self._project_id = project_id
        self._dataset = dataset
        self._query = query

        if query is not None and dataset is not None:
            raise ValueError(
                "Query and dataset kwargs cannot both be provided "
                + "(must be mutually exclusive)."
            )

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        from google.cloud import bigquery, bigquery_storage

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
        requested_session = bigquery_storage.types.ReadSession(
            table=table,
            data_format=bigquery_storage.types.DataFormat.ARROW,
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

            # Create the read task and pass the no-arg wrapper and metadata in
            read_task = ReadTask(
                lambda stream=stream: [_read_single_partition(stream)],
                metadata,
            )
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def _validate_dataset_table_exist(self, project_id: str, dataset: str) -> None:
        from google.api_core import exceptions
        from google.cloud import bigquery

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
