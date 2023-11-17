import logging
import os
import tempfile
import time
import uuid
from typing import Any, Iterable

import pyarrow.parquet as pq

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

logger = logging.getLogger(__name__)

MAX_RETRY_CNT = 10
RATE_LIMIT_EXCEEDED_SLEEP_TIME = 11


class _BigQueryDatasink(Datasink):
    def __init__(self, project_id: str, dataset: str) -> None:
        _check_import(self, module="google.cloud", package="bigquery")
        _check_import(self, module="google.cloud", package="bigquery_storage")
        _check_import(self, module="google.api_core", package="exceptions")

        self.project_id = project_id
        self.dataset = dataset

    def on_write_start(self) -> None:
        from google.api_core import exceptions
        from google.cloud import bigquery

        if self.project_id is None or self.dataset is None:
            raise ValueError("project_id and dataset are required args")

        # Set up datasets to write
        client = bigquery.Client(project=self.project_id)
        dataset_id = self.dataset.split(".", 1)[0]
        try:
            client.create_dataset(f"{self.project_id}.{dataset_id}", timeout=30)
            logger.info("Created dataset " + dataset_id)
        except exceptions.Conflict:
            logger.info(
                f"Dataset {dataset_id} already exists. "
                "The table will be overwritten if it already exists."
            )

        # Delete table if it already exists
        client.delete_table(f"{self.project_id}.{self.dataset}", not_found_ok=True)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        def _write_single_block(block: Block, project_id: str, dataset: str) -> None:
            from google.api_core import exceptions
            from google.cloud import bigquery

            block = BlockAccessor.for_block(block).to_arrow()

            client = bigquery.Client(project=project_id)
            job_config = bigquery.LoadJobConfig(autodetect=True)
            job_config.source_format = bigquery.SourceFormat.PARQUET
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

            with tempfile.TemporaryDirectory() as temp_dir:
                fp = os.path.join(temp_dir, f"block_{uuid.uuid4()}.parquet")
                pq.write_table(block, fp, compression="SNAPPY")

                retry_cnt = 0
                while retry_cnt < MAX_RETRY_CNT:
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
                        time.sleep(RATE_LIMIT_EXCEEDED_SLEEP_TIME)

                # Raise exception if retry_cnt hits MAX_RETRY_CNT
                if retry_cnt >= MAX_RETRY_CNT:
                    raise RuntimeError(
                        f"Write failed due to {MAX_RETRY_CNT} repeated"
                        + " API rate limit exceeded responses"
                    )

        _write_single_block = cached_remote_fn(_write_single_block)

        # Launch a remote task for each block within this write task
        for block in blocks:
            _write_single_block.remote(block, self.project_id, self.dataset)
        return "ok"
