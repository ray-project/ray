import logging
from json import loads
from typing import List, Optional, Tuple

import numpy as np

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class DeltaSharingDatasource(Datasource):
    def __init__(
        self,
        url: str,
        json_predicate_hints: Optional[str] = None,
        limit: Optional[int] = None,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
    ):
        _check_import(self, module="delta_sharing", package="delta-sharing")

        if limit is not None:
            assert (
                isinstance(limit, int) and limit >= 0
            ), "'limit' must be a non-negative int"

        self._url = url
        self._json_predicate_hints = json_predicate_hints
        self._limit = limit
        self._version = version
        self._timestamp = timestamp

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def _read_files(self, files, converters):
        """Read files with Delta Sharing."""
        from delta_sharing.reader import DeltaSharingReader

        for file in files:
            yield DeltaSharingReader._to_pandas(
                action=file, converters=converters, for_cdf=False, limit=None
            )

    def setup_delta_sharing_connections(self, url: str):
        """
        Set up delta sharing connections based on the url.

        :param url: a url under the format "<profile>#<share>.<schema>.<table>"
        :
        """
        from delta_sharing.protocol import DeltaSharingProfile, Table
        from delta_sharing.rest_client import DataSharingRestClient

        profile_str, share, schema, table_str = _parse_delta_sharing_url(url)
        table = Table(name=table_str, share=share, schema=schema)

        profile = DeltaSharingProfile.read_from_file(profile_str)
        rest_client = DataSharingRestClient(profile)
        return table, rest_client

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        assert parallelism > 0, f"Invalid parallelism {parallelism}"
        from delta_sharing.converter import to_converters

        self._table, self._rest_client = self.setup_delta_sharing_connections(self._url)
        self._response = self._rest_client.list_files_in_table(
            self._table,
            jsonPredicateHints=self._json_predicate_hints,
            limitHint=self._limit,
            version=self._version,
            timestamp=self._timestamp,
        )

        if len(self._response.add_files) == 0 or self._limit == 0:
            logger.warning("No files found from the delta sharing table or limit is 0")

        schema_json = loads(self._response.metadata.schema_string)
        self._converters = to_converters(schema_json)

        read_tasks = []
        # get file list to be read in this task and preserve original chunk order
        for files in np.array_split(self._response.add_files, parallelism):
            files = files.tolist()
            metadata = BlockMetadata(
                num_rows=None,
                schema=None,
                input_files=files,
                size_bytes=None,
                exec_stats=None,
            )
            converters = self._converters
            read_task = ReadTask(
                lambda f=files: self._read_files(f, converters),
                metadata,
            )
            read_tasks.append(read_task)

        return read_tasks


def _parse_delta_sharing_url(url: str) -> Tuple[str, str, str, str]:
    """
    Developed from delta_sharing's _parse_url function.
    https://github.com/delta-io/delta-sharing/blob/main/python/delta_sharing/delta_sharing.py#L36

    Args:
        url: a url under the format "<profile>#<share>.<schema>.<table>"

    Returns:
        a tuple with parsed (profile, share, schema, table)
    """
    shape_index = url.rfind("#")
    if shape_index < 0:
        raise ValueError(f"Invalid 'url': {url}")
    profile = url[0:shape_index]
    fragments = url[shape_index + 1 :].split(".")
    if len(fragments) != 3:
        raise ValueError(f"Invalid 'url': {url}")
    share, schema, table = fragments
    if len(profile) == 0 or len(share) == 0 or len(schema) == 0 or len(table) == 0:
        raise ValueError(f"Invalid 'url': {url}")
    return (profile, share, schema, table)
