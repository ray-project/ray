from typing import Any, Dict, Optional, Union

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.interfaces.operator import DatasetExportArgs
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource


class Write(DatasetExportArgs, AbstractMap):
    """Logical operator for write."""

    def __init__(
        self,
        input_op: LogicalOperator,
        datasink_or_legacy_datasource: Union[Datasink, Datasource],
        ray_remote_args: Optional[Dict[str, Any]] = None,
        concurrency: Optional[int] = None,
        **write_args,
    ):
        if isinstance(datasink_or_legacy_datasource, Datasink):
            min_rows_per_bundled_input = (
                datasink_or_legacy_datasource.min_rows_per_write
            )
        else:
            min_rows_per_bundled_input = None

        super().__init__(
            "Write",
            input_op,
            min_rows_per_bundled_input=min_rows_per_bundled_input,
            ray_remote_args=ray_remote_args,
        )
        self._datasink_or_legacy_datasource = datasink_or_legacy_datasource
        self._write_args = write_args
        self._concurrency = concurrency

    def dataset_export_args(self) -> Dict[str, Any]:
        """Export the arguments into dictionary format."""
        return {
            "datasink": self._datasink_or_legacy_datasource.get_name(),
            "write_args": self._write_args,
            "ray_remote_args": self._ray_remote_args,
            "concurrency": self._concurrency,
        }
