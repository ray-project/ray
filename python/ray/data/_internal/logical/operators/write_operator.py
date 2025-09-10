from typing import Any, Dict, List, Optional, Union

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource


class Write(AbstractMap):
    """Logical operator for write."""

    def __init__(
        self,
        input_op: LogicalOperator,
        datasink_or_legacy_datasource: Union[Datasink, Datasource, List[Datasink]],
        ray_remote_args: Optional[Dict[str, Any]] = None,
        concurrency: Optional[int] = None,
        **write_args,
    ):
        # Handle both single datasink and multi-datasink cases
        if isinstance(datasink_or_legacy_datasource, list):
            # Multi-datasink case: use the minimum rows requirement across all datasinks
            min_rows_per_bundled_input = None
            for datasink in datasink_or_legacy_datasource:
                if isinstance(datasink, Datasink) and datasink.min_rows_per_write is not None:
                    if min_rows_per_bundled_input is None:
                        min_rows_per_bundled_input = datasink.min_rows_per_write
                    else:
                        min_rows_per_bundled_input = min(
                            min_rows_per_bundled_input, datasink.min_rows_per_write
                        )
        elif isinstance(datasink_or_legacy_datasource, Datasink):
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
        
    @property
    def is_multi_write(self) -> bool:
        """Returns True if this is a multi-write operation."""
        return isinstance(self._datasink_or_legacy_datasource, list)
        
    @property
    def datasinks(self) -> List[Datasink]:
        """Returns list of datasinks for multi-write operations."""
        if self.is_multi_write:
            return self._datasink_or_legacy_datasource
        else:
            return [self._datasink_or_legacy_datasource]
