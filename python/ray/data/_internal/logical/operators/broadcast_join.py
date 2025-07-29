"""Broadcast join implementation for Ray Data using map_batches pattern."""

from typing import Optional, Tuple

import ray
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data.block import DataBatch

_JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP = {
    JoinType.INNER: "inner",
    JoinType.LEFT_OUTER: "left outer",
    JoinType.RIGHT_OUTER: "right outer",
    JoinType.FULL_OUTER: "full outer",
}


class BroadcastJoinFunction:
    """A callable class that performs broadcast joins using PyArrow.

    This class is designed to be used with Dataset.map_batches() to implement
    broadcast joins. The right dataset is materialized and broadcasted in __init__,
    and each call performs a PyArrow join on a batch from the left dataset.
    """

    def __init__(
        self,
        broadcast_table_ref,
        join_type: JoinType,
        left_key_columns: Tuple[str],
        right_key_columns: Tuple[str],
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
    ):
        """Initialize the broadcast join function.

        Args:
            broadcast_table_ref: Ray object reference to the materialized right table
            join_type: Type of join to perform
            left_key_columns: Join keys for left dataset
            right_key_columns: Join keys for right dataset
            left_columns_suffix: Suffix for left columns
            right_columns_suffix: Suffix for right columns
        """
        self.join_type = join_type
        self.left_key_columns = left_key_columns
        self.right_key_columns = right_key_columns
        self.left_columns_suffix = left_columns_suffix
        self.right_columns_suffix = right_columns_suffix

        # Materialize the right table from the Ray object reference
        try:
            self.right_table = ray.get(broadcast_table_ref)
        except Exception as e:
            raise UserWarning(
                f"Warning: {e}. \nThe dataset being broadcast is likely too large to fit in memory."
            )

    def __call__(self, batch: DataBatch) -> DataBatch:
        """Perform PyArrow join on a batch from the left dataset.

        Args:
            batch: Batch from left dataset

        Returns:
            Joined batch
        """
        import pyarrow as pa

        # Convert left batch to PyArrow table
        if isinstance(batch, dict):
            left_table = pa.table(batch)
        else:
            left_table = batch

        # Perform the join
        arrow_join_type = _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[self.join_type]
        joined_table = left_table.join(
            self.right_table,
            join_type=arrow_join_type,
            keys=list(self.left_key_columns),
            right_keys=list(self.right_key_columns),
            left_suffix=self.left_columns_suffix,
            right_suffix=self.right_columns_suffix,
        )

        return joined_table
