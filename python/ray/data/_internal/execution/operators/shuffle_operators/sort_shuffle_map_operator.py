from typing import Any, Dict, List, Optional

import pyarrow as pa

from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    PartitionFn,
)
from ray.data._internal.execution.operators.shuffle_operators.sort_sampling_operator import (  # noqa: E501
    SortSamplingOp,
)
from ray.data._internal.execution.operators.sort_shuffle import make_range_partition_fn
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.context import DataContext


def _uninitialized_partition_fn(block):
    raise RuntimeError("Sort shuffle boundaries have not been sampled yet")


class SortRangePartitioner:
    """Resolves the range boundaries of a sort shuffle and builds its partition fn.

    Boundaries come from the user (``sort_key.boundaries``), are empty when there
    is a single partition, or are read from the upstream ``SortSamplingOp`` the
    first time an input arrives. ``SortShuffleMapOp`` resolves them per input;
    the disk writer (see ``_plan_sort_v2``) ships the partitioner itself as its
    partition fn, resolved once on the first input before the map tasks pick
    it up.
    """

    def __init__(
        self,
        sort_key: SortKey,
        num_partitions: int,
        input_op: PhysicalOperator,
        data_context: DataContext,
    ):
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")

        self._sort_key = sort_key
        self._data_context = data_context
        self._boundaries: Optional[List] = None
        self._partition_fn: PartitionFn = _uninitialized_partition_fn

        user_boundaries = self._user_boundaries(sort_key)
        if user_boundaries is None and num_partitions == 1:
            # One partition: every row goes to it, nothing to sample.
            user_boundaries = []
        if user_boundaries is None and not isinstance(input_op, SortSamplingOp):
            raise ValueError(
                "Sort shuffle map requires either user-provided boundaries "
                "or a SortSamplingOp"
            )
        if user_boundaries is not None:
            self._set_boundaries(user_boundaries)

    @staticmethod
    def _user_boundaries(sort_key: SortKey) -> Optional[List]:
        if not sort_key.boundaries:
            return None
        boundaries = [(boundary,) for boundary in sort_key.boundaries]
        if sort_key.get_descending()[0]:
            boundaries.reverse()
        return boundaries

    @property
    def boundaries(self) -> Optional[List]:
        return self._boundaries

    def partition_fn_for(
        self, schema: Optional[pa.Schema], input_op: PhysicalOperator
    ) -> PartitionFn:
        """Validate ``schema`` and return the partition fn for inputs with it.

        Sampled boundaries are read from ``input_op`` on the first call; the
        sampling op only forwards inputs once they are ready.
        """
        self._sort_key.validate_schema(schema)
        if self._boundaries is None:
            assert isinstance(input_op, SortSamplingOp)
            boundaries = input_op.boundaries
            if boundaries is None:
                raise RuntimeError(
                    "SortSamplingOp forwarded input before range boundaries were ready"
                )
            self._set_boundaries(boundaries)
        return self._partition_fn

    def __call__(self, block: pa.Table) -> Dict[int, pa.Table]:
        return self._partition_fn(block)

    def _set_boundaries(self, boundaries: List) -> None:
        self._boundaries = boundaries
        self._partition_fn = make_range_partition_fn(
            boundaries,
            self._sort_key,
            self._data_context,
        )


class SortShuffleMapOp(ShuffleMapOp):
    """Locally sort and range-partition inputs for sort shuffle v2.

    When the user does not provide boundaries, the upstream ``SortSamplingOp``
    computes them before forwarding any inputs. Sampling lifecycle and buffering
    therefore remain separate from the regular shuffle-map implementation.
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        num_partitions: int,
        sort_key: SortKey,
        map_runtime_env: Optional[Dict[str, Any]] = None,
        map_cpus: float = ShuffleMapOp._DEFAULT_SHUFFLE_MAP_TASK_NUM_CPUS,
        name: str = "SortShuffleMap",
    ):
        self._partitioner = SortRangePartitioner(
            sort_key, num_partitions, input_op, data_context
        )
        super().__init__(
            input_op,
            data_context,
            num_partitions=num_partitions,
            partition_fn=_uninitialized_partition_fn,
            map_runtime_env=map_runtime_env,
            map_cpus=map_cpus,
            name=name,
        )

    @property
    def boundaries(self) -> Optional[List]:
        return self._partitioner.boundaries

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        self._partition_fn = self._partitioner.partition_fn_for(
            refs.schema, self.input_dependencies[0]
        )
        super()._add_input_inner(refs, input_index)
