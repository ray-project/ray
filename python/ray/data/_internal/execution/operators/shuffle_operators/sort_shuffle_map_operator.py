from typing import Any, Dict, List, Optional

from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.sort_sampling_operator import (  # noqa: E501
    SortSamplingOp,
)
from ray.data._internal.execution.operators.sort_shuffle import make_range_partition_fn
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.context import DataContext


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
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")

        self._sort_key = sort_key
        self._boundaries = self._user_boundaries(sort_key)
        if self._boundaries is None and not isinstance(input_op, SortSamplingOp):
            raise ValueError(
                "SortShuffleMapOp requires either user-provided boundaries "
                "or a SortSamplingOp"
            )

        partition_fn = (
            make_range_partition_fn(self._boundaries, sort_key, data_context)
            if self._boundaries is not None
            else self._uninitialized_partition_fn
        )
        super().__init__(
            input_op,
            data_context,
            num_partitions=num_partitions,
            partition_fn=partition_fn,
            map_runtime_env=map_runtime_env,
            map_cpus=map_cpus,
            name=name,
        )

    @staticmethod
    def _user_boundaries(sort_key: SortKey) -> Optional[List]:
        if not sort_key.boundaries:
            return None
        boundaries = [(boundary,) for boundary in sort_key.boundaries]
        if sort_key.get_descending()[0]:
            boundaries.reverse()
        return boundaries

    @staticmethod
    def _uninitialized_partition_fn(block):
        raise RuntimeError("Sort shuffle boundaries have not been sampled yet")

    @property
    def boundaries(self) -> Optional[List]:
        return self._boundaries

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        self._sort_key.validate_schema(refs.schema)
        self._ensure_boundaries()
        super()._add_input_inner(refs, input_index)

    def _ensure_boundaries(self) -> None:
        if self._boundaries is not None:
            return
        sampling_op = self.input_dependencies[0]
        assert isinstance(sampling_op, SortSamplingOp)
        boundaries = sampling_op.boundaries
        if boundaries is None:
            raise RuntimeError(
                "SortSamplingOp forwarded input before range boundaries were ready"
            )
        self._boundaries = boundaries
        self._partition_fn = make_range_partition_fn(
            boundaries,
            self._sort_key,
            self.data_context,
        )
