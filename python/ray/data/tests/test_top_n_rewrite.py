from typing import List, Optional

import pytest

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
)
from ray.data._internal.logical.operators import Limit, MapRows, Sort, TopN
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.logical.rules.top_n_rewrite import (
    TOP_N_REWRITE_ENABLED_CONFIG_KEY,
    TOP_N_REWRITE_MAX_LIMIT_CONFIG_KEY,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.block import BlockMetadata
from ray.data.context import DataContext, ShuffleStrategy


class _Input(LogicalOperator):
    def __init__(self, num_rows: int = 100):
        object.__setattr__(self, "_input_dependencies", [])
        self._num_rows = num_rows

    def infer_metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self._num_rows,
            size_bytes=800,
            input_files=None,
            exec_stats=None,
        )


def _make_context(*, enabled: bool = True) -> DataContext:
    context = DataContext()
    context.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    context.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, enabled)
    return context


def _optimize_dag(operator: LogicalOperator, context: DataContext) -> LogicalOperator:
    dag = LogicalOptimizer().optimize(LogicalPlan(operator, context)).dag
    assert isinstance(dag, LogicalOperator)
    return dag


def _make_sort(
    input_operator: LogicalOperator,
    *,
    boundaries: Optional[List[int]] = None,
) -> Sort:
    return Sort(
        sort_key=SortKey("value", boundaries=boundaries),
        input_dependencies=[input_operator],
        ray_remote_args={"num_cpus": 2},
    )


def test_rewrite_sort_limit_to_top_n():
    input_operator = _Input()
    optimized = _optimize_dag(
        Limit(5, input_dependencies=[_make_sort(input_operator)]),
        _make_context(),
    )

    assert isinstance(optimized, TopN)
    assert optimized.limit == 5
    assert optimized.input_dependencies == [input_operator]
    assert optimized.sort_key.get_columns() == ["value"]
    assert optimized.ray_remote_args == {"num_cpus": 2}
    assert optimized.infer_metadata().num_rows == 5


def test_rewrite_sort_map_limit_to_top_n_map():
    input_operator = _Input()
    map_operator = MapRows(
        fn=lambda row: row,
        input_dependencies=[_make_sort(input_operator)],
    )

    optimized = _optimize_dag(
        Limit(5, input_dependencies=[map_operator]),
        _make_context(),
    )

    assert isinstance(optimized, MapRows)
    assert isinstance(optimized.input_dependencies[0], TopN)
    assert optimized.input_dependencies[0].input_dependencies == [input_operator]


def test_rewrite_consecutive_limits_uses_minimum():
    input_operator = _Input()
    inner_limit = Limit(100, input_dependencies=[_make_sort(input_operator)])
    optimized = _optimize_dag(
        Limit(10, input_dependencies=[inner_limit]),
        _make_context(),
    )

    assert isinstance(optimized, TopN)
    assert optimized.limit == 10


@pytest.mark.parametrize("limit", [0, -1])
def test_rewrite_keeps_nonpositive_limit_above_top_n(limit):
    input_operator = _Input()
    top_n = TopN(
        sort_key=SortKey("value"),
        limit=10,
        input_dependencies=[input_operator],
    )

    optimized = _optimize_dag(
        Limit(limit, input_dependencies=[top_n]),
        _make_context(),
    )

    assert isinstance(optimized, Limit)
    assert optimized.limit == limit
    assert optimized.input_dependencies == [top_n]


def test_rewrite_consecutive_sorts_uses_last_sort_key():
    input_operator = _Input()
    first_sort = _make_sort(input_operator)
    second_sort = Sort(
        sort_key=SortKey("other", descending=True),
        input_dependencies=[first_sort],
    )
    optimized = _optimize_dag(
        Limit(5, input_dependencies=[second_sort]),
        _make_context(),
    )

    assert isinstance(optimized, TopN)
    assert optimized.sort_key.get_columns() == ["other"]
    assert optimized.sort_key.get_descending() == [True]
    assert optimized.input_dependencies == [input_operator]


@pytest.mark.parametrize(
    ("enabled", "limit", "boundaries", "max_limit", "shuffle_strategy"),
    [
        pytest.param(
            False,
            5,
            None,
            None,
            ShuffleStrategy.SHUFFLE_V2,
            id="disabled",
        ),
        pytest.param(
            True,
            0,
            None,
            None,
            ShuffleStrategy.SHUFFLE_V2,
            id="zero-limit",
        ),
        pytest.param(
            True,
            5,
            [10],
            None,
            ShuffleStrategy.SHUFFLE_V2,
            id="custom-boundaries",
        ),
        pytest.param(
            True,
            5,
            None,
            4,
            ShuffleStrategy.SHUFFLE_V2,
            id="above-max-limit",
        ),
        pytest.param(
            True,
            5,
            None,
            None,
            ShuffleStrategy.SORT_SHUFFLE_PULL_BASED,
            id="non-v2",
        ),
    ],
)
def test_rewrite_keeps_ineligible_sort_limit(
    enabled,
    limit,
    boundaries,
    max_limit,
    shuffle_strategy,
):
    context = _make_context(enabled=enabled)
    context.shuffle_strategy = shuffle_strategy
    if max_limit is not None:
        context.set_config(TOP_N_REWRITE_MAX_LIMIT_CONFIG_KEY, max_limit)
    input_operator = _Input()
    operator = Limit(
        limit,
        input_dependencies=[_make_sort(input_operator, boundaries=boundaries)],
    )

    optimized = _optimize_dag(operator, context)

    assert isinstance(optimized, Limit)
    assert isinstance(optimized.input_dependencies[0], Sort)


def test_rewrite_rejects_invalid_max_limit():
    context = _make_context()
    context.set_config(TOP_N_REWRITE_MAX_LIMIT_CONFIG_KEY, True)

    with pytest.raises(ValueError, match="must be a positive integer"):
        _optimize_dag(
            Limit(5, input_dependencies=[_make_sort(_Input())]),
            context,
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
