import os
import sys
import pytest

import ray
from ray.util.scheduling_strategies import (
    In,
    NotIn,
    Exists,
    DoesNotExist,
    NodeLabelSchedulingStrategy,
)


@ray.remote
class MyActor:
    def __init__(self):
        pass

    def double(self, x):
        return 2 * x


@pytest.mark.parametrize(
    "call_ray_start",
    ['ray start --head --labels={"gpu_type":"A100","region":"us"}'],
    indirect=True,
)
def test_node_label_scheduling_basic(call_ray_start):
    ray.init(address=call_ray_start)
    MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            {"gpu_type": In("A100", "T100"), "region": Exists()}
        )
    )

    MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            {"gpu_type": NotIn("A100", "T100"), "other_key": DoesNotExist()}
        )
    )

    MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={"gpu_type": Exists()},
            soft={"gpu_type": In("A100")},
        )
    )


def test_node_label_scheduling_invalid_paramter(call_ray_start):
    ray.init(address=call_ray_start)
    with pytest.raises(
        ValueError, match="Type of value in position 0 for the In operator must be str"
    ):
        MyActor.options(
            scheduling_strategy=NodeLabelSchedulingStrategy({"gpu_type": In(123)})
        )

    with pytest.raises(
        ValueError,
        match="Type of value in position 0 for the NotIn operator must be str",
    ):
        MyActor.options(
            scheduling_strategy=NodeLabelSchedulingStrategy({"gpu_type": NotIn(123)})
        )

    with pytest.raises(
        ValueError,
        match="The variadic parameter of the In operator must be a non-empty tuple",
    ):
        MyActor.options(
            scheduling_strategy=NodeLabelSchedulingStrategy({"gpu_type": In()})
        )

    with pytest.raises(
        ValueError,
        match="The variadic parameter of the NotIn operator must be a non-empty tuple",
    ):
        MyActor.options(
            scheduling_strategy=NodeLabelSchedulingStrategy({"gpu_type": NotIn()})
        )

    with pytest.raises(ValueError, match="The soft parameter must be a map"):
        MyActor.options(
            scheduling_strategy=NodeLabelSchedulingStrategy(hard=None, soft=["1"])
        )

    with pytest.raises(
        ValueError, match="The map key of the hard parameter must be of type str"
    ):
        MyActor.options(scheduling_strategy=NodeLabelSchedulingStrategy({111: "1111"}))

    with pytest.raises(
        ValueError, match="must be one of the `In`, `NotIn`, `Exists` or `DoesNotExist`"
    ):
        MyActor.options(
            scheduling_strategy=NodeLabelSchedulingStrategy({"gpu_type": "1111"})
        )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
