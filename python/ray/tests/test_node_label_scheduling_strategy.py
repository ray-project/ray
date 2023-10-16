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
        self.value = 0

    def value(self):
        return self.value

    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()


@ray.remote
def get_node_id():
    return ray.get_runtime_context().get_node_id()


@pytest.mark.parametrize(
    "call_ray_start",
    ['ray start --head --labels={"gpu_type":"A100","region":"us"}'],
    indirect=True,
)
def test_node_label_scheduling_basic(call_ray_start):
    ray.init(address=call_ray_start)
    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            {"gpu_type": In("A100", "T100"), "region": Exists()}
        )
    ).remote()
    assert ray.get(actor.value.remote(), timeout=3) == 0

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy({"gpu_type": NotIn("A100")})
    ).remote()
    with pytest.raises(TimeoutError):
        assert ray.get(actor.value.remote(), timeout=3) == 0

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={"gpu_type": DoesNotExist()},
            soft={"gpu_type": In("A100")},
        )
    ).remote()
    with pytest.raises(TimeoutError):
        assert ray.get(actor.value.remote(), timeout=3) == 0

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={"gpu_type": In("T100")},
        )
    ).remote()
    with pytest.raises(TimeoutError):
        assert ray.get(actor.value.remote(), timeout=3) == 0

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={},
            soft={"gpu_type": In("T100ssss")},
        )
    ).remote()
    assert ray.get(actor.value.remote(), timeout=3) == 0


def test_node_label_scheduling_in_cluster(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"worker1": 1},
        num_cpus=3,
        labels={"gpu_type": "A100", "azone": "azone-1"},
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    node_1 = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
    cluster.add_node(
        resources={"worker2": 1},
        num_cpus=3,
        labels={"gpu_type": "T100", "azone": "azone-1"},
    )
    node_2 = ray.get(get_node_id.options(resources={"worker2": 1}).remote())
    cluster.add_node(
        resources={"worker3": 1},
        num_cpus=3,
        labels={"gpu_type": "T100", "azone": "azone-2"},
    )
    node_3 = ray.get(get_node_id.options(resources={"worker3": 1}).remote())
    cluster.add_node(resources={"worker4": 1}, num_cpus=3)
    node_4 = ray.get(get_node_id.options(resources={"worker4": 1}).remote())
    cluster.wait_for_nodes()

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy({"gpu_type": In("A100")})
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) == node_1

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy({"ray.io/node_id": In(node_4)})
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) == node_4

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy({"gpu_type": In("T100")})
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) in (node_2, node_3)

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            {"azone": In("azone-1", "azone-2")}
        )
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) in (node_1, node_2, node_3)

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            {"gpu_type": In("T100"), "azone": In("azone-1")}
        )
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) in (node_2)

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            {
                "gpu_type": NotIn("A100"),
            }
        )
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) in (node_2, node_3, node_4)

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            {
                "gpu_type": DoesNotExist(),
            }
        )
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) in (node_4)

    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            {
                "gpu_type": Exists(),
            }
        )
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) in (node_1, node_2, node_3)


def test_node_label_scheduling_with_soft(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"worker1": 1},
        num_cpus=3,
        labels={"gpu_type": "A100", "azone": "azone-1"},
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    node_1 = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
    cluster.add_node(
        resources={"worker2": 1},
        num_cpus=3,
        labels={"gpu_type": "T100", "azone": "azone-1"},
    )
    node_2 = ray.get(get_node_id.options(resources={"worker2": 1}).remote())
    cluster.add_node(
        resources={"worker3": 1},
        num_cpus=3,
        labels={"gpu_type": "T100", "azone": "azone-2"},
    )
    node_3 = ray.get(get_node_id.options(resources={"worker3": 1}).remote())
    cluster.add_node(resources={"worker4": 1}, num_cpus=3)
    node_4 = ray.get(get_node_id.options(resources={"worker4": 1}).remote())
    cluster.wait_for_nodes()

    # hard match and soft match
    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={"azone": In("azone-1")}, soft={"gpu_type": In("T100")}
        )
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) == node_2

    # hard match and soft don't match
    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={"azone": In("azone-1")}, soft={"gpu_type": In("H100")}
        )
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) in (node_1, node_2)

    # no hard and  soft match
    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={}, soft={"gpu_type": Exists()}
        )
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) in (node_1, node_2, node_3)

    # no hard and soft don't match
    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={}, soft={"gpu_type": In("H100")}
        )
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) in (
        node_1,
        node_2,
        node_3,
        node_4,
    )

    # hard don't match and soft match
    actor = MyActor.options(
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={"azone": In("azone-3")}, soft={"gpu_type": In("T100")}
        )
    ).remote()
    with pytest.raises(TimeoutError):
        ray.get(actor.get_node_id.remote(), timeout=3)


def test_node_not_available(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"worker1": 1}, num_cpus=1, labels={"gpu_type": "A100"})
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    node_1 = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
    cluster.add_node(resources={"worker2": 1}, num_cpus=1)
    node_2 = ray.get(get_node_id.options(resources={"worker2": 1}).remote())
    cluster.wait_for_nodes()

    # Infeasible
    actor = MyActor.options(
        num_cpus=2,
        scheduling_strategy=NodeLabelSchedulingStrategy(hard={"gpu_type": In("A100")}),
    ).remote()
    with pytest.raises(TimeoutError):
        ray.get(actor.get_node_id.remote(), timeout=3)

    actor = MyActor.options(
        num_cpus=1,
        scheduling_strategy=NodeLabelSchedulingStrategy(hard={"gpu_type": In("A100")}),
    ).remote()
    assert ray.get(actor.get_node_id.remote(), timeout=3) == node_1

    # Soft match node is not available, sheduling to other available node.
    actor_2 = MyActor.options(
        num_cpus=1,
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={}, soft={"gpu_type": In("A100")}
        ),
    ).remote()
    assert ray.get(actor_2.get_node_id.remote(), timeout=3) == node_2

    # No available nodes.
    actor_3 = MyActor.options(
        num_cpus=1,
        scheduling_strategy=NodeLabelSchedulingStrategy(
            hard={}, soft={"gpu_type": In("A100")}
        ),
    ).remote()
    with pytest.raises(TimeoutError):
        ray.get(actor_3.get_node_id.remote(), timeout=3)

    # node_1 change to available
    ray.kill(actor)
    assert ray.get(actor_3.get_node_id.remote(), timeout=3) == node_1


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

    with pytest.raises(
        ValueError,
        match="The `hard` and `soft` parameter "
        "of NodeLabelSchedulingStrategy cannot both be empty.",
    ):
        MyActor.options(scheduling_strategy=NodeLabelSchedulingStrategy(hard={}))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
