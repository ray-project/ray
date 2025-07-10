import sys
import warnings
import os

import pytest

import ray
from ray._private.utils import get_ray_doc_version
from ray._private.test_utils import placement_group_assert_no_leak
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.placement_group import (
    validate_placement_group,
    _validate_bundles,
    _validate_bundle_label_selector,
    VALID_PLACEMENT_GROUP_STRATEGIES,
)


def are_pairwise_unique(g):
    s = set()
    for x in g:
        if x in s:
            return False
        s.add(x)
    return True


def test_placement_ready(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def v(self):
            return 10

    # kBundle_ResourceLabel is placement group reserved resources and
    # can't be used in bundles
    with pytest.raises(Exception):
        ray.util.placement_group(bundles=[{"bundle": 1}])
    # This test is to test the case that even there all resource in the
    # bundle got allocated, we are still able to return from ready[I
    # since ready use 0 CPU
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    ray.get(pg.ready())
    a = Actor.options(
        num_cpus=1,
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
    ).remote()
    ray.get(a.v.remote())
    ray.get(pg.ready())

    with pytest.raises(ValueError):
        a = Actor.options(
            resources={"bundle": 1},
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
        ).remote()
        ray.get(a.v.remote())

    placement_group_assert_no_leak([pg])


@pytest.mark.skipif(
    ray._private.client_mode_hook.is_client_mode_enabled, reason="Fails w/ Ray Client."
)
def test_placement_group_invalid_resource_request(shutdown_only):
    """
    Make sure exceptions are raised if
    requested resources don't fit any bundles.
    """
    ray.init(resources={"a": 1})
    pg = ray.util.placement_group(bundles=[{"a": 1}])

    #
    # Test an actor with 0 cpu.
    #
    @ray.remote
    class A:
        def ready(self):
            pass

    # The actor cannot be scheduled with the default because
    # it requires 1 cpu for the placement, but the pg doesn't have it.
    with pytest.raises(ValueError):
        a = A.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    # Shouldn't work with 1 CPU because pg doesn't contain CPUs.
    with pytest.raises(ValueError):
        a = A.options(
            num_cpus=1,
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
        ).remote()
    # 0 CPU should work.
    a = A.options(
        num_cpus=0,
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
    ).remote()
    ray.get(a.ready.remote())
    del a

    #
    # Test an actor with non-0 resources.
    #
    @ray.remote(resources={"a": 1})
    class B:
        def ready(self):
            pass

    # When resources are given to the placement group,
    # it automatically adds 1 CPU to resources, so it should fail.
    with pytest.raises(ValueError):
        b = B.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    # If 0 cpu is given, it should work.
    b = B.options(
        num_cpus=0,
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
    ).remote()
    ray.get(b.ready.remote())
    del b
    # If resources are requested too much, it shouldn't work.
    with pytest.raises(ValueError):
        # The actor cannot be scheduled with no resource specified.
        # Note that the default actor has 0 cpu.
        B.options(
            num_cpus=0,
            resources={"a": 2},
            schduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
        ).remote()

    #
    # Test a function with 1 CPU.
    #
    @ray.remote
    def f():
        pass

    # 1 CPU shouldn't work because the pg doesn't have CPU bundles.
    with pytest.raises(ValueError):
        f.options(
            schduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    # 0 CPU should work.
    ray.get(
        f.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
            num_cpus=0,
        ).remote()
    )

    #
    # Test a function with 0 CPU.
    #
    @ray.remote(num_cpus=0)
    def g():
        pass

    # 0 CPU should work.
    ray.get(
        g.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    )

    placement_group_assert_no_leak([pg])


@pytest.mark.parametrize("gcs_actor_scheduling_enabled", [False, True])
def test_placement_group_pack(ray_start_cluster, gcs_actor_scheduling_enabled):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=4,
            _system_config={
                "gcs_actor_scheduling_enabled": gcs_actor_scheduling_enabled
            }
            if i == 0
            else {},
        )
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="PACK",
        bundles=[
            {"CPU": 2, "GPU": 0},  # Test 0 resource spec doesn't break tests.
            {"CPU": 2},
        ],
    )
    ray.get(placement_group.ready())
    actor_1 = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=placement_group, placement_group_bundle_index=0
        )
    ).remote()
    actor_2 = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=placement_group, placement_group_bundle_index=1
        )
    ).remote()

    ray.get(actor_1.value.remote())
    ray.get(actor_2.value.remote())

    # Get all actors.
    actor_infos = ray._private.state.actors()

    # Make sure all actors in counter_list are collocated in one node.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 == node_of_actor_2
    placement_group_assert_no_leak([placement_group])


def test_placement_group_strict_pack(ray_start_cluster):
    @ray.remote(num_cpus=2)
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_PACK",
        bundles=[
            {
                "memory": 50
                * 1024
                * 1024,  # Test memory resource spec doesn't break tests.
                "CPU": 2,
            },
            {"CPU": 2},
        ],
    )
    ray.get(placement_group.ready())
    actor_1 = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=placement_group, placement_group_bundle_index=0
        )
    ).remote()
    actor_2 = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=placement_group, placement_group_bundle_index=1
        )
    ).remote()

    ray.get(actor_1.value.remote())
    ray.get(actor_2.value.remote())

    # Get all actors.
    actor_infos = ray._private.state.actors()

    # Make sure all actors in counter_list are collocated in one node.
    actor_info_1 = actor_infos.get(actor_1._actor_id.hex())
    actor_info_2 = actor_infos.get(actor_2._actor_id.hex())

    assert actor_info_1 and actor_info_2

    node_of_actor_1 = actor_info_1["Address"]["NodeID"]
    node_of_actor_2 = actor_info_2["Address"]["NodeID"]
    assert node_of_actor_1 == node_of_actor_2

    placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("gcs_actor_scheduling_enabled", [False, True])
def test_placement_group_spread(ray_start_cluster, gcs_actor_scheduling_enabled):
    @ray.remote
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 2
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=4,
            _system_config={
                "gcs_actor_scheduling_enabled": gcs_actor_scheduling_enabled
            }
            if i == 0
            else {},
        )
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_SPREAD",
        bundles=[{"CPU": 2}, {"CPU": 2}],
    )
    ray.get(placement_group.ready())
    actors = [
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=placement_group, placement_group_bundle_index=i
            ),
            num_cpus=2,
        ).remote()
        for i in range(num_nodes)
    ]

    [ray.get(actor.value.remote()) for actor in actors]

    # Get all actors.
    actor_infos = ray._private.state.actors()

    # Make sure all actors in counter_list are located in separate nodes.
    actor_info_objs = [actor_infos.get(actor._actor_id.hex()) for actor in actors]
    assert are_pairwise_unique(
        [info_obj["Address"]["NodeID"] for info_obj in actor_info_objs]
    )

    placement_group_assert_no_leak([placement_group])


@pytest.mark.parametrize("gcs_actor_scheduling_enabled", [False, True])
def test_placement_group_strict_spread(ray_start_cluster, gcs_actor_scheduling_enabled):
    @ray.remote
    class Actor(object):
        def __init__(self):
            self.n = 0

        def value(self):
            return self.n

    cluster = ray_start_cluster
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=4,
            _system_config={
                "gcs_actor_scheduling_enabled": gcs_actor_scheduling_enabled
            }
            if i == 0
            else {},
        )
    ray.init(address=cluster.address)

    placement_group = ray.util.placement_group(
        name="name",
        strategy="STRICT_SPREAD",
        bundles=[{"CPU": 2}, {"CPU": 2}, {"CPU": 2}],
    )
    ray.get(placement_group.ready())
    actors = [
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=placement_group, placement_group_bundle_index=i
            ),
            num_cpus=1,
        ).remote()
        for i in range(num_nodes)
    ]

    [ray.get(actor.value.remote()) for actor in actors]

    # Get all actors.
    actor_infos = ray._private.state.actors()

    # Make sure all actors in counter_list are located in separate nodes.
    actor_info_objs = [actor_infos.get(actor._actor_id.hex()) for actor in actors]
    assert are_pairwise_unique(
        [info_obj["Address"]["NodeID"] for info_obj in actor_info_objs]
    )

    actors_no_special_bundle = [
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=placement_group
            ),
            num_cpus=1,
        ).remote()
        for _ in range(num_nodes)
    ]
    [ray.get(actor.value.remote()) for actor in actors_no_special_bundle]

    actor_no_resource = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=placement_group
        ),
        num_cpus=2,
    ).remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(actor_no_resource.value.remote(), timeout=0.5)

    placement_group_assert_no_leak([placement_group])


def test_placement_group_actor_resource_ids(ray_start_cluster):
    @ray.remote(num_cpus=1)
    class F:
        def f(self):
            return ray.get_runtime_context().get_assigned_resources()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    g1 = ray.util.placement_group([{"CPU": 2}])
    a1 = F.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=g1)
    ).remote()
    resources = ray.get(a1.f.remote())
    assert resources == {"CPU": 1}
    placement_group_assert_no_leak([g1])


def test_placement_group_task_resource_ids(ray_start_cluster):
    @ray.remote(num_cpus=1)
    def f():
        return ray.get_runtime_context().get_assigned_resources()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    g1 = ray.util.placement_group([{"CPU": 2}])
    o1 = f.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=g1)
    ).remote()
    resources = ray.get(o1)
    assert resources == {"CPU": 1}

    # Now retry with a bundle index constraint.
    o1 = f.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=g1, placement_group_bundle_index=0
        )
    ).remote()
    resources = ray.get(o1)
    assert resources == {"CPU": 1}

    placement_group_assert_no_leak([g1])


def test_placement_group_hang(ray_start_cluster):
    @ray.remote(num_cpus=1)
    def f():
        return ray.get_runtime_context().get_assigned_resources()

    cluster = ray_start_cluster
    num_nodes = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    # Warm workers up, so that this triggers the hang rice.
    ray.get(f.remote())

    g1 = ray.util.placement_group([{"CPU": 2}])
    # This will start out infeasible. The placement group will then be
    # created and it transitions to feasible.
    o1 = f.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=g1)
    ).remote()

    resources = ray.get(o1)
    assert resources == {"CPU": 1}

    placement_group_assert_no_leak([g1])


def test_placement_group_empty_bundle_error(ray_start_regular):
    with pytest.raises(ValueError):
        ray.util.placement_group([])


def test_placement_group_equal_hash(ray_start_regular):
    from copy import copy

    pg1 = ray.util.placement_group([{"CPU": 1}])
    pg2 = copy(pg1)

    # __eq__
    assert pg1 == pg2

    # __hash__
    s = set()
    s.add(pg1)
    assert pg2 in s

    # Compare in remote task
    @ray.remote(num_cpus=0)
    def same(a, b):
        return a == b and b in {a}

    assert ray.get(same.remote(pg1, pg2))

    # Compare before/after object store
    assert ray.get(ray.put(pg1)) == pg1


@pytest.mark.filterwarnings("default:placement_group parameter is deprecated")
def test_placement_group_scheduling_warning(ray_start_regular):
    @ray.remote
    class Foo:
        def foo():
            pass

    pg = ray.util.placement_group(
        name="bar",
        strategy="PACK",
        bundles=[
            {"CPU": 1, "GPU": 0},
        ],
    )
    ray.get(pg.ready())

    # Warning on using deprecated parameters.
    with warnings.catch_warnings(record=True) as w:
        Foo.options(placement_group=pg, placement_group_bundle_index=0).remote()
    assert any(
        "placement_group parameter is deprecated" in str(warning.message)
        for warning in w
    )
    assert any(
        f"docs.ray.io/en/{get_ray_doc_version()}" in str(warning.message)
        for warning in w
    )

    # Pointing to the same doc version as ray.__version__.
    ray.__version__ = "1.13.0"
    with warnings.catch_warnings(record=True) as w:
        Foo.options(placement_group=pg, placement_group_bundle_index=0).remote()
    assert any(
        "docs.ray.io/en/releases-1.13.0" in str(warning.message) for warning in w
    )

    # No warning when scheduling_strategy is specified.
    with warnings.catch_warnings(record=True) as w:
        Foo.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=0
            ),
        ).remote()
    assert not w


@pytest.mark.skipif(
    ray._private.client_mode_hook.is_client_mode_enabled, reason="Fails w/ Ray Client."
)
@pytest.mark.filterwarnings(
    "default:Setting 'object_store_memory' for actors is deprecated"
)
@pytest.mark.filterwarnings(
    "default:Setting 'object_store_memory' for bundles is deprecated"
)
def test_object_store_memory_deprecation_warning(ray_start_regular):
    with warnings.catch_warnings(record=True) as w:

        @ray.remote(object_store_memory=1)
        class Actor:
            pass

        Actor.remote()
    assert any(
        "Setting 'object_store_memory' for actors is deprecated" in str(warning.message)
        for warning in w
    )

    with warnings.catch_warnings(record=True) as w:
        ray.util.placement_group([{"object_store_memory": 1}], strategy="STRICT_PACK")
    assert any(
        "Setting 'object_store_memory' for bundles is deprecated"
        in str(warning.message)
        for warning in w
    )


def test_get_assigned_resources_in_pg(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=3)
    ray.init(address=cluster.address)

    @ray.remote
    def get_assigned_resources():
        return ray.get_runtime_context().get_assigned_resources()

    resources = ray.get(get_assigned_resources.options(num_cpus=1).remote())
    assert resources == {"CPU": 1}

    pg = ray.util.placement_group(bundles=[{"CPU": 3, "memory": 500}])
    ray.get(pg.ready())

    resources = ray.get(
        get_assigned_resources.options(
            num_cpus=1,
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
        ).remote()
    )
    assert resources == {"CPU": 1}

    resources = ray.get(
        get_assigned_resources.options(
            num_cpus=1,
            memory=100,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=0
            ),
        ).remote()
    )
    assert resources == {"CPU": 1, "memory": 100}


def test_omp_num_threads_in_pg(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=3)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=3)
    def test_omp_num_threads():
        omp_threads = os.environ["OMP_NUM_THREADS"]
        return int(omp_threads)

    assert ray.get(test_omp_num_threads.remote()) == 3

    pg = ray.util.placement_group(bundles=[{"CPU": 3}])
    ray.get(pg.ready())

    ref = test_omp_num_threads.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
    ).remote()
    assert ray.get(ref) == 3

    ref = test_omp_num_threads.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=0
        )
    ).remote()
    assert ray.get(ref) == 3


class TestPlacementGroupValidation:
    def test_strategy_validation(self):
        """Test strategy validation when creating a placement group."""

        # Valid strategies should not raise an exception.
        for strategy in VALID_PLACEMENT_GROUP_STRATEGIES:
            validate_placement_group(bundles=[{"CPU": 1}], strategy=strategy)

        # Any other strategy should raise a ValueError.
        with pytest.raises(ValueError, match="Invalid placement group strategy"):
            validate_placement_group(bundles=[{"CPU": 1}], strategy="invalid")

    def test_bundle_validation(self):
        """Test _validate_bundle()."""

        # Valid bundles should not raise an exception.
        valid_bundles = [{"CPU": 1, "custom-resource": 2.2}, {"GPU": 0.75}]
        _validate_bundles(valid_bundles)

        # Non-list bundles should raise an exception.
        with pytest.raises(ValueError, match="must be a list"):
            _validate_bundles("not a list")

        # Empty list bundles should raise an exception.
        with pytest.raises(ValueError, match="must be a non-empty list"):
            _validate_bundles([])

        # List that doesn't contain dictionaries should raise an exception.
        with pytest.raises(ValueError, match="resource dictionaries"):
            _validate_bundles([{"CPU": 1}, "not a dict"])

        # List with invalid dictionary entries should raise an exception.
        with pytest.raises(ValueError, match="resource dictionaries"):
            _validate_bundles([{8: 7}, {5: 3.5}])
        with pytest.raises(ValueError, match="resource dictionaries"):
            _validate_bundles([{"CPU": "6"}, {"GPU": "5"}])

        # Bundles with resources that all have 0 values should raise an exception.
        with pytest.raises(ValueError, match="only 0 values"):
            _validate_bundles([{"CPU": 0, "GPU": 0}])

    def test_bundle_label_selector_validation(self):
        """Test _validate_bundle_label_selector()."""

        # Valid label selector list should not raise an exception.
        valid_label_selectors = [
            {"ray.io/market_type": "spot"},
            {"ray.io/accelerator-type": "A100"},
        ]
        _validate_bundle_label_selector(valid_label_selectors)

        # Non-list input should raise an exception.
        with pytest.raises(ValueError, match="must be a list"):
            _validate_bundle_label_selector("not a list")

        # Empty list should not raise (interpreted as no-op).
        _validate_bundle_label_selector([])

        # List with non-dictionary elements should raise an exception.
        with pytest.raises(ValueError, match="must be a list of string dictionary"):
            _validate_bundle_label_selector(["not a dict", {"valid": "label"}])

        # Dictionary with non-string keys or values should raise an exception.
        with pytest.raises(ValueError, match="must be a list of string dictionary"):
            _validate_bundle_label_selector([{1: "value"}, {"key": "val"}])
        with pytest.raises(ValueError, match="must be a list of string dictionary"):
            _validate_bundle_label_selector([{"key": 123}, {"valid": "label"}])

        # Invalid label key or value syntax (delegated to validate_label_selector).
        with pytest.raises(ValueError, match="Invalid label selector provided"):
            _validate_bundle_label_selector([{"INVALID key!": "value"}])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
