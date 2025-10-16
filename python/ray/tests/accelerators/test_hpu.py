import os
import sys
from unittest.mock import patch

import pytest

import ray
from ray._private.accelerators import HPUAcceleratorManager, hpu
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_user_configured_more_than_visible(monkeypatch, call_ray_stop_only):
    # Test more hpus are configured than visible.
    monkeypatch.setenv("HABANA_VISIBLE_MODULES", "0,1,2")
    with pytest.raises(ValueError):
        ray.init(resources={"HPU": 4})


@patch(
    "ray._private.accelerators.HPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=4,
)
def test_auto_detected_more_than_visible(
    mock_get_num_accelerators, monkeypatch, shutdown_only
):
    # Test more hpus are detected than visible.
    monkeypatch.setenv("HABANA_VISIBLE_MODULES", "0,1,2")
    ray.init()
    _ = mock_get_num_accelerators.called
    assert ray.available_resources()["HPU"] == 3


@patch(
    "ray._private.accelerators.HPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=2,
)
def test_auto_detect_resources(mock_get_num_accelerators, shutdown_only):
    # Test that ray node resources are filled with auto detected count.
    ray.init()
    _ = mock_get_num_accelerators.called
    assert ray.available_resources()["HPU"] == 2


def test_get_current_process_visible_accelerator_ids():
    os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] = "0,1,2"
    assert HPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "1",
        "2",
    ]  # noqa: E501

    del os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR]
    assert HPUAcceleratorManager.get_current_process_visible_accelerator_ids() is None

    os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] = ""
    assert HPUAcceleratorManager.get_current_process_visible_accelerator_ids() == []

    del os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR]


def test_set_current_process_visible_accelerator_ids():
    HPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] == "0"

    HPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] == "0,1"

    HPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1", "2"])
    assert os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] == "0,1,2"

    del os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR]


@pytest.mark.parametrize(
    "test_config",
    [
        (1, False),
        (0.5, True),
        (3, False),
    ],
)
def test_validate_resource_request_quantity(test_config):
    num_hpus, expect_error = test_config

    if expect_error:
        assert (
            HPUAcceleratorManager.validate_resource_request_quantity(num_hpus)[0]
            is False
        )
        assert (
            HPUAcceleratorManager.validate_resource_request_quantity(num_hpus)[1]
            is not None
        )
    else:
        assert (
            HPUAcceleratorManager.validate_resource_request_quantity(num_hpus)[0]
            is True
        )
        assert (
            HPUAcceleratorManager.validate_resource_request_quantity(num_hpus)[1]
            is None
        )


def test_check_accelerator_info():

    if HPUAcceleratorManager.is_initialized():
        assert (
            "Intel-GAUDI" in HPUAcceleratorManager.get_current_node_accelerator_type()
        )
    else:
        assert HPUAcceleratorManager.get_current_node_accelerator_type() is None

    assert HPUAcceleratorManager.get_resource_name() == "HPU"


def test_decorator_args():

    # This is a valid way of using the decorator.
    @ray.remote(resources={"HPU": 1})  # noqa: F811
    class Actor:  # noqa: F811
        def __init__(self):
            pass

    # This is a valid way of using the decorator.
    @ray.remote(num_cpus=1, resources={"HPU": 1})  # noqa: F811
    class Actor:  # noqa: F811
        def __init__(self):
            pass


def test_actor_deletion_with_hpus(shutdown_only):
    ray.init(num_cpus=1, resources={"HPU": 1})

    # When an actor that uses an HPU exits, make sure that the HPU resources
    # are released.

    @ray.remote(resources={"HPU": 1})
    class Actor:
        def getpid(self):
            return os.getpid()

    for _ in range(5):
        # If we can successfully create an actor, that means that enough
        # HPU resources are available.
        a = Actor.remote()
        ray.get(a.getpid.remote())


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_actor_hpus(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 2
    num_hpus_per_raylet = 2
    for i in range(num_nodes):
        cluster.add_node(num_cpus=10 * 2, resources={"HPU": num_hpus_per_raylet})
    ray.init(address=cluster.address)

    @ray.remote(resources={"HPU": 1})
    class Actor1:
        def __init__(self):
            resource_ids = ray.get_runtime_context().get_accelerator_ids()
            self.hpu_ids = resource_ids.get("HPU")

        def get_location_and_ids(self):
            return (
                ray.get_runtime_context().get_node_id(),
                tuple(self.hpu_ids),
            )

    # Create one actor per HPU.
    actors = [Actor1.remote() for _ in range(num_nodes * num_hpus_per_raylet)]
    # Make sure that no two actors are assigned to the same HPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors]
    )
    node_names = {location for location, hpu_id in locations_and_ids}
    assert len(node_names) == num_nodes
    location_actor_combinations = []
    for node_name in node_names:
        for hpu_id in range(num_hpus_per_raylet):
            location_actor_combinations.append((node_name, (f"{hpu_id}",)))

    assert set(locations_and_ids) == set(location_actor_combinations)

    # Creating a new actor should fail because all of the HPUs are being
    # used.
    a = Actor1.remote()
    ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=0.01)
    assert ready_ids == []


def test_actor_habana_visible_devices(shutdown_only):
    """Test user can overwrite HABANA_VISIBLE_MODULES
    after the actor is created."""
    ray.init(resources={"HPU": 1})

    @ray.remote(resources={"HPU": 1})
    class Actor:
        def set_habana_visible_devices(self, habana_visible_devices):
            os.environ["HABANA_VISIBLE_MODULES"] = habana_visible_devices

        def get_habana_visible_devices(self):
            return os.environ["HABANA_VISIBLE_MODULES"]

    actor = Actor.remote()
    assert ray.get(actor.get_habana_visible_devices.remote()) == "0"
    ray.get(actor.set_habana_visible_devices.remote("0,1"))
    assert ray.get(actor.get_habana_visible_devices.remote()) == "0,1"


def test_hpu_ids(shutdown_only):
    num_hpus = 3
    ray.init(num_cpus=num_hpus, resources={"HPU": num_hpus})

    def get_hpu_ids(hpus_per_worker):
        hpu_ids = ray.get_runtime_context().get_accelerator_ids()["HPU"]
        assert len(hpu_ids) == hpus_per_worker
        modules = os.environ.get("HABANA_VISIBLE_MODULES")
        if modules is not None:
            assert modules == ",".join([str(i) for i in hpu_ids])  # noqa
        for hpu_id in hpu_ids:
            assert hpu_id in [str(i) for i in range(num_hpus)]
        return hpu_ids

    f0 = ray.remote(resources={"HPU": 0})(lambda: get_hpu_ids(0))
    f1 = ray.remote(resources={"HPU": 1})(lambda: get_hpu_ids(1))

    list_of_ids = ray.get([f0.remote() for _ in range(10)])
    assert list_of_ids == 10 * [[]]
    ray.get([f1.remote() for _ in range(10)])

    # Test that actors have HABANA_VISIBLE_MODULES set properly.

    @ray.remote
    class Actor:
        def __init__(self, num_hpus):
            self.num_hpus = num_hpus
            hpu_ids = ray.get_runtime_context().get_accelerator_ids()["HPU"]
            assert len(hpu_ids) == num_hpus
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]  # noqa
            )
            # Set self.x to make sure that we got here.
            self.x = num_hpus

        def test(self):
            hpu_ids = ray.get_runtime_context().get_accelerator_ids()["HPU"]
            assert len(hpu_ids) == self.num_hpus
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]  # noqa
            )
            return self.x

    a0 = Actor.remote(0)
    assert ray.get(a0.test.remote()) == 0

    a1 = Actor.options(resources={"HPU": 1}).remote(1)
    assert ray.get(a1.test.remote()) == 1


def test_hpu_with_placement_group(shutdown_only):
    num_hpus = 2
    ray.init(num_cpus=1, resources={"HPU": num_hpus})

    @ray.remote(resources={"HPU": num_hpus})
    class HPUActor:
        def __init__(self):
            pass

        def ready(self):
            hpu_ids = ray.get_runtime_context().get_accelerator_ids()["HPU"]
            assert len(hpu_ids) == num_hpus
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]  # noqa
            )

    # Reserve a placement group of 1 bundle that reserves 1 CPU and 2 HPU.
    pg = placement_group([{"CPU": 1, "HPU": num_hpus}])

    # Wait until placement group is created.
    ray.get(pg.ready(), timeout=10)

    actor = HPUActor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
        )
    ).remote()

    ray.get(actor.ready.remote(), timeout=10)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
