import os
import sys
import time
from unittest.mock import patch

import pytest

from ray._private.accelerators import (
    MLUAcceleratorManager,
    get_accelerator_manager_for_resource,
)
from ray._private.accelerators.mlu import (
    CN_VISIBLE_DEVICES_ENV_VAR,
    NOSET_CN_VISIBLE_DEVICES_ENV_VAR,
)


@patch("glob.glob", return_value=[f"/dev/cambricon_dev{i}" for i in range(4)])
def test_autodetect_num_mlus_from_device_files(mock_glob):
    assert MLUAcceleratorManager.get_current_node_num_accelerators() == 4
    mock_glob.assert_called_once_with("/dev/cambricon_dev[0-9]*")


@patch("glob.glob", side_effect=OSError("cannot inspect /dev"))
def test_autodetect_num_mlus_without_device_files(mock_glob):
    assert MLUAcceleratorManager.get_current_node_num_accelerators() == 0
    mock_glob.assert_called_once_with("/dev/cambricon_dev[0-9]*")


def test_mlu_accelerator_manager_api():
    assert MLUAcceleratorManager.get_resource_name() == "MLU"
    assert (
        MLUAcceleratorManager.get_visible_accelerator_ids_env_var()
        == CN_VISIBLE_DEVICES_ENV_VAR
    )
    assert MLUAcceleratorManager.validate_resource_request_quantity(0.5) == (
        True,
        None,
    )
    assert MLUAcceleratorManager.validate_resource_request_quantity(1) == (True, None)
    assert get_accelerator_manager_for_resource("MLU") is MLUAcceleratorManager
    assert get_accelerator_manager_for_resource("GPU") is not MLUAcceleratorManager


def test_get_current_node_accelerator_type():
    assert MLUAcceleratorManager.get_current_node_accelerator_type() is None


def test_get_current_process_visible_accelerator_ids(monkeypatch):
    monkeypatch.delenv(CN_VISIBLE_DEVICES_ENV_VAR, raising=False)
    assert MLUAcceleratorManager.get_current_process_visible_accelerator_ids() is None

    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "0,1,2")
    assert MLUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "1",
        "2",
    ]

    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "")
    assert MLUAcceleratorManager.get_current_process_visible_accelerator_ids() == []

    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "NoDevFiles")
    assert MLUAcceleratorManager.get_current_process_visible_accelerator_ids() == []


def test_set_current_process_visible_accelerator_ids(monkeypatch):
    monkeypatch.delenv(NOSET_CN_VISIBLE_DEVICES_ENV_VAR, raising=False)
    MLUAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "2"])
    assert os.environ[CN_VISIBLE_DEVICES_ENV_VAR] == "0,2"

    monkeypatch.setenv(NOSET_CN_VISIBLE_DEVICES_ENV_VAR, "false")
    MLUAcceleratorManager.set_current_process_visible_accelerator_ids(["1"])
    assert os.environ[CN_VISIBLE_DEVICES_ENV_VAR] == "1"

    monkeypatch.setenv(NOSET_CN_VISIBLE_DEVICES_ENV_VAR, "true")
    MLUAcceleratorManager.set_current_process_visible_accelerator_ids(["3"])
    assert os.environ[CN_VISIBLE_DEVICES_ENV_VAR] == "1"


def test_ray_registers_mlu_resources_without_type(monkeypatch, shutdown_only):
    monkeypatch.delenv(CN_VISIBLE_DEVICES_ENV_VAR, raising=False)
    with patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value={"MLU"},
    ), patch.object(
        MLUAcceleratorManager,
        "get_current_node_num_accelerators",
        return_value=4,
    ):
        import ray

        ray.init(num_cpus=1, include_dashboard=False)

    resources = ray.cluster_resources()
    labels = ray.nodes()[0]["Labels"]
    assert resources["MLU"] == 4
    assert not any(name.startswith("accelerator_type:") for name in resources)
    assert "ray.io/accelerator-type" not in labels


def test_ray_limits_and_isolates_visible_mlus(monkeypatch, shutdown_only):
    import ray

    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "4,5,6")
    with patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value={"MLU"},
    ), patch.object(
        MLUAcceleratorManager,
        "get_current_node_num_accelerators",
        return_value=4,
    ):
        ray.init(num_cpus=3, include_dashboard=False)

    assert ray.cluster_resources()["MLU"] == 3

    @ray.remote(resources={"MLU": 1})
    class MLUActor:
        def assignment(self):
            return (
                ray.get_runtime_context().get_accelerator_ids()["MLU"],
                os.environ[CN_VISIBLE_DEVICES_ENV_VAR],
            )

    actors = [MLUActor.remote() for _ in range(3)]
    assignments = ray.get([actor.assignment.remote() for actor in actors])
    accelerator_ids = [ids[0] for ids, _ in assignments]
    visible_devices = [visible for _, visible in assignments]
    assert sorted(accelerator_ids) == ["4", "5", "6"]
    assert sorted(visible_devices) == ["4", "5", "6"]


def test_ray_respects_noset_mlu_visible_devices(monkeypatch, shutdown_only):
    import ray

    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "4,5")
    monkeypatch.setenv(NOSET_CN_VISIBLE_DEVICES_ENV_VAR, "true")
    with patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value={"MLU"},
    ), patch.object(
        MLUAcceleratorManager,
        "get_current_node_num_accelerators",
        return_value=2,
    ):
        ray.init(num_cpus=1, include_dashboard=False)

    @ray.remote(resources={"MLU": 1})
    def get_assignment():
        return (
            ray.get_runtime_context().get_accelerator_ids()["MLU"],
            os.environ[CN_VISIBLE_DEVICES_ENV_VAR],
        )

    accelerator_ids, visible_devices = ray.get(get_assignment.remote())
    assert accelerator_ids in (["4"], ["5"])
    assert visible_devices == "4,5"


def test_ray_mlu_task_ids_reuse_and_actor_environment(monkeypatch, shutdown_only):
    import ray

    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "4,5,6")
    monkeypatch.delenv(NOSET_CN_VISIBLE_DEVICES_ENV_VAR, raising=False)
    with patch.object(
        MLUAcceleratorManager,
        "get_current_node_num_accelerators",
        return_value=3,
    ):
        ray.init(num_cpus=3, include_dashboard=False)

    @ray.remote(num_cpus=1)
    def task(num_mlus):
        time.sleep(0.1)
        ids = ray.get_runtime_context().get_accelerator_ids()["MLU"]
        return ids, os.environ[CN_VISIBLE_DEVICES_ENV_VAR]

    no_mlu_ids, no_mlu_visible = ray.get(task.options(resources={"MLU": 0}).remote(0))
    assert no_mlu_ids == []
    assert no_mlu_visible == "4,5,6"

    two_mlu_ids, two_mlu_visible = ray.get(task.options(resources={"MLU": 2}).remote(2))
    assert len(two_mlu_ids) == 2
    assert sorted(two_mlu_ids) == sorted(two_mlu_visible.split(","))
    assert set(two_mlu_ids).issubset({"4", "5", "6"})

    # Run two waves to verify that completed tasks return their MLU instances.
    for _ in range(2):
        assignments = ray.get(
            [task.options(resources={"MLU": 1}).remote(1) for _ in range(3)]
        )
        ids = [task_ids[0] for task_ids, _ in assignments]
        visible = [task_visible for _, task_visible in assignments]
        assert sorted(ids) == ["4", "5", "6"]
        assert sorted(visible) == ["4", "5", "6"]

    @ray.remote(resources={"MLU": 1})
    class MutableEnvironmentActor:
        def assignment(self):
            return (
                ray.get_runtime_context().get_accelerator_ids()["MLU"],
                os.environ[CN_VISIBLE_DEVICES_ENV_VAR],
            )

        def replace_visible_devices(self):
            os.environ[CN_VISIBLE_DEVICES_ENV_VAR] = "user-controlled"

        def visible_devices(self):
            return os.environ[CN_VISIBLE_DEVICES_ENV_VAR]

    actor = MutableEnvironmentActor.remote()
    actor_ids, actor_visible = ray.get(actor.assignment.remote())
    assert actor_ids == [actor_visible]
    ray.get(actor.replace_visible_devices.remote())
    assert ray.get(actor.visible_devices.remote()) == "user-controlled"


def test_ray_fractional_mlu_assignments(monkeypatch, shutdown_only):
    import ray

    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "4,5,6")
    with patch.object(
        MLUAcceleratorManager,
        "get_current_node_num_accelerators",
        return_value=3,
    ):
        ray.init(num_cpus=6, include_dashboard=False)

    @ray.remote(num_cpus=1, resources={"MLU": 0.5})
    class FractionalMLUActor:
        def assignment(self):
            ids = ray.get_runtime_context().get_accelerator_ids()["MLU"]
            return ids, os.environ[CN_VISIBLE_DEVICES_ENV_VAR]

    actors = [FractionalMLUActor.remote() for _ in range(6)]
    assignments = ray.get([actor.assignment.remote() for actor in actors])
    ids = [actor_ids[0] for actor_ids, _ in assignments]
    assert all(len(actor_ids) == 1 for actor_ids, _ in assignments)
    assert all(actor_ids[0] == visible for actor_ids, visible in assignments)
    assert sorted(ids) == ["4", "4", "5", "5", "6", "6"]


def test_ray_rejects_fractional_mlu_quantity_above_one(shutdown_only):
    import ray

    @ray.remote(resources={"MLU": 1.5})
    def invalid_mlu_task():
        pass

    with pytest.raises(ValueError):
        invalid_mlu_task.remote()


def test_ray_mlu_placement_group_assignments(monkeypatch, shutdown_only):
    import ray
    from ray.util.placement_group import placement_group
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "4,5,6,7")
    with patch.object(
        MLUAcceleratorManager,
        "get_current_node_num_accelerators",
        return_value=4,
    ):
        ray.init(num_cpus=4, include_dashboard=False)

    pg = placement_group([{"CPU": 1, "MLU": 1} for _ in range(4)])
    ray.get(pg.ready())

    @ray.remote(num_cpus=1, resources={"MLU": 1})
    class MLUActor:
        def assignment(self):
            return (
                ray.get_runtime_context().get_accelerator_ids()["MLU"],
                os.environ[CN_VISIBLE_DEVICES_ENV_VAR],
            )

    actors = [
        MLUActor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg,
                placement_group_bundle_index=bundle_index,
            )
        ).remote()
        for bundle_index in [0, 3, 2, 1]
    ]
    assignments = ray.get([actor.assignment.remote() for actor in actors])
    ids = [actor_ids[0] for actor_ids, _ in assignments]
    assert all(actor_ids == [visible] for actor_ids, visible in assignments)
    assert sorted(ids) == ["4", "5", "6", "7"]


@pytest.mark.parametrize(
    ("override_on_zero", "expected_visible"),
    [(None, "4,5"), ("1", "")],
)
def test_ray_mlu_zero_resource_environment(
    override_on_zero, expected_visible, monkeypatch, shutdown_only
):
    import ray
    from ray._private.accelerators import RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO_ENV_VAR

    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "4,5")
    monkeypatch.delenv(NOSET_CN_VISIBLE_DEVICES_ENV_VAR, raising=False)
    if override_on_zero is None:
        monkeypatch.delenv(
            RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO_ENV_VAR,
            raising=False,
        )
    else:
        monkeypatch.setenv(
            RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO_ENV_VAR,
            override_on_zero,
        )

    with patch.object(
        MLUAcceleratorManager,
        "get_current_node_num_accelerators",
        return_value=2,
    ):
        ray.init(num_cpus=2, include_dashboard=False)

    @ray.remote(num_cpus=1, resources={"MLU": 0})
    def task_visible_devices():
        return (
            ray.get_runtime_context().get_accelerator_ids()["MLU"],
            os.environ.get(CN_VISIBLE_DEVICES_ENV_VAR),
        )

    @ray.remote(num_cpus=1, resources={"MLU": 0})
    class Actor:
        def visible_devices(self):
            return (
                ray.get_runtime_context().get_accelerator_ids()["MLU"],
                os.environ.get(CN_VISIBLE_DEVICES_ENV_VAR),
            )

    assert ray.get(task_visible_devices.remote()) == ([], expected_visible)
    assert ray.get(Actor.remote().visible_devices.remote()) == ([], expected_visible)


def test_ray_mlu_assignments_across_nodes(monkeypatch, ray_start_cluster):
    import ray

    cluster = ray_start_cluster
    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "4")
    cluster.add_node(num_cpus=1, resources={"MLU": 1, "mlu_node_a": 1})
    monkeypatch.setenv(CN_VISIBLE_DEVICES_ENV_VAR, "5")
    cluster.add_node(num_cpus=1, resources={"MLU": 1, "mlu_node_b": 1})
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=1, resources={"MLU": 1})
    def assignment():
        return (
            ray.get_runtime_context().get_accelerator_ids()["MLU"],
            os.environ[CN_VISIBLE_DEVICES_ENV_VAR],
        )

    node_a = assignment.options(resources={"MLU": 1, "mlu_node_a": 1}).remote()
    node_b = assignment.options(resources={"MLU": 1, "mlu_node_b": 1}).remote()
    assert ray.get([node_a, node_b]) == [(["4"], "4"), (["5"], "5")]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
