from unittest.mock import MagicMock, patch

from ray.data._internal.cluster_autoscaler import default_autoscaling_coordinator
from ray.data._internal.execution import autoscaling_requester
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_head_node_placement_options():
    options = default_autoscaling_coordinator._head_node_placement_options()

    assert options == {
        "resources": {
            default_autoscaling_coordinator.HEAD_NODE_RESOURCE_LABEL: (
                default_autoscaling_coordinator.HEAD_NODE_RESOURCE_CONSTRAINT
            )
        },
        "scheduling_strategy": PlacementGroupSchedulingStrategy(placement_group=None),
    }


def test_get_or_create_autoscaling_requester_actor_pins_to_head():
    actor_handle = object()
    options_builder = MagicMock()
    options_builder.remote.return_value = actor_handle

    with patch.object(
        autoscaling_requester.AutoscalingRequester,
        "options",
        return_value=options_builder,
    ) as mock_options:
        assert (
            autoscaling_requester.get_or_create_autoscaling_requester_actor()
            is actor_handle
        )

    mock_options.assert_called_once_with(
        name="AutoscalingRequester",
        namespace="AutoscalingRequester",
        get_if_exists=True,
        lifetime="detached",
        **default_autoscaling_coordinator._head_node_placement_options(),
    )
    options_builder.remote.assert_called_once_with()


def test_get_or_create_autoscaling_coordinator_pins_to_head():
    actor_handle = object()
    actor_cls = MagicMock()
    actor_cls.options.return_value.remote.return_value = actor_handle
    remote_decorator = MagicMock(return_value=actor_cls)

    with patch.object(
        default_autoscaling_coordinator.ray,
        "remote",
        return_value=remote_decorator,
    ) as mock_remote:
        assert (
            default_autoscaling_coordinator.get_or_create_autoscaling_coordinator()
            is actor_handle
        )

    mock_remote.assert_called_once_with(
        num_cpus=0,
        max_restarts=-1,
        max_task_retries=-1,
    )
    remote_decorator.assert_called_once_with(
        default_autoscaling_coordinator._AutoscalingCoordinatorActor
    )
    actor_cls.options.assert_called_once_with(
        name="AutoscalingCoordinator",
        namespace="AutoscalingCoordinator",
        get_if_exists=True,
        lifetime="detached",
        **default_autoscaling_coordinator._head_node_placement_options(),
    )
    actor_cls.options.return_value.remote.assert_called_once_with()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
