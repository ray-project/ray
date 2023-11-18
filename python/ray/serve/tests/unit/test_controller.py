import pytest

from ray.serve._private.common import (
    ApplicationStatus,
    ApplicationStatusInfo,
    TargetCapacityScaleDirection,
)
from ray.serve._private.controller import (
    calculate_scale_direction,
    live_applications_match_config,
)
from ray.serve.schema import ServeApplicationSchema, ServeDeploySchema


def create_app_config(name: str) -> ServeApplicationSchema:
    return ServeApplicationSchema(
        name=name, import_path=f"fake.{name}", route_prefix=f"/{name}"
    )


class TestLiveApplicationsMatchConfig:
    def test_config_with_matching_app_names(self):
        config = ServeDeploySchema(
            applications=[create_app_config(name="app1")],
        )

        app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
        }

        assert live_applications_match_config(config, app_statuses) is True

    def test_config_with_fewer_apps(self):
        config = ServeDeploySchema(
            applications=[create_app_config(name="app1")],
        )

        app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
            "app2": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
        }

        assert live_applications_match_config(config, app_statuses) is False

    def test_config_with_more_apps(self):
        config = ServeDeploySchema(
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
            ],
        )

        app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
        }

        assert live_applications_match_config(config, app_statuses) is False

    def test_live_app_statuses(self):
        """Check that all live app statuses are actually counted as live."""

        live_app_statuses = [
            ApplicationStatus.NOT_STARTED,
            ApplicationStatus.DEPLOYING,
            ApplicationStatus.DEPLOY_FAILED,
            ApplicationStatus.RUNNING,
            ApplicationStatus.UNHEALTHY,
        ]

        config = ServeDeploySchema(
            applications=[
                create_app_config(name=f"app_{app_id}")
                for app_id in range(len(live_app_statuses))
            ],
        )

        app_statuses = {
            f"app_{app_id}": ApplicationStatusInfo(status=live_app_statuses[app_id])
            for app_id in range(len(live_app_statuses))
        }

        assert live_applications_match_config(config, app_statuses) is True

    def test_non_live_app_statuses(self):
        """Check that non-live apps are ignored."""

        non_live_app_statuses = [ApplicationStatus.DELETING]

        config = ServeDeploySchema(
            # Only live apps should be compared.
            applications=[create_app_config(name="live_app")],
        )

        app_statuses = {
            f"app_{app_id}": ApplicationStatusInfo(status=non_live_app_statuses[app_id])
            for app_id in range(len(non_live_app_statuses))
        }
        app_statuses.update(
            {"live_app": ApplicationStatusInfo(status=ApplicationStatus.RUNNING)}
        )

        assert live_applications_match_config(config, app_statuses) is True


class TestCalculateScaleDirection:
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityScaleDirection.UP, TargetCapacityScaleDirection.DOWN],
    )
    @pytest.mark.parametrize(
        "new_direction",
        [TargetCapacityScaleDirection.UP, TargetCapacityScaleDirection.DOWN],
    )
    def test_change_target_capacity_numeric(self, curr_direction, new_direction):
        curr_target_capacity = 5

        if new_direction == TargetCapacityScaleDirection.UP:
            new_target_capacity = curr_target_capacity * 3.3
        elif new_direction == TargetCapacityScaleDirection.DOWN:
            new_target_capacity = curr_target_capacity / 3.3

        app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
            "app2": ApplicationStatusInfo(status=ApplicationStatus.DEPLOYING),
        }

        new_config = ServeDeploySchema(
            target_capacity=new_target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
            ],
        )

        # The new direction must be returned, regardless of the current direction.
        assert (
            calculate_scale_direction(
                new_config,
                app_statuses,
                curr_target_capacity,
                curr_direction,
            )
            == new_direction
        )

    @pytest.mark.parametrize("target_capacity", [0, 50, 100, None])
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityScaleDirection.UP, TargetCapacityScaleDirection.DOWN, None],
    )
    def test_no_change_target_capacity(self, target_capacity, curr_direction):
        """When target_capacity doesn't change, return the current direction."""

        if target_capacity is None:
            # When target_capacity is None, the current direction must be None.
            curr_direction = None

        app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
            "app2": ApplicationStatusInfo(status=ApplicationStatus.DEPLOYING),
            "app3": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
        }

        new_config = ServeDeploySchema(
            target_capacity=target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
                create_app_config(name="app3"),
            ],
        )

        assert (
            calculate_scale_direction(
                new_config,
                app_statuses,
                target_capacity,
                curr_direction,
            )
            == curr_direction
        )

    @pytest.mark.parametrize("curr_target_capacity", [0, 50, 100, None])
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityScaleDirection.UP, TargetCapacityScaleDirection.DOWN, None],
    )
    def test_enter_null_target_capacity(self, curr_target_capacity, curr_direction):
        """When target capacity becomes null, scale up/down behavior must stop."""

        if curr_target_capacity is None:
            # When target_capacity is None, the current direction must be None.
            curr_direction = None

        app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
            "app2": ApplicationStatusInfo(status=ApplicationStatus.DEPLOYING),
            "app3": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
        }

        new_config = ServeDeploySchema(
            target_capacity=None,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
                create_app_config(name="app3"),
            ],
        )

        assert (
            calculate_scale_direction(
                new_config,
                app_statuses,
                curr_target_capacity,
                curr_direction,
            )
            is None
        )

    @pytest.mark.parametrize("new_target_capacity", [0, 50, 100])
    def test_exit_null_target_capacity(self, new_target_capacity):
        """When target capacity goes null -> non-null, scale down must start."""

        app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
        }

        new_config = ServeDeploySchema(
            target_capacity=new_target_capacity,
            applications=[create_app_config(name="app1")],
        )

        # When Serve is already running the applications at target_capacity
        # None, and then a target_capacity is applied, the direction must
        # become DOWN.
        assert (
            calculate_scale_direction(
                new_config,
                app_statuses,
                None,
                None,
            )
            == TargetCapacityScaleDirection.DOWN
        )

    def test_scale_up_first_config(self):
        """Check how Serve handles the first config that's applied."""

        # No config is running, so no application statuses exist yet.
        app_statuses = {}

        # Case 1: target_capacity is set. Serve should transition to scaling up.

        new_config = ServeDeploySchema(
            target_capacity=20,
            applications=[create_app_config(name="app1")],
        )
        assert (
            calculate_scale_direction(
                new_config,
                app_statuses,
                None,
                None,
            )
            == TargetCapacityScaleDirection.UP
        )

        # Case 2: target_capacity is not set. Serve should not be scaling.

        new_config = ServeDeploySchema(
            target_capacity=None,
            applications=[create_app_config(name="app1")],
        )
        assert (
            calculate_scale_direction(
                new_config,
                app_statuses,
                None,
                None,
            )
            is None
        )

    @pytest.mark.parametrize("prev_target_capacity", [0, 50, 100, None])
    @pytest.mark.parametrize(
        "prev_direction",
        [TargetCapacityScaleDirection.UP, TargetCapacityScaleDirection.DOWN, None],
    )
    def test_config_live_apps_mismatch(self, prev_target_capacity, prev_direction):
        """Apply a config with apps that don't match the live apps.

        Serve should treat this like applying the first config. Its scaling
        direction should not be based on the previous config's target_capacity.
        """

        if prev_target_capacity is None:
            # When target_capacity is None, the current direction must be None.
            prev_direction = None

        prev_app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
            "app2": ApplicationStatusInfo(status=ApplicationStatus.DEPLOYING),
            "app3": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
        }

        # Case 1: target_capacity is set. Serve should transition to scaling up.
        new_config = ServeDeploySchema(
            target_capacity=30,
            applications=[
                create_app_config(name="new_app1"),
                create_app_config(name="app2"),
            ],
        )

        assert (
            calculate_scale_direction(
                new_config,
                prev_app_statuses,
                prev_target_capacity,
                prev_direction,
            )
            is TargetCapacityScaleDirection.UP
        )

        # Case 2: target_capacity is not set. Serve should not be scaling.
        new_config = ServeDeploySchema(
            target_capacity=None,
            applications=[
                create_app_config(name="new_app1"),
                create_app_config(name="app2"),
            ],
        )

        assert (
            calculate_scale_direction(
                new_config,
                prev_app_statuses,
                prev_target_capacity,
                prev_direction,
            )
            is None
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
