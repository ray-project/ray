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


class TestLiveApplicationsMatchConfig:
    def create_app_config(self, app_name: str) -> ServeApplicationSchema:
        return ServeApplicationSchema(
            name=app_name, import_path=f"fake.{app_name}", route_prefix=f"/{app_name}"
        )

    def test_config_with_matching_app_names(self):
        config = ServeDeploySchema(
            applications=[self.create_app_config(name="app1")],
        )

        app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
        }

        assert live_applications_match_config(config, app_statuses) is True

    def test_config_with_fewer_apps(self):
        config = ServeDeploySchema(
            applications=[self.create_app_config(name="app1")],
        )

        app_statuses = {
            "app1": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
            "app2": ApplicationStatusInfo(status=ApplicationStatus.RUNNING),
        }

        assert live_applications_match_config(config, app_statuses) is False

    def test_config_with_more_apps(self):
        config = ServeDeploySchema(
            applications=[
                self.create_app_config(name="app1"),
                self.create_app_config(name="app2"),
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
                self.create_app_config(name=f"app_{app_id}")
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
            applications=[
                self.create_app_config(name=f"app_{app_id}")
                for app_id in range(len(non_live_app_statuses))
            ]
            + [
                # Check that live apps are still counted.
                self.create_app_config(name="live_app")
            ],
        )

        app_statuses = {
            f"app_{app_id}": ApplicationStatusInfo(status=non_live_app_statuses[app_id])
            for app_id in range(len(non_live_app_statuses))
        } + {"live_app": ApplicationStatusInfo(status=ApplicationStatus.RUNNING)}

        assert live_applications_match_config(config, app_statuses) is True


class TestCalculateScaleDirection:

    def test_scale_up_numeric(self):
        ...
    
    def test_scale_down_numeric(self):
        ...
    
    def test_no_change_target_capacity(self):

        # Case 1: both target_capacities are values.

        # Case 2: both target_capacities are None.

        ...
    
    def test_enter_null_target_capacity(self):
        """When target capacity becomes null, scale up/down behavior must stop."""

        ...
    
    def test_exit_null_target_capacity(self):
        """When target capacity goes null -> non-null, scale down must start."""

        ...
    
    def test_scale_up_first_config(self):
        ...
    
    def test_config_live_apps_mismatch(self):

        # Case 1: target_capacity is set. Serve should transition to scaling up.

        # Case 2: target_capacity is not set. Serve should not be scaling.

        ...


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
