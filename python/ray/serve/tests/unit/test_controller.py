import pytest

from ray.serve._private.common import ApplicationStatus, ApplicationStatusInfo
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

    ...


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
