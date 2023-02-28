import sys
import pytest
from typing import List
import time

import ray
from ray._private.test_utils import SignalActor
from ray.serve._private.application_state import ApplicationStateManager
from ray.serve._private.common import ApplicationStatus
from ray.serve._private.common import DeploymentStatus, DeploymentStatusInfo
from ray.serve.exceptions import RayServeException


class MockDeploymentStateManager:
    def __init__(self):
        self.deployment_statuses = [
            DeploymentStatusInfo("d1", DeploymentStatus.UPDATING),
            DeploymentStatusInfo("d2", DeploymentStatus.UPDATING),
        ]

    def add_deployment_status(self, status: DeploymentStatusInfo):
        assert type(status) == DeploymentStatusInfo
        self.deployment_statuses.append(status)

    def set_deployment_statuses_unhealthy(self, index: int = 0):
        self.deployment_statuses[index].status = DeploymentStatus.UNHEALTHY

    def set_deployment_statuses_healthy(self, index: int = 0):
        self.deployment_statuses[index].status = DeploymentStatus.HEALTHY

    def get_deployment_statuses(self, deployment_names: List[str]):
        return self.deployment_statuses

    def get_all_deployments(self):
        return [d.name for d in self.deployment_statuses]

    def add_deployment(self, status: DeploymentStatusInfo):
        self.deployment_statuses.append(status)

    def get_deployment(self, deployment_name: str) -> DeploymentStatusInfo:
        for deployment in self.deployment_statuses:
            if deployment.name == deployment_name:
                return deployment

    def delete_deployment(self, deployment_name: str):
        statuses = []
        for deployment in self.deployment_statuses:
            if deployment.name != deployment_name:
                statuses.append(deployment)
        self.deployment_statuses = statuses


def test_deploy_app():
    """Test DEPLOYING status"""
    app_state_manager = ApplicationStateManager(MockDeploymentStateManager())
    app_state_manager.deploy_application("test_app", {})

    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING
    assert app_status.deployment_timestamp > 0


def test_delete_app():
    """Test DELETING status"""
    app_state_manager = ApplicationStateManager(MockDeploymentStateManager())
    app_state_manager.deploy_application("test_app", {})
    app_state_manager.delete_application("test_app")
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DELETING


def test_create_app():
    """Test object ref based deploy and set DEPLOYING"""
    app_state_manager = ApplicationStateManager(MockDeploymentStateManager())
    app_state_manager.create_application_state("test_app", ray.ObjectRef.nil())
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING


def test_update_app_running():
    """Test DEPLOYING -> RUNNING"""
    app_state_manager = ApplicationStateManager(MockDeploymentStateManager())
    app_state_manager.deploy_application("test_app", {})
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING
    app_state_manager.deployment_state_manager.set_deployment_statuses_healthy(0)
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING
    app_state_manager.deployment_state_manager.set_deployment_statuses_healthy(1)
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.RUNNING

    # rerun update, application status should not make difference
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.RUNNING


def test_update_app_deploy_failed():
    """Test DEPLOYING -> DEPLOY_FAILED"""
    app_state_manager = ApplicationStateManager(MockDeploymentStateManager())
    app_state_manager.deploy_application("test_app", {})
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING
    app_state_manager.deployment_state_manager.set_deployment_statuses_unhealthy(0)
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOY_FAILED
    # rerun update, application status should not make difference
    app_state_manager.update()
    assert app_status.status == ApplicationStatus.DEPLOY_FAILED


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("fail_deploy", [False, True])
def test_config_deploy_app(fail_deploy):
    """Test config based deploy
    DEPLOYING -> RUNNING
    DEPLOYING -> DEPLOY_FAILED
    """
    signal = SignalActor.remote()

    @ray.remote
    def task():
        ray.get(signal.wait.remote())
        if fail_deploy:
            raise Exception("fail!")

    object_ref = task.remote()
    app_state_manager = ApplicationStateManager(MockDeploymentStateManager())
    app_state_manager.create_application_state("test_app", object_ref)
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING

    app_state_manager.update()
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING

    signal.send.remote()
    time.sleep(2)
    if fail_deploy:
        app_state_manager.update()
        app_status = app_state_manager.get_app_status("test_app")
        assert app_status.status == ApplicationStatus.DEPLOY_FAILED
    else:
        app_state_manager.deployment_state_manager.set_deployment_statuses_healthy(0)
        app_state_manager.deployment_state_manager.set_deployment_statuses_healthy(1)
        app_state_manager.update()
        app_status = app_state_manager.get_app_status("test_app")
        assert app_status.status == ApplicationStatus.RUNNING


def test_redeploy_same_app():
    """Test deploying the same app with different deploy_params."""

    app_state_manager = ApplicationStateManager(MockDeploymentStateManager())
    app_state_manager.deploy_application("test_app", [{"name": "d1"}, {"name": "d2"}])
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING

    # Deploy the same app with different deployments
    unused_deployments = app_state_manager.deploy_application(
        "test_app", [{"name": "d2"}, {"name": "d3"}]
    )
    assert unused_deployments == ["d1"]

    app_state_manager.deployment_state_manager.add_deployment_status(
        DeploymentStatusInfo("d3", DeploymentStatus.UPDATING)
    )
    assert app_state_manager._application_states["test_app"].deployments_to_delete == {
        "d1"
    }

    # After updating, the deployment should be deleted successfully, and
    # deployments_to_delete should be empty
    app_state_manager.deployment_state_manager.delete_deployment("d1")
    app_state_manager.update()
    assert (
        app_state_manager._application_states["test_app"].deployments_to_delete == set()
    )


def test_deploy_with_route_prefix_conflict():
    """Test that an application fails to deploy with a route prefix conflict."""
    app_state_manager = ApplicationStateManager(MockDeploymentStateManager())

    app_state_manager.deploy_application(
        "test_app", [{"name": "d1", "route_prefix": "/url1"}]
    )
    with pytest.raises(RayServeException):
        app_state_manager.deploy_application(
            "test_app1", [{"name": "d1", "route_prefix": "/url1"}]
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
