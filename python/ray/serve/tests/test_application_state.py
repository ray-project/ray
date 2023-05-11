import sys
import pytest
from typing import List, Tuple, Dict
import time

import ray
from ray._private.test_utils import SignalActor
from ray.serve._private.application_state import ApplicationStateManager
from ray.serve._private.common import ApplicationStatus, DeploymentInfo
from ray.serve._private.common import DeploymentStatus, DeploymentStatusInfo
from ray.serve.config import DeploymentConfig, ReplicaConfig
from ray.serve.exceptions import RayServeException


class MockDeploymentStateManager:
    def __init__(self):
        self.deployment_statuses: Dict[str, DeploymentStatusInfo] = dict()

    def deploy(self, deployment_name: str, deployment_info: DeploymentInfo):
        self.deployment_statuses[deployment_name] = DeploymentStatusInfo(
            name=deployment_name,
            status=DeploymentStatus.UPDATING,
            message="",
        )

    @property
    def deployments(self) -> List[str]:
        return list(self.deployment_statuses.keys())

    def set_deployment_statuses_unhealthy(self, name: str):
        self.deployment_statuses[name].status = DeploymentStatus.UNHEALTHY

    def set_deployment_statuses_healthy(self, name: str):
        self.deployment_statuses[name].status = DeploymentStatus.HEALTHY

    def get_deployment_statuses(self, deployment_names: List[str]):
        return list(self.deployment_statuses.values())

    def get_deployment(self, deployment_name: str) -> DeploymentInfo:
        if deployment_name in self.deployment_statuses:
            # Return dummy deployment info object
            return DeploymentInfo(
                deployment_config=DeploymentConfig(num_replicas=1, user_config={}),
                replica_config=ReplicaConfig.create(lambda x: x),
                start_time_ms=0,
                deployer_job_id="",
            )

    def delete_deployment(self, deployment_name: str):
        del self.deployment_statuses[deployment_name]


@pytest.fixture
def mocked_application_state_manager() -> Tuple[
    ApplicationStateManager, MockDeploymentStateManager
]:
    deployment_state_manager = MockDeploymentStateManager()
    application_state_manager = ApplicationStateManager(deployment_state_manager)
    yield application_state_manager, deployment_state_manager


def test_deploy_app(mocked_application_state_manager):
    """Test DEPLOYING status"""
    app_state_manager, _ = mocked_application_state_manager
    app_state_manager.deploy_application("test_app", [{"name": "d1"}])

    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING
    assert app_status.deployment_timestamp > 0


def test_delete_app(mocked_application_state_manager):
    """Test DELETING status"""
    app_state_manager, _ = mocked_application_state_manager
    app_state_manager.deploy_application("test_app", [{"name": "d1"}])
    app_state_manager.delete_application("test_app")
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DELETING


def test_create_app(mocked_application_state_manager):
    """Test object ref based deploy and set DEPLOYING"""
    app_state_manager, _ = mocked_application_state_manager
    app_state_manager.create_application_state("test_app", ray.ObjectRef.nil())
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING


def test_update_app_running(mocked_application_state_manager):
    """Test DEPLOYING -> RUNNING"""
    app_state_manager, deployment_state_manager = mocked_application_state_manager
    app_state_manager.deploy_application(
        "test_app",
        [{"name": "d1"}, {"name": "d2"}],
    )
    # Simulate controller
    deployment_state_manager.deploy("d1", None)
    deployment_state_manager.deploy("d2", None)

    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING
    deployment_state_manager.set_deployment_statuses_healthy("d1")
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING
    deployment_state_manager.set_deployment_statuses_healthy("d2")
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.RUNNING

    # rerun update, application status should not make difference
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.RUNNING


def test_update_app_deploy_failed(mocked_application_state_manager):
    """Test DEPLOYING -> DEPLOY_FAILED"""
    app_state_manager, deployment_state_manager = mocked_application_state_manager
    app_state_manager.deploy_application("test_app", [{"name": "d1"}])
    # Simulate controller
    deployment_state_manager.deploy("d1", None)

    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING
    deployment_state_manager.set_deployment_statuses_unhealthy("d1")
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOY_FAILED
    # rerun update, application status should not make difference
    app_state_manager.update()
    assert app_status.status == ApplicationStatus.DEPLOY_FAILED


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("fail_deploy", [False, True])
def test_config_deploy_app(mocked_application_state_manager, fail_deploy):
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
    app_state_manager, deployment_state_manager = mocked_application_state_manager
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
        # Simulate task calling deploy_application on controller
        app_state_manager.deploy_application("test_app", [{"name": "d1"}])
        deployment_state_manager.deploy("d1", None)

        deployment_state_manager.set_deployment_statuses_healthy("d1")
        app_state_manager.update()
        app_status = app_state_manager.get_app_status("test_app")
        assert app_status.status == ApplicationStatus.RUNNING


def test_redeploy_same_app(mocked_application_state_manager):
    """Test deploying the same app with different deploy_params."""

    app_state_manager, deployment_state_manager = mocked_application_state_manager
    app_state_manager.deploy_application("test_app", [{"name": "d1"}, {"name": "d2"}])
    # Simulate controller
    deployment_state_manager.deploy("d1", None)
    deployment_state_manager.deploy("d2", None)

    app_status = app_state_manager.get_app_status("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING

    # Deploy the same app with different deployments
    unused_deployments = app_state_manager.deploy_application(
        "test_app", [{"name": "d2"}, {"name": "d3"}]
    )
    assert unused_deployments == ["d1"]

    deployment_state_manager.deploy("d3", None)
    assert app_state_manager._application_states["test_app"]._deployments_to_delete == {
        "d1"
    }

    # After updating, the deployment should be deleted successfully, and
    # deployments_to_delete should be empty
    deployment_state_manager.delete_deployment("d1")
    app_state_manager.update()
    assert (
        app_state_manager._application_states["test_app"]._deployments_to_delete
        == set()
    )


def test_deploy_with_route_prefix_conflict(mocked_application_state_manager):
    """Test that an application fails to deploy with a route prefix conflict."""
    app_state_manager, _ = mocked_application_state_manager

    app_state_manager.deploy_application(
        "test_app", [{"name": "d1", "route_prefix": "/url1"}]
    )
    with pytest.raises(RayServeException):
        app_state_manager.deploy_application(
            "test_app1", [{"name": "d1", "route_prefix": "/url1"}]
        )


def test_deploy_with_renamed_app(mocked_application_state_manager):
    """
    Test that an application deploys successfully when there is a route prefix conflict
    with an old app running on the cluster.
    """
    app_state_manager, deployment_state_manager = mocked_application_state_manager

    # deploy app1
    app_state_manager.deploy_application(
        "app1", [{"name": "d1", "route_prefix": "/url1"}]
    )
    # Simulate controller
    deployment_state_manager.deploy("d1", None)

    app_status = app_state_manager.get_app_status("app1")
    assert app_status.status == ApplicationStatus.DEPLOYING

    deployment_state_manager.set_deployment_statuses_healthy("d1")
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("app1")
    assert app_status.status == ApplicationStatus.RUNNING

    # delete app1
    app_state_manager.delete_application("app1")
    app_status = app_state_manager.get_app_status("app1")
    assert app_status.status == ApplicationStatus.DELETING

    # deploy app2
    app_state_manager.deploy_application(
        "app2", [{"name": "d2", "route_prefix": "/url1"}]
    )
    # Simulate controller
    deployment_state_manager.deploy("d2", None)

    app_status = app_state_manager.get_app_status("app2")
    assert app_status.status == ApplicationStatus.DEPLOYING

    # app2 deploys before app1 finishes deleting
    deployment_state_manager.set_deployment_statuses_healthy("d2")
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("app2")
    assert app_status.status == ApplicationStatus.RUNNING

    # app1 finally finishes deleting
    deployment_state_manager.delete_deployment("d1")
    app_state_manager.update()
    app_status = app_state_manager.get_app_status("app1")
    assert app_status.status == ApplicationStatus.NOT_STARTED


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
