import sys
import pytest
from typing import List, Dict

import ray
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve._private.application_state import ApplicationStateManager

from ray.serve.exceptions import RayServeException

from ray.serve._private.common import (
    ApplicationStatus,
    DeploymentConfig,
    DeploymentStatus,
    DeploymentStatusInfo,
    ReplicaConfig,
    DeploymentInfo,
)

@pytest.fixture
def mock_application_state_manager() -> ApplicationStateManager:
    class MockDeploymentStateManager:
        def __init__(self):
            self._deployment_states: Dict[str, DeploymentStatusInfo] = {}

        def get_deployment_statuses(self, names: List[str] = None):
            return list(self._deployment_states.values())

        def deploy(
            self,
            deployment_name: str,
            deployment_info: DeploymentInfo,
        ):
            self._deployment_states[deployment_name] = DeploymentStatusInfo(
                name=deployment_name,
                status=DeploymentStatus.UPDATING,
                message="",
            )

        def delete_deployment(self, deployment_name: str):
            pass

        def set_deployment_unhealthy(self, name: str):
            self._deployment_states[name].status = DeploymentStatus.UNHEALTHY

        def set_deployment_healthy(self, name: str):
            self._deployment_states[name].status = DeploymentStatus.HEALTHY
            
        def set_deployment_deleted(self, name: str):
            del self._deployment_states[name]

        def get_deployment(self, deployment_name: str) -> DeploymentInfo:
            if deployment_name in self._deployment_states:
                # Return dummy deployment info object
                return DeploymentInfo(
                    deployment_config=DeploymentConfig(num_replicas=1, user_config={}),
                    replica_config=ReplicaConfig.create(lambda x: x),
                    start_time_ms=0,
                    deployer_job_id="",
                )

    class MockEndpointState:
        def update_endpoint(self, endpoint, endpoint_info):
            pass

        def delete_endpoint(self, endpoint):
            pass

    mock_deployment_state_manager = MockDeploymentStateManager()
    application_state_manager = ApplicationStateManager(
        mock_deployment_state_manager, MockEndpointState()
    )

    yield application_state_manager, mock_deployment_state_manager


def deployment_params(
    name: str,
    route_prefix: str = None,
):
    return {
        "name": name,
        "deployment_config_proto_bytes": DeploymentConfig(
            num_replicas=1, user_config={}
        ).to_proto_bytes(),
        "replica_config_proto_bytes": ReplicaConfig.create(
            lambda x: x
        ).to_proto_bytes(),
        "deployer_job_id": "random",
        "route_prefix": route_prefix,
        "docs_path": None,
        "is_driver_deployment": False,
    }


def test_deploy_app(mock_application_state_manager):
    """Test DEPLOYING status"""
    app_state_manager, _ = mock_application_state_manager
    app_state_manager.deploy_application("test_app", {})

    app_status = app_state_manager.get_app_status_info("test_app")
    assert app_status.status == ApplicationStatus.DEPLOYING
    assert app_status.deployment_timestamp > 0


def test_delete_app(mock_application_state_manager):
    """Test DELETING status"""
    app_state_manager, _ = mock_application_state_manager
    app_state_manager.deploy_application("test_app", {})
    app_state_manager.delete_application("test_app")
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.DELETING


def test_update_app_running(mock_application_state_manager):
    """Test DEPLOYING -> RUNNING"""
    app_state_manager, deployment_state_manager = mock_application_state_manager
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.NOT_STARTED

    # Deploy application with two deployments
    app_state_manager.deploy_application(
        "test_app", [deployment_params("a"), deployment_params("b")]
    )
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.DEPLOYING

    # One deployment healthy
    deployment_state_manager.set_deployment_healthy("a")
    app_state_manager.update()
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.DEPLOYING

    # Both deployments healthy
    deployment_state_manager.set_deployment_healthy("b")
    app_state_manager.update()
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.RUNNING

    # Rerun update, application status should not make difference
    app_state_manager.update()
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.RUNNING


def test_update_app_deploy_failed(
    mock_application_state_manager: ApplicationStateManager,
):
    """Test DEPLOYING -> DEPLOY_FAILED"""
    app_state_manager, deployment_state_manager = mock_application_state_manager
    app_state_manager.deploy_application("test_app", [deployment_params("a")])

    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.DEPLOYING

    # One deployment is unhealthy
    deployment_state_manager.set_deployment_unhealthy("a")
    app_state_manager.update()
    assert (
        app_state_manager.get_app_status("test_app") == ApplicationStatus.DEPLOY_FAILED
    )

    # Rerun update, application status should not make difference
    app_state_manager.update()
    assert (
        app_state_manager.get_app_status("test_app") == ApplicationStatus.DEPLOY_FAILED
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("fail_deploy", [False, True])
def test_config_deploy_app(mock_application_state_manager, fail_deploy):
    """Test config based deploy
    DEPLOYING -> RUNNING
    DEPLOYING -> DEPLOY_FAILED
    """
    app_state_manager, deployment_state_manager = mock_application_state_manager
    signal = SignalActor.remote()

    @ray.remote
    def task():
        ray.get(signal.wait.remote())
        if fail_deploy:
            raise Exception("Intentionally failed build task.")
        else:
            return [deployment_params("a"), deployment_params("b")]

    build_app_obj_ref = task.remote()

    # Create application state - status should be NOT_STARTED
    app_state_manager.create_application_state("test_app", build_app_obj_ref)
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.NOT_STARTED
    # Before object ref is read, status should still be NOT_STARTED
    app_state_manager.update()
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.NOT_STARTED

    signal.send.remote()
    # Wait for task to return
    wait_for_condition(lambda: len(ray.wait([build_app_obj_ref], timeout=0)[0]))
    if fail_deploy:
        app_state_manager.update()
        assert (
            app_state_manager.get_app_status("test_app")
            == ApplicationStatus.DEPLOY_FAILED
        )
    else:
        app_state_manager.update()
        deployment_state_manager.set_deployment_healthy("a")
        deployment_state_manager.set_deployment_healthy("b")
        app_state_manager.update()
        assert app_state_manager.get_app_status("test_app") == ApplicationStatus.RUNNING


def test_redeploy_same_app(mock_application_state_manager):
    """Test deploying the same app with different deploy_params."""
    app_state_manager, deployment_state_manager = mock_application_state_manager
    app_state_manager.deploy_application(
        "app", [deployment_params("a"), deployment_params("b")]
    )
    assert app_state_manager.get_app_status("app") == ApplicationStatus.DEPLOYING

    # Deploy the same app with different deployments
    app_state_manager.deploy_application(
        "app", [deployment_params("b"), deployment_params("c")]
    )
    assert app_state_manager._application_states["app"].deployments_to_delete == {"a"}

    # When the deployment should be deleted successfully, deployments_to_delete should
    # be empty
    deployment_state_manager.set_deployment_deleted("a")
    app_state_manager.update()
    assert app_state_manager._application_states["app"].deployments_to_delete == set()
    assert "a" not in deployment_state_manager._deployment_states


def test_deploy_with_route_prefix_conflict(mock_application_state_manager):
    """Test that an application fails to deploy with a route prefix conflict."""
    app_state_manager, deployment_state_manager = mock_application_state_manager

    app_state_manager.deploy_application("app1", [deployment_params("a", "/hi")])
    with pytest.raises(RayServeException):
        app_state_manager.deploy_application("app2", [deployment_params("b", "/hi")])


def test_deploy_with_renamed_app(mock_application_state_manager):
    """
    Test that an application deploys successfully when there is a route prefix
    conflict with an old app running on the cluster.
    """
    app_state_manager, deployment_state_manager = mock_application_state_manager

    # deploy app1
    app_state_manager.deploy_application("app1", [deployment_params("a", "/url1")])
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DEPLOYING

    # Once its single deployment is healthy, app1 should be running
    deployment_state_manager.set_deployment_healthy("a")
    app_state_manager.update()
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.RUNNING

    # delete app1
    app_state_manager.delete_application("app1")
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DELETING

    # deploy app2
    app_state_manager.deploy_application("app2", [deployment_params("b", "/url1")])
    assert app_state_manager.get_app_status("app2") == ApplicationStatus.DEPLOYING

    # app2 deploys before app1 finishes deleting
    deployment_state_manager.set_deployment_healthy("b")
    app_state_manager.update()
    assert app_state_manager.get_app_status("app2") == ApplicationStatus.RUNNING
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DELETING

    # app1 finally finishes deleting
    deployment_state_manager.set_deployment_deleted("a")
    app_state_manager.update()
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.NOT_STARTED
    assert app_state_manager.get_app_status("app2") == ApplicationStatus.RUNNING
    assert "a" not in deployment_state_manager._deployment_states


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
