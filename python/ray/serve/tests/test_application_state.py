import sys
import pytest
from typing import List, Tuple, Dict

import ray
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve._private.application_state import (
    ApplicationState,
    ApplicationStateManager,
)

from ray.serve.exceptions import RayServeException

from ray.serve._private.common import (
    ApplicationStatus,
    DeploymentConfig,
    DeploymentStatus,
    DeploymentStatusInfo,
    ReplicaConfig,
    DeploymentInfo,
)


class MockEndpointState:
    def update_endpoint(self, endpoint, endpoint_info):
        pass

    def delete_endpoint(self, endpoint):
        pass


class MockKVStore:
    def get(self, *args):
        pass

    def put(self, *args):
        pass


class MockDeploymentStateManager:
    def __init__(self):
        self.deployment_infos: Dict[str, DeploymentInfo] = dict()
        self.deployment_statuses: Dict[str, DeploymentStatusInfo] = dict()

    def deploy(self, deployment_name: str, deployment_info: DeploymentInfo):
        self.deployment_infos[deployment_name] = deployment_info
        if deployment_name not in self.deployment_statuses:
            self.deployment_statuses[deployment_name] = DeploymentStatusInfo(
                name=deployment_name,
                status=DeploymentStatus.UPDATING,
                message="",
            )

    @property
    def deployments(self) -> List[str]:
        return list(self.deployment_infos.keys())

    def set_deployment_unhealthy(self, name: str):
        self.deployment_statuses[name].status = DeploymentStatus.UNHEALTHY

    def set_deployment_healthy(self, name: str):
        self.deployment_statuses[name].status = DeploymentStatus.HEALTHY

    def set_deployment_updating(self, name: str):
        self.deployment_statuses[name].status = DeploymentStatus.UPDATING

    def get_deployment_statuses(self, deployment_names: List[str]):
        return [self.deployment_statuses[name] for name in deployment_names]

    def get_deployment(self, deployment_name: str) -> DeploymentInfo:
        if deployment_name in self.deployment_statuses:
            # Return dummy deployment info object
            return DeploymentInfo(
                deployment_config=DeploymentConfig(num_replicas=1, user_config={}),
                replica_config=ReplicaConfig.create(lambda x: x),
                start_time_ms=0,
                deployer_job_id="",
            )

    def get_deployments_in_application(self, app_name: str):
        return [
            name
            for name, info in self.deployment_infos.items()
            if info.app_name == app_name
        ]

    def delete_deployment(self, deployment_name: str):
        pass

    def set_deployment_deleted(self, name: str):
        del self.deployment_infos[name]
        del self.deployment_statuses[name]


@pytest.fixture
def mocked_application_state_manager() -> Tuple[
    ApplicationStateManager, MockDeploymentStateManager
]:
    deployment_state_manager = MockDeploymentStateManager()

    application_state_manager = ApplicationStateManager(
        deployment_state_manager, MockEndpointState(), MockKVStore()
    )
    yield application_state_manager, deployment_state_manager


def deployment_params(name: str, route_prefix: str = None):
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


@pytest.fixture
def mocked_application_state() -> Tuple[ApplicationState, MockDeploymentStateManager]:
    deployment_state_manager = MockDeploymentStateManager()

    application_state = ApplicationState(
        "test_app",
        deployment_state_manager,
        MockEndpointState(),
        lambda *args, **kwargs: None,
    )
    yield application_state, deployment_state_manager


def test_deploy_app(mocked_application_state):
    """Test DEPLOYING status"""
    app_state, _ = mocked_application_state
    app_state.apply_deployment_args([deployment_params("d1")])

    app_status = app_state.get_application_status_info()
    assert app_status.status == ApplicationStatus.DEPLOYING
    assert app_status.deployment_timestamp > 0


def test_delete_app(mocked_application_state):
    """Test DELETING status"""
    app_state, _ = mocked_application_state
    app_state.apply_deployment_args([deployment_params("d1")])
    app_state.delete()
    assert app_state.status == ApplicationStatus.DELETING


def test_create_app(mocked_application_state_manager):
    """Test object ref based deploy and set DEPLOYING"""
    app_state_manager, _ = mocked_application_state_manager
    app_state_manager.create_application_state("test_app", ray.ObjectRef.nil())
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.DEPLOYING


def test_app_running(mocked_application_state):
    """Test DEPLOYING -> RUNNING"""
    app_state, deployment_state_manager = mocked_application_state
    assert app_state.status == ApplicationStatus.NOT_STARTED
    # Immediately after apply_deployment_args() returns, status should be DEPLOYING
    app_state.apply_deployment_args([deployment_params("a"), deployment_params("b")])
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before deployments are healthy, status should remain DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # One deployment healthy
    deployment_state_manager.set_deployment_healthy("a")
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Both deployments healthy
    deployment_state_manager.set_deployment_healthy("b")
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # Rerun update, application status should not make difference
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING


def test_app_deploy_failed(mocked_application_state):
    """Test DEPLOYING -> DEPLOY_FAILED"""
    app_state, deployment_state_manager = mocked_application_state
    app_state.apply_deployment_args([deployment_params("d1")])
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before status of deployment changes, app should still be DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Mark deployment unhealthy -> app should be DEPLOY_FAILED
    deployment_state_manager.set_deployment_unhealthy("d1")
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED

    # Rerun update, application status should not make difference
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED

    # """Test DEPLOYING -> DEPLOY_FAILED -> DEPLOYING -> RUNNING"""
    # app_state_manager, deployment_state_manager = mocked_application_state_manager
    # app_state_manager.deploy_application("test_app", [{"name": "d1"}])
    # # Simulate controller
    # deployment_state_manager.deploy("d1", None)

    # app_status = app_state_manager.get_app_status("test_app")
    # assert app_status.status == ApplicationStatus.DEPLOYING
    # deployment_state_manager.set_deployment_statuses_unhealthy("d1")
    # app_state_manager.update()
    # app_status = app_state_manager.get_app_status("test_app")
    # assert app_status.status == ApplicationStatus.DEPLOY_FAILED
    # # rerun update, application status should not make difference
    # deploy_failed_msg = app_status.message
    # assert len(deploy_failed_msg) != 0
    # app_state_manager.update()
    # assert app_status.status == ApplicationStatus.DEPLOY_FAILED
    # assert app_status.message == deploy_failed_msg

    # app_state_manager.deploy_application("test_app", [{"name": "d1"}, {"name": "d2"}])
    # # Simulate controller
    # deployment_state_manager.deploy("d1", None)
    # deployment_state_manager.deploy("d2", None)

    # app_status = app_state_manager.get_app_status("test_app")
    # assert app_status.status == ApplicationStatus.DEPLOYING
    # assert app_status.message != deploy_failed_msg
    # deployment_state_manager.set_deployment_statuses_healthy("d1")
    # deployment_state_manager.set_deployment_statuses_healthy("d2")
    # app_state_manager.update()
    # app_status = app_state_manager.get_app_status("test_app")
    # assert app_status.status == ApplicationStatus.RUNNING
    # running_msg = app_status.message
    # assert running_msg != deploy_failed_msg
    # # rerun update, application status should not make difference
    # app_state_manager.update()
    # assert app_status.status == ApplicationStatus.RUNNING
    # assert app_status.message == running_msg


def test_app_unhealthy(mocked_application_state):
    """something."""
    app_state, deployment_state_manager = mocked_application_state
    app_state.apply_deployment_args([deployment_params("a"), deployment_params("b")])
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Update
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING
    assert set(app_state.deployments) == {"a", "b"}

    # Set running
    deployment_state_manager.set_deployment_healthy("a")
    deployment_state_manager.set_deployment_healthy("b")
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # If a deployment becomes unhealthy, application should become unhealthy
    deployment_state_manager.set_deployment_unhealthy("a")
    app_state.update()
    assert app_state.status == ApplicationStatus.UNHEALTHY

    # Rerun update, application status should remain unhealthy
    app_state.update()
    assert app_state.status == ApplicationStatus.UNHEALTHY


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("fail_deploy", [False, True])
def test_config_deploy_app(mocked_application_state_manager, fail_deploy):
    """Test config based deploy
    DEPLOYING -> RUNNING
    DEPLOYING -> DEPLOY_FAILED
    """
    app_state_manager, deployment_state_manager = mocked_application_state_manager
    signal = SignalActor.remote()

    @ray.remote
    def task():
        ray.get(signal.wait.remote())
        if fail_deploy:
            raise Exception("Intentionally failed task.")

    deploy_app_obj_ref = task.remote()

    # Create application state
    app_state_manager.create_application_state("test_app", deploy_app_obj_ref)
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.DEPLOYING
    # Before object ref is read
    app_state_manager.update()
    assert app_state_manager.get_app_status("test_app") == ApplicationStatus.DEPLOYING

    signal.send.remote()
    # Wait for task to return
    wait_for_condition(lambda: len(ray.wait([deploy_app_obj_ref], timeout=0)[0]))
    if fail_deploy:
        app_state_manager.update()
        assert (
            app_state_manager.get_app_status("test_app")
            == ApplicationStatus.DEPLOY_FAILED
        )
    else:
        app_state_manager.apply_deployment_args(
            "test_app", [deployment_params("a"), deployment_params("b")]
        )
        app_state_manager.update()
        deployment_state_manager.set_deployment_healthy("a")
        deployment_state_manager.set_deployment_healthy("b")
        app_state_manager.update()
        assert app_state_manager.get_app_status("test_app") == ApplicationStatus.RUNNING


def test_redeploy_same_app(mocked_application_state):
    """Test deploying the same app with different deploy_params."""
    app_state, deployment_state_manager = mocked_application_state
    app_state.apply_deployment_args([deployment_params("a"), deployment_params("b")])
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Update
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING
    assert set(app_state.deployments) == {"a", "b"}

    # Set running
    deployment_state_manager.set_deployment_healthy("a")
    deployment_state_manager.set_deployment_healthy("b")
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # Deploy the same app with different deployments
    app_state.apply_deployment_args([deployment_params("b"), deployment_params("c")])
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Update
    app_state.update()
    assert "a" not in app_state.deployments

    # Remove deployment a
    deployment_state_manager.set_deployment_deleted("a")
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Move to running
    deployment_state_manager.set_deployment_healthy("b")
    deployment_state_manager.set_deployment_healthy("c")
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING


def test_deploy_with_route_prefix_conflict(mocked_application_state_manager):
    """Test that an application fails to deploy with a route prefix conflict."""
    app_state_manager, _ = mocked_application_state_manager

    app_state_manager.apply_deployment_args("app1", [deployment_params("a", "/hi")])
    with pytest.raises(RayServeException):
        app_state_manager.apply_deployment_args("app2", [deployment_params("b", "/hi")])


def test_deploy_with_renamed_app(mocked_application_state_manager):
    """
    Test that an application deploys successfully when there is a route prefix
    conflict with an old app running on the cluster.
    """
    app_state_manager, deployment_state_manager = mocked_application_state_manager

    # deploy app1
    app_state_manager.apply_deployment_args("app1", [deployment_params("a", "/url1")])
    app_state = app_state_manager._application_states["app1"]
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DEPLOYING

    # Update
    app_state_manager.update()
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DEPLOYING
    assert set(app_state.deployments) == {"a"}

    # Once its single deployment is healthy, app1 should be running
    deployment_state_manager.set_deployment_healthy("a")
    app_state_manager.update()
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.RUNNING

    # delete app1
    app_state_manager.delete_application("app1")
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DELETING
    app_state_manager.update()

    # deploy app2
    app_state_manager.apply_deployment_args("app2", [deployment_params("b", "/url1")])
    assert app_state_manager.get_app_status("app2") == ApplicationStatus.DEPLOYING
    app_state_manager.update()

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
