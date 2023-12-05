import sys
from typing import Dict, List, Tuple
from unittest.mock import Mock, PropertyMock, patch

import pytest

from ray.exceptions import RayTaskError
from ray.serve._private.application_state import (
    ApplicationState,
    ApplicationStateManager,
    override_deployment_info,
)
from ray.serve._private.common import (
    ApplicationStatus,
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    DeploymentStatusTrigger,
)
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.deploy_utils import deploy_args_to_deployment_info
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.utils import get_random_letters
from ray.serve.exceptions import RayServeException
from ray.serve.schema import DeploymentSchema, ServeApplicationSchema
from ray.serve.tests.common.utils import MockKVStore


class MockEndpointState:
    def __init__(self):
        self.endpoints = dict()

    def update_endpoint(self, endpoint, endpoint_info):
        self.endpoints[endpoint] = endpoint_info

    def delete_endpoint(self, endpoint):
        if endpoint in self.endpoints:
            del self.endpoints[endpoint]


class MockDeploymentStateManager:
    def __init__(self, kv_store):
        self.kv_store = kv_store
        self.deployment_infos: Dict[DeploymentID, DeploymentInfo] = dict()
        self.deployment_statuses: Dict[DeploymentID, DeploymentStatusInfo] = dict()
        self.deleting: Dict[DeploymentID, bool] = dict()

        # Recover
        recovered_deployments = self.kv_store.get("fake_deployment_state_checkpoint")
        if recovered_deployments is not None:
            for name, checkpointed_data in recovered_deployments.items():
                (info, deleting) = checkpointed_data

                self.deployment_infos[name] = info
                self.deployment_statuses[name] = DeploymentStatus.UPDATING
                self.deleting[name] = deleting

    def deploy(
        self,
        deployment_id: DeploymentID,
        deployment_info: DeploymentInfo,
    ):
        existing_info = self.deployment_infos.get(deployment_id)
        self.deleting[deployment_id] = False
        self.deployment_infos[deployment_id] = deployment_info
        if not existing_info or existing_info.version != deployment_info.version:
            self.deployment_statuses[deployment_id] = DeploymentStatusInfo(
                name=deployment_id.name,
                status=DeploymentStatus.UPDATING,
                status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                message="",
            )

        self.kv_store.put(
            "fake_deployment_state_checkpoint",
            dict(
                zip(
                    self.deployment_infos.keys(),
                    zip(self.deployment_infos.values(), self.deleting.values()),
                )
            ),
        )

    @property
    def deployments(self) -> List[str]:
        return list(self.deployment_infos.keys())

    def get_deployment_statuses(self, ids: List[DeploymentID]):
        return [self.deployment_statuses[id] for id in ids]

    def get_deployment(self, deployment_id: DeploymentID) -> DeploymentInfo:
        if deployment_id in self.deployment_statuses:
            # Return dummy deployment info object
            return DeploymentInfo(
                deployment_config=DeploymentConfig(num_replicas=1, user_config={}),
                replica_config=ReplicaConfig.create(lambda x: x),
                start_time_ms=0,
                deployer_job_id="",
            )

    def get_deployments_in_application(self, app_name: str):
        deployments = []
        for deployment_id in self.deployment_infos:
            if deployment_id.app == app_name:
                deployments.append(deployment_id.name)

        return deployments

    def set_deployment_unhealthy(self, id: DeploymentID):
        self.deployment_statuses[id].status = DeploymentStatus.UNHEALTHY

    def set_deployment_healthy(self, id: DeploymentID):
        self.deployment_statuses[id].status = DeploymentStatus.HEALTHY

    def set_deployment_updating(self, id: DeploymentID):
        self.deployment_statuses[id].status = DeploymentStatus.UPDATING

    def set_deployment_deleted(self, id: str):
        if not self.deployment_infos[id]:
            raise ValueError(
                f"Tried to mark deployment {id} as deleted, but {id} not found"
            )
        if not self.deleting[id]:
            raise ValueError(
                f"Tried to mark deployment {id} as deleted, but delete_deployment()"
                f"hasn't been called for {id} yet"
            )

        del self.deployment_infos[id]
        del self.deployment_statuses[id]
        del self.deleting[id]

    def delete_deployment(self, id: DeploymentID):
        self.deleting[id] = True


@pytest.fixture
def mocked_application_state_manager() -> (
    Tuple[ApplicationStateManager, MockDeploymentStateManager]
):
    kv_store = MockKVStore()

    deployment_state_manager = MockDeploymentStateManager(kv_store)
    application_state_manager = ApplicationStateManager(
        deployment_state_manager, MockEndpointState(), kv_store
    )
    yield application_state_manager, deployment_state_manager, kv_store


def deployment_params(name: str, route_prefix: str = None, docs_path: str = None):
    return {
        "deployment_name": name,
        "deployment_config_proto_bytes": DeploymentConfig(
            num_replicas=1, user_config={}, version=get_random_letters()
        ).to_proto_bytes(),
        "replica_config_proto_bytes": ReplicaConfig.create(
            lambda x: x
        ).to_proto_bytes(),
        "deployer_job_id": "random",
        "route_prefix": route_prefix,
        "docs_path": docs_path,
        "ingress": False,
    }


def deployment_info(name: str, route_prefix: str = None, docs_path: str = None):
    params = deployment_params(name, route_prefix, docs_path)
    return deploy_args_to_deployment_info(**params, app_name="test_app")


@pytest.fixture
def mocked_application_state() -> Tuple[ApplicationState, MockDeploymentStateManager]:
    kv_store = MockKVStore()

    deployment_state_manager = MockDeploymentStateManager(kv_store)
    application_state = ApplicationState(
        "test_app",
        deployment_state_manager,
        MockEndpointState(),
        lambda *args, **kwargs: None,
    )
    yield application_state, deployment_state_manager


@patch.object(
    ApplicationState, "target_deployments", PropertyMock(return_value=["a", "b", "c"])
)
class TestDetermineAppStatus:
    @patch.object(ApplicationState, "get_deployments_statuses")
    def test_running(self, get_deployments_statuses, mocked_application_state):
        app_state, _ = mocked_application_state
        get_deployments_statuses.return_value = [
            DeploymentStatusInfo(
                "a",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "b",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "c",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
        ]
        assert app_state._determine_app_status() == (ApplicationStatus.RUNNING, "")

    @patch.object(ApplicationState, "get_deployments_statuses")
    def test_stay_running(self, get_deployments_statuses, mocked_application_state):
        app_state, _ = mocked_application_state
        app_state._status = ApplicationStatus.RUNNING
        get_deployments_statuses.return_value = [
            DeploymentStatusInfo(
                "a",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "b",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "c",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
        ]
        assert app_state._determine_app_status() == (ApplicationStatus.RUNNING, "")

    @patch.object(ApplicationState, "get_deployments_statuses")
    def test_deploying(self, get_deployments_statuses, mocked_application_state):
        app_state, _ = mocked_application_state
        get_deployments_statuses.return_value = [
            DeploymentStatusInfo(
                "a",
                DeploymentStatus.UPDATING,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "b",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "c",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
        ]
        assert app_state._determine_app_status() == (ApplicationStatus.DEPLOYING, "")

    @patch.object(ApplicationState, "get_deployments_statuses")
    def test_deploy_failed(self, get_deployments_statuses, mocked_application_state):
        app_state, _ = mocked_application_state
        get_deployments_statuses.return_value = [
            DeploymentStatusInfo(
                "a",
                DeploymentStatus.UPDATING,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "b",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "c",
                DeploymentStatus.UNHEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
        ]
        status, error_msg = app_state._determine_app_status()
        assert status == ApplicationStatus.DEPLOY_FAILED
        assert error_msg

    @patch.object(ApplicationState, "get_deployments_statuses")
    def test_unhealthy(self, get_deployments_statuses, mocked_application_state):
        app_state, _ = mocked_application_state
        app_state._status = ApplicationStatus.RUNNING
        get_deployments_statuses.return_value = [
            DeploymentStatusInfo(
                "a",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "b",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "c",
                DeploymentStatus.UNHEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
        ]
        status, error_msg = app_state._determine_app_status()
        assert status == ApplicationStatus.UNHEALTHY
        assert error_msg

    @patch.object(ApplicationState, "get_deployments_statuses")
    def test_autoscaling(self, get_deployments_statuses, mocked_application_state):
        app_state, _ = mocked_application_state
        app_state._status = ApplicationStatus.RUNNING
        get_deployments_statuses.return_value = [
            DeploymentStatusInfo(
                "a",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "b", DeploymentStatus.UPSCALING, DeploymentStatusTrigger.AUTOSCALING
            ),
            DeploymentStatusInfo(
                "c", DeploymentStatus.DOWNSCALING, DeploymentStatusTrigger.AUTOSCALING
            ),
        ]
        status, error_msg = app_state._determine_app_status()
        assert status == ApplicationStatus.RUNNING

    @patch.object(ApplicationState, "get_deployments_statuses")
    def test_manual_scale_num_replicas(
        self, get_deployments_statuses, mocked_application_state
    ):
        app_state, _ = mocked_application_state
        app_state._status = ApplicationStatus.RUNNING
        get_deployments_statuses.return_value = [
            DeploymentStatusInfo(
                "a",
                DeploymentStatus.HEALTHY,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "b",
                DeploymentStatus.UPSCALING,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            DeploymentStatusInfo(
                "c",
                DeploymentStatus.DOWNSCALING,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
        ]
        status, error_msg = app_state._determine_app_status()
        assert status == ApplicationStatus.DEPLOYING


def test_deploy_and_delete_app(mocked_application_state):
    """Deploy app with 2 deployments, transition DEPLOYING -> RUNNING -> DELETING.
    This tests the basic typical workflow.
    """
    app_state, deployment_state_manager = mocked_application_state

    # DEPLOY application with deployments {d1, d2}
    d1_id = DeploymentID("d1", "test_app")
    d2_id = DeploymentID("d2", "test_app")
    app_state.deploy(
        {
            "d1": deployment_info("d1", "/hi", "/documentation"),
            "d2": deployment_info("d2"),
        }
    )
    assert app_state.route_prefix == "/hi"
    assert app_state.docs_path == "/documentation"

    app_status = app_state.get_application_status_info()
    assert app_status.status == ApplicationStatus.DEPLOYING
    assert app_status.deployment_timestamp > 0

    app_state.update()
    # After one update, deployments {d1, d2} should be created
    assert deployment_state_manager.get_deployment(d1_id)
    assert deployment_state_manager.get_deployment(d2_id)
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Until both deployments are healthy, app should be deploying
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Mark deployment d1 healthy (app should be deploying)
    deployment_state_manager.set_deployment_healthy(d1_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Mark deployment d2 healthy (app should be running)
    deployment_state_manager.set_deployment_healthy(d2_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # Rerun update, status shouldn't change (still running)
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # Delete application (app should be deleting)
    app_state.delete()
    assert app_state.status == ApplicationStatus.DELETING

    app_state.update()
    deployment_state_manager.set_deployment_deleted(d1_id)
    ready_to_be_deleted = app_state.update()
    assert not ready_to_be_deleted
    assert app_state.status == ApplicationStatus.DELETING

    # Once both deployments are deleted, the app should be ready to delete
    deployment_state_manager.set_deployment_deleted(d2_id)
    ready_to_be_deleted = app_state.update()
    assert ready_to_be_deleted


def test_app_deploy_failed_and_redeploy(mocked_application_state):
    """Test DEPLOYING -> DEPLOY_FAILED -> (redeploy) -> DEPLOYING -> RUNNING"""
    app_state, deployment_state_manager = mocked_application_state
    d1_id = DeploymentID("d1", "test_app")
    d2_id = DeploymentID("d2", "test_app")
    app_state.deploy({"d1": deployment_info("d1")})
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before status of deployment changes, app should still be DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Mark deployment unhealthy -> app should be DEPLOY_FAILED
    deployment_state_manager.set_deployment_unhealthy(d1_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED

    # Message and status should not change
    deploy_failed_msg = app_state._status_msg
    assert len(deploy_failed_msg) != 0
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED
    assert app_state._status_msg == deploy_failed_msg

    app_state.deploy({"d1": deployment_info("d1"), "d2": deployment_info("d2")})
    assert app_state.status == ApplicationStatus.DEPLOYING
    assert app_state._status_msg != deploy_failed_msg

    # After one update, deployments {d1, d2} should be created
    app_state.update()
    assert deployment_state_manager.get_deployment(d1_id)
    assert deployment_state_manager.get_deployment(d2_id)
    assert app_state.status == ApplicationStatus.DEPLOYING

    deployment_state_manager.set_deployment_healthy(d1_id)
    deployment_state_manager.set_deployment_healthy(d2_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # Message and status should not change
    running_msg = app_state._status_msg
    assert running_msg != deploy_failed_msg
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING
    assert app_state._status_msg == running_msg


def test_app_deploy_failed_and_recover(mocked_application_state):
    """Test DEPLOYING -> DEPLOY_FAILED -> (self recovered) -> RUNNING

    If while the application is deploying a deployment becomes unhealthy,
    the app is marked as deploy failed. But if the deployment recovers,
    the application status should update to running.
    """
    app_state, deployment_state_manager = mocked_application_state
    deployment_id = DeploymentID("d1", "test_app")
    app_state.deploy({"d1": deployment_info("d1")})
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before status of deployment changes, app should still be DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Mark deployment unhealthy -> app should be DEPLOY_FAILED
    deployment_state_manager.set_deployment_unhealthy(deployment_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED

    # Deployment recovers to healthy -> app should be RUNNING
    deployment_state_manager.set_deployment_healthy(deployment_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING


def test_app_unhealthy(mocked_application_state):
    """Test DEPLOYING -> RUNNING -> UNHEALTHY -> RUNNING.
    Even after an application becomes running, if a deployment becomes
    unhealthy at some point, the application status should also be
    updated to unhealthy.
    """
    app_state, deployment_state_manager = mocked_application_state
    id_a, id_b = DeploymentID("a", "test_app"), DeploymentID("b", "test_app")
    app_state.deploy({"a": deployment_info("a"), "b": deployment_info("b")})
    assert app_state.status == ApplicationStatus.DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Once both deployments become healthy, app should be running
    deployment_state_manager.set_deployment_healthy(id_a)
    deployment_state_manager.set_deployment_healthy(id_b)
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # If a deployment becomes unhealthy, application should become unhealthy
    deployment_state_manager.set_deployment_unhealthy(id_a)
    app_state.update()
    assert app_state.status == ApplicationStatus.UNHEALTHY
    # Rerunning update shouldn't make a difference
    app_state.update()
    assert app_state.status == ApplicationStatus.UNHEALTHY

    # If the deployment recovers, the application should also recover
    deployment_state_manager.set_deployment_healthy(id_a)
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING


@patch("ray.serve._private.application_state.build_serve_application", Mock())
@patch("ray.get", Mock(return_value=([deployment_params("a", "/old", "/docs")], None)))
@patch("ray.serve._private.application_state.check_obj_ref_ready_nowait")
def test_deploy_through_config_succeed(check_obj_ref_ready_nowait):
    """Test deploying through config successfully.
    Deploy obj ref finishes successfully, so status should transition to running.
    """
    kv_store = MockKVStore()
    deployment_id = DeploymentID("a", "test_app")
    deployment_state_manager = MockDeploymentStateManager(kv_store)
    app_state_manager = ApplicationStateManager(
        deployment_state_manager, MockEndpointState(), kv_store
    )

    # Deploy config
    app_config = ServeApplicationSchema(import_path="fa.ke", route_prefix="/new")
    app_state_manager.deploy_config(name="test_app", app_config=app_config)
    app_state = app_state_manager._application_states["test_app"]
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before object ref is ready
    check_obj_ref_ready_nowait.return_value = False
    app_state.update()
    assert app_state._build_app_task_info
    assert app_state.status == ApplicationStatus.DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Object ref is ready
    check_obj_ref_ready_nowait.return_value = True
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING
    assert app_state.target_deployments == ["a"]
    assert app_state.route_prefix == "/new"
    assert app_state.docs_path == "/docs"

    # Set healthy
    deployment_state_manager.set_deployment_healthy(deployment_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING


@patch(
    "ray.serve._private.application_state.get_app_code_version",
    Mock(return_value="123"),
)
@patch("ray.serve._private.application_state.build_serve_application", Mock())
@patch("ray.get", Mock(side_effect=RayTaskError(None, "intentionally failed", None)))
@patch("ray.serve._private.application_state.check_obj_ref_ready_nowait")
def test_deploy_through_config_fail(check_obj_ref_ready_nowait):
    """Test fail to deploy through config.
    Deploy obj ref errors out, so status should transition to deploy failed.
    """
    kv_store = MockKVStore()
    deployment_state_manager = MockDeploymentStateManager(kv_store)
    app_state_manager = ApplicationStateManager(
        deployment_state_manager, MockEndpointState(), kv_store
    )
    # Deploy config
    app_state_manager.deploy_config(name="test_app", app_config=Mock())
    app_state = app_state_manager._application_states["test_app"]
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before object ref is ready
    check_obj_ref_ready_nowait.return_value = False
    app_state.update()
    assert app_state._build_app_task_info
    assert app_state.status == ApplicationStatus.DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Object ref is ready, and the task has called apply_deployment_args
    check_obj_ref_ready_nowait.return_value = True
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED
    assert "failed" in app_state._status_msg or "error" in app_state._status_msg


def test_redeploy_same_app(mocked_application_state):
    """Test redeploying same application with updated deployments."""
    app_state, deployment_state_manager = mocked_application_state
    a_id = DeploymentID("a", "test_app")
    b_id = DeploymentID("b", "test_app")
    c_id = DeploymentID("c", "test_app")
    app_state.deploy({"a": deployment_info("a"), "b": deployment_info("b")})
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Update
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING
    assert set(app_state.target_deployments) == {"a", "b"}

    # Transition to running
    deployment_state_manager.set_deployment_healthy(a_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING
    deployment_state_manager.set_deployment_healthy(b_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # Deploy the same app with different deployments
    app_state.deploy({"b": deployment_info("b"), "c": deployment_info("c")})
    assert app_state.status == ApplicationStatus.DEPLOYING
    # Target state should be updated immediately
    assert "a" not in app_state.target_deployments

    # Remove deployment `a`
    app_state.update()
    deployment_state_manager.set_deployment_deleted(a_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Move to running
    deployment_state_manager.set_deployment_healthy(c_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING
    deployment_state_manager.set_deployment_healthy(b_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.RUNNING


def test_deploy_with_route_prefix_conflict(mocked_application_state_manager):
    """Test that an application with a route prefix conflict fails to deploy"""
    app_state_manager, _, _ = mocked_application_state_manager

    app_state_manager.apply_deployment_args("app1", [deployment_params("a", "/hi")])
    with pytest.raises(RayServeException):
        app_state_manager.apply_deployment_args("app2", [deployment_params("b", "/hi")])


def test_deploy_with_renamed_app(mocked_application_state_manager):
    """
    Test that an application deploys successfully when there is a route prefix
    conflict with an old app running on the cluster.
    """
    app_state_manager, deployment_state_manager, _ = mocked_application_state_manager
    a_id, b_id = DeploymentID("a", "app1"), DeploymentID("b", "app2")

    # deploy app1
    app_state_manager.apply_deployment_args("app1", [deployment_params("a", "/url1")])
    app_state = app_state_manager._application_states["app1"]
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DEPLOYING

    # Update
    app_state_manager.update()
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DEPLOYING
    assert set(app_state.target_deployments) == {"a"}

    # Once its single deployment is healthy, app1 should be running
    deployment_state_manager.set_deployment_healthy(a_id)
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
    deployment_state_manager.set_deployment_healthy(b_id)
    app_state_manager.update()
    assert app_state_manager.get_app_status("app2") == ApplicationStatus.RUNNING
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DELETING

    # app1 finally finishes deleting
    deployment_state_manager.set_deployment_deleted(a_id)
    app_state_manager.update()
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.NOT_STARTED
    assert app_state_manager.get_app_status("app2") == ApplicationStatus.RUNNING


def test_application_state_recovery(mocked_application_state_manager):
    """Test DEPLOYING -> RUNNING -> (controller crash) -> DEPLOYING -> RUNNING"""
    (
        app_state_manager,
        deployment_state_manager,
        kv_store,
    ) = mocked_application_state_manager
    deployment_id = DeploymentID("d1", "test_app")
    app_name = "test_app"

    # DEPLOY application with deployments {d1, d2}
    params = deployment_params("d1")
    app_state_manager.apply_deployment_args(app_name, [params])
    app_state = app_state_manager._application_states[app_name]
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Once deployment is healthy, app should be running
    app_state_manager.update()
    assert deployment_state_manager.get_deployment(deployment_id)
    deployment_state_manager.set_deployment_healthy(deployment_id)
    app_state_manager.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # Simulate controller crashed!! Create new deployment state manager,
    # which should recover target state for deployment "d1" from kv store
    new_deployment_state_manager = MockDeploymentStateManager(kv_store)
    version1 = new_deployment_state_manager.deployment_infos[deployment_id].version

    # Create new application state manager, and it should call _recover_from_checkpoint
    new_app_state_manager = ApplicationStateManager(
        new_deployment_state_manager, MockEndpointState(), kv_store
    )
    app_state = new_app_state_manager._application_states[app_name]
    assert app_state.status == ApplicationStatus.DEPLOYING
    assert app_state._target_state.deployment_infos["d1"].version == version1

    new_deployment_state_manager.set_deployment_healthy(deployment_id)
    new_app_state_manager.update()
    assert app_state.status == ApplicationStatus.RUNNING


def test_recover_during_update(mocked_application_state_manager):
    """Test that application and deployment states are recovered if
    controller crashed in the middle of a redeploy.

    Target state is checkpointed in the application state manager,
    but not yet the deployment state manager when the controller crashes
    Then the deployment state manager should recover an old version of
    the deployment during initial recovery, but the application state
    manager should eventually reconcile this.
    """
    (
        app_state_manager,
        deployment_state_manager,
        kv_store,
    ) = mocked_application_state_manager
    deployment_id = DeploymentID("d1", "test_app")
    app_name = "test_app"

    # DEPLOY application with deployment "d1"
    params = deployment_params("d1")
    app_state_manager.apply_deployment_args(app_name, [params])
    app_state = app_state_manager._application_states[app_name]
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Once deployment is healthy, app should be running
    app_state_manager.update()
    assert deployment_state_manager.get_deployment(deployment_id)
    deployment_state_manager.set_deployment_healthy(deployment_id)
    app_state_manager.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # Deploy new version of "d1" (this auto generates new random version)
    params2 = deployment_params("d1")
    app_state_manager.apply_deployment_args(app_name, [params2])
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before application state manager could propagate new version to
    # deployment state manager, controller crashes.
    # Create new deployment state manager. It should recover the old
    # version of the deployment from the kv store
    new_deployment_state_manager = MockDeploymentStateManager(kv_store)
    dr_version = new_deployment_state_manager.deployment_infos[deployment_id].version

    # Create new application state manager, and it should call _recover_from_checkpoint
    new_app_state_manager = ApplicationStateManager(
        new_deployment_state_manager, MockEndpointState(), kv_store
    )
    app_state = new_app_state_manager._application_states[app_name]
    ar_version = app_state._target_state.deployment_infos["d1"].version
    assert app_state.status == ApplicationStatus.DEPLOYING
    assert ar_version != dr_version

    new_app_state_manager.update()
    assert (
        new_deployment_state_manager.deployment_infos[deployment_id].version
        == ar_version
    )
    assert app_state.status == ApplicationStatus.DEPLOYING

    new_deployment_state_manager.set_deployment_healthy(deployment_id)
    new_app_state_manager.update()
    assert app_state.status == ApplicationStatus.RUNNING


def test_is_ready_for_shutdown(mocked_application_state_manager):
    """Test `is_ready_for_shutdown()` returns the correct state.

    When shutting down applications before deployments are deleted, application state
    `is_deleted()` should return False and `is_ready_for_shutdown()` should return
    False. When shutting down applications after deployments are deleted, application
    state `is_deleted()` should return True and `is_ready_for_shutdown()` should return
    True.
    """
    (
        app_state_manager,
        deployment_state_manager,
        kv_store,
    ) = mocked_application_state_manager
    app_name = "test_app"
    deployment_name = "d1"
    deployment_id = DeploymentID(deployment_name, app_name)

    # DEPLOY application with deployment "d1"
    params = deployment_params(deployment_name)
    app_state_manager.apply_deployment_args(app_name, [params])
    app_state = app_state_manager._application_states[app_name]
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Once deployment is healthy, app should be running
    app_state_manager.update()
    assert deployment_state_manager.get_deployment(deployment_id)
    deployment_state_manager.set_deployment_healthy(deployment_id)
    app_state_manager.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # When shutting down applications before deployments are deleted, application state
    # `is_deleted()` should return False and `is_ready_for_shutdown()` should return
    # False
    app_state_manager.shutdown()
    assert not app_state.is_deleted()
    assert not app_state_manager.is_ready_for_shutdown()

    # When shutting down applications after deployments are deleted, application state
    # `is_deleted()` should return True and `is_ready_for_shutdown()` should return True
    deployment_state_manager.delete_deployment(deployment_id)
    deployment_state_manager.set_deployment_deleted(deployment_id)
    app_state_manager.update()
    assert app_state.is_deleted()
    assert app_state_manager.is_ready_for_shutdown()


class TestOverrideDeploymentInfo:
    @pytest.fixture
    def info(self):
        return DeploymentInfo(
            route_prefix="/",
            version="123",
            deployment_config=DeploymentConfig(num_replicas=1),
            replica_config=ReplicaConfig.create(lambda x: x),
            start_time_ms=0,
            deployer_job_id="",
        )

    def test_override_deployment_config(self, info):
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            deployments=[
                DeploymentSchema(
                    name="A",
                    num_replicas=3,
                    max_concurrent_queries=200,
                    user_config={"price": "4"},
                    graceful_shutdown_wait_loop_s=4,
                    graceful_shutdown_timeout_s=40,
                    health_check_period_s=20,
                    health_check_timeout_s=60,
                )
            ],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert updated_info.deployment_config.max_concurrent_queries == 200
        assert updated_info.deployment_config.user_config == {"price": "4"}
        assert updated_info.deployment_config.graceful_shutdown_wait_loop_s == 4
        assert updated_info.deployment_config.graceful_shutdown_timeout_s == 40
        assert updated_info.deployment_config.health_check_period_s == 20
        assert updated_info.deployment_config.health_check_timeout_s == 60

    def test_override_autoscaling_config(self, info):
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            deployments=[
                DeploymentSchema(
                    name="A",
                    autoscaling_config={
                        "min_replicas": 1,
                        "initial_replicas": 12,
                        "max_replicas": 79,
                    },
                )
            ],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert updated_info.autoscaling_policy.config.min_replicas == 1
        assert updated_info.autoscaling_policy.config.initial_replicas == 12
        assert updated_info.autoscaling_policy.config.max_replicas == 79

    def test_override_route_prefix_1(self, info):
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            deployments=[DeploymentSchema(name="A", route_prefix="/alice")],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/alice"
        assert updated_info.version == "123"

    def test_override_route_prefix_2(self, info):
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            route_prefix="/bob",
            deployments=[
                DeploymentSchema(
                    name="A",
                )
            ],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/bob"
        assert updated_info.version == "123"

    def test_override_route_prefix_3(self, info):
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            route_prefix="/bob",
            deployments=[DeploymentSchema(name="A", route_prefix="/alice")],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/bob"
        assert updated_info.version == "123"

    def test_override_ray_actor_options_1(self, info):
        """Test runtime env specified in config at deployment level."""
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            deployments=[
                DeploymentSchema(
                    name="A",
                    ray_actor_options={"runtime_env": {"working_dir": "s3://B"}},
                )
            ],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert (
            updated_info.replica_config.ray_actor_options["runtime_env"]["working_dir"]
            == "s3://B"
        )

    def test_override_ray_actor_options_2(self, info):
        """Test application runtime env is propagated to deployments."""
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            runtime_env={"working_dir": "s3://C"},
            deployments=[
                DeploymentSchema(
                    name="A",
                )
            ],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert (
            updated_info.replica_config.ray_actor_options["runtime_env"]["working_dir"]
            == "s3://C"
        )

    def test_override_ray_actor_options_3(self, info):
        """If runtime env is specified in the config at the deployment level, it should
        override the application-level runtime env.
        """
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            runtime_env={"working_dir": "s3://C"},
            deployments=[
                DeploymentSchema(
                    name="A",
                    ray_actor_options={"runtime_env": {"working_dir": "s3://B"}},
                )
            ],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert (
            updated_info.replica_config.ray_actor_options["runtime_env"]["working_dir"]
            == "s3://B"
        )

    def test_override_ray_actor_options_4(self):
        """If runtime env is specified for the deployment in code, it should override
        the application-level runtime env.
        """
        info = DeploymentInfo(
            route_prefix="/",
            version="123",
            deployment_config=DeploymentConfig(num_replicas=1),
            replica_config=ReplicaConfig.create(
                lambda x: x,
                ray_actor_options={"runtime_env": {"working_dir": "s3://A"}},
            ),
            start_time_ms=0,
            deployer_job_id="",
        )
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            runtime_env={"working_dir": "s3://C"},
            deployments=[
                DeploymentSchema(
                    name="A",
                )
            ],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert (
            updated_info.replica_config.ray_actor_options["runtime_env"]["working_dir"]
            == "s3://A"
        )

    def test_override_ray_actor_options_5(self):
        """If runtime env is specified in all three places:
        - In code
        - In the config at the deployment level
        - In the config at the application level
        The one specified in the config at the deployment level should take precedence.
        """
        info = DeploymentInfo(
            route_prefix="/",
            version="123",
            deployment_config=DeploymentConfig(num_replicas=1),
            replica_config=ReplicaConfig.create(
                lambda x: x,
                ray_actor_options={"runtime_env": {"working_dir": "s3://A"}},
            ),
            start_time_ms=0,
            deployer_job_id="",
        )
        config = ServeApplicationSchema(
            name="default",
            import_path="test.import.path",
            runtime_env={"working_dir": "s3://C"},
            deployments=[
                DeploymentSchema(
                    name="A",
                    ray_actor_options={"runtime_env": {"working_dir": "s3://B"}},
                )
            ],
        )

        updated_infos = override_deployment_info("default", {"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert (
            updated_info.replica_config.ray_actor_options["runtime_env"]["working_dir"]
            == "s3://B"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
