import sys
import time
from typing import Dict, List, Optional, Tuple
from unittest.mock import Mock, PropertyMock, patch

import pytest

from ray.exceptions import RayTaskError
from ray.serve._private.application_state import (
    ApplicationState,
    ApplicationStateManager,
    ApplicationStatusInfo,
    BuildAppStatus,
    StatusOverview,
    override_deployment_info,
)
from ray.serve._private.autoscaling_state import AutoscalingStateManager
from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentHandleSource,
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    DeploymentStatusTrigger,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TimeStampedValue,
)
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.constants import RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE
from ray.serve._private.deploy_utils import deploy_args_to_deployment_info
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.test_utils import MockKVStore
from ray.serve._private.utils import get_random_string
from ray.serve.config import AutoscalingConfig
from ray.serve.exceptions import RayServeException
from ray.serve.generated.serve_pb2 import (
    ApplicationStatusInfo as ApplicationStatusInfoProto,
    StatusOverview as StatusOverviewProto,
)
from ray.serve.schema import (
    APIType,
    ApplicationStatus,
    DeploymentSchema,
    LoggingConfig,
    ServeApplicationSchema,
)


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
                self.deployment_statuses[name] = DeploymentStatusInfo(
                    name=name,
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message="",
                )
                self.deleting[name] = deleting
        self._scaling_decisions = {}

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
                deployment_config=DeploymentConfig(
                    num_replicas=self.deployment_infos[
                        deployment_id
                    ].deployment_config.num_replicas,
                    user_config={},
                ),
                replica_config=ReplicaConfig.create(lambda x: x),
                start_time_ms=0,
                deployer_job_id="",
            )

    def get_deployments_in_application(self, app_name: str):
        deployments = []
        for deployment_id in self.deployment_infos:
            if deployment_id.app_name == app_name:
                deployments.append(deployment_id.name)

        return deployments

    def set_deployment_unhealthy(self, id: DeploymentID):
        self.deployment_statuses[id].status = DeploymentStatus.UNHEALTHY

    def set_deployment_deploy_failed(self, id: DeploymentID):
        self.deployment_statuses[id].status = DeploymentStatus.DEPLOY_FAILED

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

    def get_deployment_target_num_replicas(self, id: DeploymentID) -> Optional[int]:
        return self.deployment_infos[id].deployment_config.num_replicas

    def save_checkpoint(self):
        """Mock save checkpoint method."""
        pass

    def autoscale(self, id: DeploymentID, target_num_replicas: int):
        self._scaling_decisions[id] = target_num_replicas
        return True

    def get_deployment_route_patterns(self, id: DeploymentID) -> Optional[List[str]]:
        return None


@pytest.fixture
def mocked_application_state_manager() -> (
    Tuple[ApplicationStateManager, MockDeploymentStateManager]
):
    kv_store = MockKVStore()

    deployment_state_manager = MockDeploymentStateManager(kv_store)
    application_state_manager = ApplicationStateManager(
        deployment_state_manager,
        AutoscalingStateManager(),
        MockEndpointState(),
        kv_store,
        LoggingConfig(),
    )
    yield application_state_manager, deployment_state_manager, kv_store


def deployment_params(
    name: str,
    route_prefix: str = None,
    autoscaling_config: AutoscalingConfig = None,
    num_replicas: int = 1,
):
    return {
        "deployment_name": name,
        "deployment_config_proto_bytes": DeploymentConfig(
            num_replicas=num_replicas,
            user_config={},
            version=get_random_string(),
            autoscaling_config=autoscaling_config,
        ).to_proto_bytes(),
        "replica_config_proto_bytes": ReplicaConfig.create(
            lambda x: x
        ).to_proto_bytes(),
        "deployer_job_id": "random",
        "route_prefix": route_prefix,
        "ingress": False,
        "serialized_autoscaling_policy_def": None,
        "serialized_request_router_cls": None,
    }


def deployment_info(
    name: str,
    route_prefix: str = None,
    autoscaling_config: AutoscalingConfig = None,
    num_replicas: int = 1,
):
    params = deployment_params(name, route_prefix, autoscaling_config, num_replicas)
    return deploy_args_to_deployment_info(**params, app_name="test_app")


@pytest.fixture
def mocked_application_state() -> Tuple[ApplicationState, MockDeploymentStateManager]:
    kv_store = MockKVStore()

    deployment_state_manager = MockDeploymentStateManager(kv_store)
    application_state = ApplicationState(
        name="test_app",
        deployment_state_manager=deployment_state_manager,
        autoscaling_state_manager=AutoscalingStateManager(),
        endpoint_state=MockEndpointState(),
        logging_config=LoggingConfig(),
    )
    yield application_state, deployment_state_manager


class TestApplicationStatusInfo:
    def test_application_status_required(self):
        with pytest.raises(TypeError):
            ApplicationStatusInfo(
                message="context about status", deployment_timestamp=time.time()
            )

    @pytest.mark.parametrize("status", list(ApplicationStatus))
    def test_proto(self, status):
        serve_application_status_info = ApplicationStatusInfo(
            status=status,
            message="context about status",
            deployment_timestamp=time.time(),
        )
        serialized_proto = serve_application_status_info.to_proto().SerializeToString()
        deserialized_proto = ApplicationStatusInfoProto.FromString(serialized_proto)
        reconstructed_info = ApplicationStatusInfo.from_proto(deserialized_proto)

        assert serve_application_status_info == reconstructed_info


class TestStatusOverview:
    def get_valid_serve_application_status_info(self):
        return ApplicationStatusInfo(
            status=ApplicationStatus.RUNNING,
            message="",
            deployment_timestamp=time.time(),
        )

    def test_app_status_required(self):
        with pytest.raises(TypeError):
            StatusOverview(deployment_statuses=[])

    def test_empty_list_valid(self):
        """Should be able to create StatusOverview with no deployment statuses."""

        # Check default is empty list
        status_info = StatusOverview(
            app_status=self.get_valid_serve_application_status_info()
        )
        status_info.deployment_statuses == []

        # Ensure empty list can be passed in explicitly
        status_info = StatusOverview(
            app_status=self.get_valid_serve_application_status_info(),
            deployment_statuses=[],
        )
        status_info.deployment_statuses == []

    def test_equality_mismatched_deployment_statuses(self):
        """Check that StatusOverviews with different numbers of statuses are unequal."""

        status_info_few_deployments = StatusOverview(
            app_status=self.get_valid_serve_application_status_info(),
            deployment_statuses=[
                DeploymentStatusInfo(
                    name="1",
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="2",
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
            ],
        )

        status_info_many_deployments = StatusOverview(
            app_status=self.get_valid_serve_application_status_info(),
            deployment_statuses=[
                DeploymentStatusInfo(
                    name="1",
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="2",
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="3",
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="4",
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
            ],
        )

        assert status_info_few_deployments != status_info_many_deployments

    @pytest.mark.parametrize("application_status", list(ApplicationStatus))
    def test_proto(self, application_status):
        status_info = StatusOverview(
            app_status=ApplicationStatusInfo(
                status=application_status,
                message="context about this status",
                deployment_timestamp=time.time(),
            ),
            deployment_statuses=[
                DeploymentStatusInfo(
                    name="name1",
                    status=DeploymentStatus.UPDATING,
                    message="deployment updating",
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="name2",
                    status=DeploymentStatus.HEALTHY,
                    message="",
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="name3",
                    status=DeploymentStatus.UNHEALTHY,
                    message="this deployment is unhealthy",
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
            ],
        )
        serialized_proto = status_info.to_proto().SerializeToString()
        deserialized_proto = StatusOverviewProto.FromString(serialized_proto)
        reconstructed_info = StatusOverview.from_proto(deserialized_proto)

        assert status_info == reconstructed_info


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
                DeploymentStatus.DEPLOY_FAILED,
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
    d1_id = DeploymentID(name="d1", app_name="test_app")
    d2_id = DeploymentID(name="d2", app_name="test_app")
    app_state.deploy_app(
        {
            "d1": deployment_info("d1", "/hi"),
            "d2": deployment_info("d2"),
        }
    )
    assert app_state.route_prefix == "/hi"

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
    ready_to_be_deleted, _ = app_state.update()
    assert not ready_to_be_deleted
    assert app_state.status == ApplicationStatus.DELETING

    # Once both deployments are deleted, the app should be ready to delete
    deployment_state_manager.set_deployment_deleted(d2_id)
    ready_to_be_deleted = app_state.update()
    assert ready_to_be_deleted


def test_app_deploy_failed_and_redeploy(mocked_application_state):
    """Test DEPLOYING -> DEPLOY_FAILED -> (redeploy) -> DEPLOYING -> RUNNING"""
    app_state, deployment_state_manager = mocked_application_state
    d1_id = DeploymentID(name="d1", app_name="test_app")
    d2_id = DeploymentID(name="d2", app_name="test_app")
    app_state.deploy_app({"d1": deployment_info("d1")})
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before status of deployment changes, app should still be DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Mark deployment unhealthy -> app should be DEPLOY_FAILED
    deployment_state_manager.set_deployment_deploy_failed(d1_id)
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED

    # Message and status should not change
    deploy_failed_msg = app_state._status_msg
    assert len(deploy_failed_msg) != 0
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED
    assert app_state._status_msg == deploy_failed_msg

    app_state.deploy_app({"d1": deployment_info("d1"), "d2": deployment_info("d2")})
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
    deployment_id = DeploymentID(name="d1", app_name="test_app")
    app_state.deploy_app({"d1": deployment_info("d1")})
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before status of deployment changes, app should still be DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Mark deployment unhealthy -> app should be DEPLOY_FAILED
    deployment_state_manager.set_deployment_deploy_failed(deployment_id)
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
    id_a, id_b = DeploymentID(name="a", app_name="test_app"), DeploymentID(
        name="b", app_name="test_app"
    )
    app_state.deploy_app({"a": deployment_info("a"), "b": deployment_info("b")})
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
@patch("ray.get", Mock(return_value=(None, [deployment_params("a", "/old")], None)))
@patch("ray.serve._private.application_state.check_obj_ref_ready_nowait")
def test_apply_app_configs_succeed(check_obj_ref_ready_nowait):
    """Test deploying through config successfully.
    Deploy obj ref finishes successfully, so status should transition to running.
    """
    kv_store = MockKVStore()
    deployment_id = DeploymentID(name="a", app_name="test_app")
    deployment_state_manager = MockDeploymentStateManager(kv_store)
    app_state_manager = ApplicationStateManager(
        deployment_state_manager,
        AutoscalingStateManager(),
        MockEndpointState(),
        kv_store,
        LoggingConfig(),
    )

    # Deploy config
    app_config = ServeApplicationSchema(
        name="test_app", import_path="fa.ke", route_prefix="/new"
    )
    app_state_manager.apply_app_configs([app_config])
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
def test_apply_app_configs_fail(check_obj_ref_ready_nowait):
    """Test fail to deploy through config.
    Deploy obj ref errors out, so status should transition to deploy failed.
    """
    kv_store = MockKVStore()
    deployment_state_manager = MockDeploymentStateManager(kv_store)
    app_state_manager = ApplicationStateManager(
        deployment_state_manager,
        AutoscalingStateManager(),
        MockEndpointState(),
        kv_store,
        LoggingConfig(),
    )

    # Deploy config
    app_config = ServeApplicationSchema(
        name="test_app", import_path="fa.ke", route_prefix="/new"
    )
    app_state_manager.apply_app_configs([app_config])
    app_state = app_state_manager._application_states["test_app"]
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Before object ref is ready
    check_obj_ref_ready_nowait.return_value = False
    app_state.update()
    assert app_state._build_app_task_info
    assert app_state.status == ApplicationStatus.DEPLOYING
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Object ref is ready, and the task has called deploy_app
    check_obj_ref_ready_nowait.return_value = True
    app_state.update()
    assert app_state.status == ApplicationStatus.DEPLOY_FAILED
    assert "failed" in app_state._status_msg or "error" in app_state._status_msg


@patch(
    "ray.serve._private.application_state.get_app_code_version",
    Mock(return_value="123"),
)
@patch("ray.serve._private.application_state.build_serve_application", Mock())
@patch("ray.get", Mock(return_value=(None, [deployment_params("a", "/old")], None)))
@patch("ray.serve._private.application_state.check_obj_ref_ready_nowait")
def test_apply_app_configs_deletes_existing(check_obj_ref_ready_nowait):
    """Test that apply_app_configs deletes existing apps that aren't in the new list.

    This should *not* apply to apps that were deployed via `deploy_app` (which is
    an imperative API).
    """
    kv_store = MockKVStore()
    deployment_state_manager = MockDeploymentStateManager(kv_store)
    app_state_manager = ApplicationStateManager(
        deployment_state_manager,
        AutoscalingStateManager(),
        MockEndpointState(),
        kv_store,
        LoggingConfig(),
    )

    # Deploy an app via `deploy_app` - should not be affected.
    a_id = DeploymentID(name="a", app_name="imperative_app")
    app_state_manager.deploy_app("imperative_app", [deployment_params("a", "/hi")])
    imperative_app_state = app_state_manager._application_states["imperative_app"]
    assert imperative_app_state.api_type == APIType.IMPERATIVE
    assert imperative_app_state.status == ApplicationStatus.DEPLOYING

    imperative_app_state.update()
    deployment_state_manager.set_deployment_healthy(a_id)
    imperative_app_state.update()
    assert imperative_app_state.status == ApplicationStatus.RUNNING

    # Now deploy an initial version of the config with app 1 and app 2.
    app1_config = ServeApplicationSchema(
        name="app1", import_path="fa.ke", route_prefix="/1"
    )
    app2_config = ServeApplicationSchema(
        name="app2", import_path="fa.ke", route_prefix="/2"
    )
    app_state_manager.apply_app_configs([app1_config, app2_config])
    app1_state = app_state_manager._application_states["app1"]
    assert app1_state.api_type == APIType.DECLARATIVE
    app2_state = app_state_manager._application_states["app2"]
    assert app2_state.api_type == APIType.DECLARATIVE
    app1_state.update()
    app2_state.update()
    assert app1_state.status == ApplicationStatus.DEPLOYING
    assert app2_state.status == ApplicationStatus.DEPLOYING

    # Now redeploy a new config that removes app 1 and adds app 3.
    app3_config = ServeApplicationSchema(
        name="app3", import_path="fa.ke", route_prefix="/3"
    )
    app_state_manager.apply_app_configs([app3_config, app2_config])
    app3_state = app_state_manager._application_states["app3"]
    assert app3_state.api_type == APIType.DECLARATIVE
    app1_state.update()
    app2_state.update()
    app3_state.update()
    assert app1_state.status == ApplicationStatus.DELETING
    assert app2_state.status == ApplicationStatus.DEPLOYING
    assert app3_state.status == ApplicationStatus.DEPLOYING


def test_redeploy_same_app(mocked_application_state):
    """Test redeploying same application with updated deployments."""
    app_state, deployment_state_manager = mocked_application_state
    a_id = DeploymentID(name="a", app_name="test_app")
    b_id = DeploymentID(name="b", app_name="test_app")
    c_id = DeploymentID(name="c", app_name="test_app")
    app_state.deploy_app({"a": deployment_info("a"), "b": deployment_info("b")})
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
    app_state.deploy_app({"b": deployment_info("b"), "c": deployment_info("c")})
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

    app_state_manager.deploy_app("app1", [deployment_params("a", "/hi")])
    with pytest.raises(RayServeException):
        app_state_manager.deploy_app("app2", [deployment_params("b", "/hi")])


def test_deploy_with_renamed_app(mocked_application_state_manager):
    """
    Test that an application deploys successfully when there is a route prefix
    conflict with an old app running on the cluster.
    """
    app_state_manager, deployment_state_manager, _ = mocked_application_state_manager
    a_id, b_id = DeploymentID(name="a", app_name="app1"), DeploymentID(
        name="b", app_name="app2"
    )

    # deploy app1
    app_state_manager.deploy_app("app1", [deployment_params("a", "/url1")])
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
    app_state_manager.delete_app("app1")
    assert app_state_manager.get_app_status("app1") == ApplicationStatus.DELETING
    app_state_manager.update()

    # deploy app2
    app_state_manager.deploy_app("app2", [deployment_params("b", "/url1")])
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
    deployment_id = DeploymentID(name="d1", app_name="test_app")
    app_name = "test_app"

    # DEPLOY application with deployments {d1, d2}
    params = deployment_params("d1")
    app_state_manager.deploy_app(app_name, [params])
    app_state = app_state_manager._application_states[app_name]
    assert app_state.status == ApplicationStatus.DEPLOYING

    # Once deployment is healthy, app should be running
    app_state_manager.update()
    assert deployment_state_manager.get_deployment(deployment_id)
    deployment_state_manager.set_deployment_healthy(deployment_id)
    app_state_manager.update()
    assert app_state.status == ApplicationStatus.RUNNING

    # In real code this checkpoint would be done by the caller of the deploys
    app_state_manager.save_checkpoint()

    # Simulate controller crashed!! Create new deployment state manager,
    # which should recover target state for deployment "d1" from kv store
    new_deployment_state_manager = MockDeploymentStateManager(kv_store)
    version1 = new_deployment_state_manager.deployment_infos[deployment_id].version

    # Create new application state manager, and it should call _recover_from_checkpoint
    new_app_state_manager = ApplicationStateManager(
        new_deployment_state_manager,
        AutoscalingStateManager(),
        MockEndpointState(),
        kv_store,
        LoggingConfig(),
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
    deployment_id = DeploymentID(name="d1", app_name="test_app")
    app_name = "test_app"

    # DEPLOY application with deployment "d1"
    params = deployment_params("d1")
    app_state_manager.deploy_app(app_name, [params])
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
    app_state_manager.deploy_app(app_name, [params2])
    assert app_state.status == ApplicationStatus.DEPLOYING

    # In real code this checkpoint would be done by the caller of the deploys
    app_state_manager.save_checkpoint()

    # Before application state manager could propagate new version to
    # deployment state manager, controller crashes.
    # Create new deployment state manager. It should recover the old
    # version of the deployment from the kv store
    new_deployment_state_manager = MockDeploymentStateManager(kv_store)
    dr_version = new_deployment_state_manager.deployment_infos[deployment_id].version

    # Create new application state manager, and it should call _recover_from_checkpoint
    new_app_state_manager = ApplicationStateManager(
        new_deployment_state_manager,
        AutoscalingStateManager(),
        MockEndpointState(),
        kv_store,
        LoggingConfig(),
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
    deployment_id = DeploymentID(name=deployment_name, app_name=app_name)

    # DEPLOY application with deployment "d1"
    params = deployment_params(deployment_name)
    app_state_manager.deploy_app(app_name, [params])
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
                    max_ongoing_requests=200,
                    user_config={"price": "4"},
                    graceful_shutdown_wait_loop_s=4,
                    graceful_shutdown_timeout_s=40,
                    health_check_period_s=20,
                    health_check_timeout_s=60,
                )
            ],
        )

        updated_infos = override_deployment_info({"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert updated_info.deployment_config.max_ongoing_requests == 200
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

        updated_infos = override_deployment_info({"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert updated_info.deployment_config.autoscaling_config.min_replicas == 1
        assert updated_info.deployment_config.autoscaling_config.initial_replicas == 12
        assert updated_info.deployment_config.autoscaling_config.max_replicas == 79

    def test_override_route_prefix(self, info):
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

        updated_infos = override_deployment_info({"A": info}, config)
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

        updated_infos = override_deployment_info({"A": info}, config)
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

        updated_infos = override_deployment_info({"A": info}, config)
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

        updated_infos = override_deployment_info({"A": info}, config)
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

        updated_infos = override_deployment_info({"A": info}, config)
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

        updated_infos = override_deployment_info({"A": info}, config)
        updated_info = updated_infos["A"]
        assert updated_info.route_prefix == "/"
        assert updated_info.version == "123"
        assert (
            updated_info.replica_config.ray_actor_options["runtime_env"]["working_dir"]
            == "s3://B"
        )


class TestAutoscale:
    def test_autoscale(self, mocked_application_state_manager):
        """Test autoscaling behavior with two deployments under load."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Create autoscaling configuration
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        # Setup: Deploy two deployments
        d1_id, d2_id = self._deploy_test_deployments(
            app_state_manager, deployment_state_manager, autoscaling_config
        )

        # Setup: Register deployments with autoscaling manager
        asm = app_state_manager._autoscaling_state_manager
        self._register_deployments_with_asm(asm, d1_id, d2_id, autoscaling_config)

        # Setup: Create running replicas
        self._create_running_replicas(asm, d1_id, d2_id)

        # Test: Simulate load metrics
        self._simulate_load_metrics(asm, d1_id, d2_id)

        # Verify: Check autoscaling decisions
        app_state_manager.update()
        assert app_state_manager.get_app_status("test_app") == ApplicationStatus.RUNNING
        assert deployment_state_manager._scaling_decisions == {d1_id: 4, d2_id: 2}

    def test_should_autoscale_with_autoscaling_deployments(
        self, mocked_application_state_manager
    ):
        """Test should_autoscale returns True when app has autoscaling deployments."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Create autoscaling configuration
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
        }

        # Deploy app with autoscaling enabled
        d1_id, d2_id = self._deploy_test_deployments(
            app_state_manager, deployment_state_manager, autoscaling_config
        )

        # Register with autoscaling manager
        asm = app_state_manager._autoscaling_state_manager
        self._register_deployments_with_asm(asm, d1_id, d2_id, autoscaling_config)

        # Get the application state
        app_state = app_state_manager._application_states["test_app"]

        # Verify should_autoscale returns True
        assert app_state.should_autoscale() is True

    def test_should_autoscale_without_autoscaling_deployments(
        self, mocked_application_state_manager
    ):
        """Test should_autoscale returns False when app has no autoscaling deployments."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Deploy app without autoscaling configuration
        d1_id = DeploymentID(name="d1", app_name="test_app")
        d1_params = deployment_params("d1", "/hi")  # No autoscaling config

        app_state_manager.deploy_app("test_app", [d1_params])
        app_state_manager.update()
        deployment_state_manager.set_deployment_healthy(d1_id)
        app_state_manager.update()

        # Get the application state
        app_state = app_state_manager._application_states["test_app"]

        # Verify should_autoscale returns False
        assert app_state.should_autoscale() is False

    def test_autoscale_with_no_deployments(self, mocked_application_state_manager):
        """Test autoscale returns False when app has no target deployments."""
        app_state_manager, _, _ = mocked_application_state_manager

        # Create app state without any deployments
        app_state = ApplicationState(
            name="empty_app",
            deployment_state_manager=MockDeploymentStateManager(MockKVStore()),
            autoscaling_state_manager=AutoscalingStateManager(),
            endpoint_state=MockEndpointState(),
            logging_config=LoggingConfig(),
        )

        # Verify autoscale returns False
        assert app_state.autoscale() is False

    def test_autoscale_with_deployment_details_none(
        self, mocked_application_state_manager
    ):
        """Test autoscale handles None deployment details gracefully."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Deploy app with autoscaling
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
        }

        d1_id, d2_id = self._deploy_test_deployments(
            app_state_manager, deployment_state_manager, autoscaling_config
        )

        # Mock get_deployment_target_num_replicas to return None
        deployment_state_manager.get_deployment_target_num_replicas = Mock(
            return_value=None
        )

        app_state = app_state_manager._application_states["test_app"]

        # Verify autoscale returns False when deployment details are None
        assert app_state.autoscale() is False

    def test_autoscale_applies_decisions_correctly(
        self, mocked_application_state_manager
    ):
        """Test autoscale applies autoscaling decisions to deployment state manager."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Deploy app with autoscaling
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        d1_id, d2_id = self._deploy_test_deployments(
            app_state_manager, deployment_state_manager, autoscaling_config
        )

        # Register with autoscaling manager and create replicas
        asm = app_state_manager._autoscaling_state_manager
        self._register_deployments_with_asm(asm, d1_id, d2_id, autoscaling_config)
        self._create_running_replicas(asm, d1_id, d2_id)

        # Simulate load: d1 has 3x target load, d2 has 0.5x target load
        self._simulate_load_metrics(asm, d1_id, d2_id, d1_load=3, d2_load=0)

        app_state = app_state_manager._application_states["test_app"]

        # Call autoscale
        result = app_state.autoscale()

        # Verify it returns True (target state changed)
        assert result is True

        # Verify scaling decisions were applied
        # d1 should scale up (high load), d2 should scale down (low load)
        assert d1_id in deployment_state_manager._scaling_decisions
        assert d2_id in deployment_state_manager._scaling_decisions

    def test_autoscale_no_decisions_returns_false(
        self, mocked_application_state_manager
    ):
        """Test autoscale returns False when no autoscaling decisions are made."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Deploy app with autoscaling
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        d1_id, d2_id = self._deploy_test_deployments(
            app_state_manager, deployment_state_manager, autoscaling_config
        )

        # Register with autoscaling manager and create replicas
        asm = app_state_manager._autoscaling_state_manager
        self._register_deployments_with_asm(asm, d1_id, d2_id, autoscaling_config)
        self._create_running_replicas(asm, d1_id, d2_id)

        # Simulate balanced load (exactly at target, so no scaling needed)
        self._simulate_load_metrics(asm, d1_id, d2_id, d1_load=1, d2_load=1)

        app_state = app_state_manager._application_states["test_app"]

        # Call autoscale
        result = app_state.autoscale()

        # Verify it returns False (no scaling decisions needed)
        # When load exactly matches target, autoscaler shouldn't make changes
        assert result is False or deployment_state_manager._scaling_decisions == {
            d1_id: 2,
            d2_id: 2,
        }

    def test_application_state_manager_autoscaling_integration(
        self, mocked_application_state_manager
    ):
        """Test autoscaling integration in ApplicationStateManager.update()."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Deploy app with autoscaling
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        d1_id, d2_id = self._deploy_test_deployments(
            app_state_manager, deployment_state_manager, autoscaling_config
        )

        # Register with autoscaling manager and create replicas
        asm = app_state_manager._autoscaling_state_manager
        self._register_deployments_with_asm(asm, d1_id, d2_id, autoscaling_config)
        self._create_running_replicas(asm, d1_id, d2_id)

        # Simulate high load on d1, moderate load on d2
        self._simulate_load_metrics(asm, d1_id, d2_id, d1_load=4, d2_load=2)

        # Clear any existing scaling decisions
        deployment_state_manager._scaling_decisions.clear()

        # Call ApplicationStateManager.update()
        app_state_manager.update()

        # Verify autoscaling decisions were applied during update
        # Both deployments should have scaling decisions due to load
        assert len(deployment_state_manager._scaling_decisions) > 0

    def test_autoscaling_with_mixed_deployment_types(
        self, mocked_application_state_manager
    ):
        """Test autoscaling behavior with mix of autoscaling and non-autoscaling deployments."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Deploy app with one autoscaling and one non-autoscaling deployment
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        d1_id = DeploymentID(name="d1", app_name="test_app")
        d2_id = DeploymentID(name="d2", app_name="test_app")

        # d1 has autoscaling, d2 doesn't
        d1_params = deployment_params(
            "d1", "/hi", autoscaling_config=autoscaling_config
        )
        d2_params = deployment_params("d2")  # No autoscaling config

        app_state_manager.deploy_app("test_app", [d1_params, d2_params])
        app_state_manager.update()

        deployment_state_manager.set_deployment_healthy(d1_id)
        deployment_state_manager.set_deployment_healthy(d2_id)
        app_state_manager.update()

        # Register only d1 with autoscaling manager and create replicas
        asm = app_state_manager._autoscaling_state_manager
        d1_info = deployment_info("d1", "/hi", autoscaling_config=autoscaling_config)
        asm.register_deployment(d1_id, d1_info, 1)

        # Create replicas for d1 only
        d1_replicas = [
            ReplicaID(unique_id=f"replica_{i}", deployment_id=d1_id) for i in [1, 2]
        ]
        asm.update_running_replica_ids(d1_id, d1_replicas)

        # Simulate high load on d1
        current_time = time.time()
        timestamp_offset = current_time - 0.1

        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
            d1_handle_report = HandleMetricReport(
                deployment_id=d1_id,
                handle_id="random",
                actor_id="actor_id",
                handle_source=DeploymentHandleSource.UNKNOWN,
                queued_requests=[TimeStampedValue(timestamp_offset, 0)],
                aggregated_queued_requests=0,
                aggregated_metrics={
                    RUNNING_REQUESTS_KEY: {
                        ReplicaID(unique_id="replica_1", deployment_id=d1_id): 3,
                        ReplicaID(unique_id="replica_2", deployment_id=d1_id): 3,
                    }
                },
                metrics={
                    RUNNING_REQUESTS_KEY: {
                        ReplicaID(unique_id="replica_1", deployment_id=d1_id): [
                            TimeStampedValue(timestamp_offset, 3)
                        ],
                        ReplicaID(unique_id="replica_2", deployment_id=d1_id): [
                            TimeStampedValue(timestamp_offset, 3)
                        ],
                    }
                },
                timestamp=time.time(),
            )
            asm.record_request_metrics_for_handle(d1_handle_report)
        else:
            for i in [1, 2]:
                replica_report = ReplicaMetricReport(
                    replica_id=ReplicaID(unique_id=f"replica_{i}", deployment_id=d1_id),
                    aggregated_metrics={RUNNING_REQUESTS_KEY: 3},
                    metrics={
                        RUNNING_REQUESTS_KEY: [TimeStampedValue(timestamp_offset, 3)]
                    },
                    timestamp=time.time(),
                )
                asm.record_request_metrics_for_replica(replica_report)

        app_state = app_state_manager._application_states["test_app"]

        # Call autoscale
        result = app_state.autoscale()

        # Verify only d1's decision was applied (d2 has no autoscaling)
        assert result is True
        assert d1_id in deployment_state_manager._scaling_decisions
        assert d2_id not in deployment_state_manager._scaling_decisions

    def test_autoscale_multiple_apps_independent(
        self, mocked_application_state_manager
    ):
        """Test that autoscaling decisions for one app don't affect another app."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Create autoscaling configuration
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        # Deploy app1 with two deployments
        app1_d1_id = DeploymentID(name="d1", app_name="app1")
        app1_d2_id = DeploymentID(name="d2", app_name="app1")
        app1_d1_params = deployment_params(
            "d1", "/app1", autoscaling_config=autoscaling_config
        )
        app1_d2_params = deployment_params("d2", autoscaling_config=autoscaling_config)

        app_state_manager.deploy_app("app1", [app1_d1_params, app1_d2_params])
        app_state_manager.update()
        deployment_state_manager.set_deployment_healthy(app1_d1_id)
        deployment_state_manager.set_deployment_healthy(app1_d2_id)
        app_state_manager.update()

        # Deploy app2 with two deployments
        app2_d1_id = DeploymentID(name="d1", app_name="app2")
        app2_d2_id = DeploymentID(name="d2", app_name="app2")
        app2_d1_params = deployment_params(
            "d1", "/app2", autoscaling_config=autoscaling_config
        )
        app2_d2_params = deployment_params("d2", autoscaling_config=autoscaling_config)

        app_state_manager.deploy_app("app2", [app2_d1_params, app2_d2_params])
        app_state_manager.update()
        deployment_state_manager.set_deployment_healthy(app2_d1_id)
        deployment_state_manager.set_deployment_healthy(app2_d2_id)
        app_state_manager.update()

        # Register app1 deployments with autoscaling manager
        asm = app_state_manager._autoscaling_state_manager
        app1_d1_info = deployment_info(
            "d1", "/app1", autoscaling_config=autoscaling_config
        )
        app1_d2_info = deployment_info("d2", autoscaling_config=autoscaling_config)
        app1_d1_info.app_name = "app1"
        app1_d2_info.app_name = "app1"
        asm.register_deployment(app1_d1_id, app1_d1_info, 1)
        asm.register_deployment(app1_d2_id, app1_d2_info, 1)

        # Register app2 deployments with autoscaling manager
        app2_d1_info = deployment_info(
            "d1", "/app2", autoscaling_config=autoscaling_config
        )
        app2_d2_info = deployment_info("d2", autoscaling_config=autoscaling_config)
        app2_d1_info.app_name = "app2"
        app2_d2_info.app_name = "app2"
        asm.register_deployment(app2_d1_id, app2_d1_info, 1)
        asm.register_deployment(app2_d2_id, app2_d2_info, 1)

        # Create replicas for both apps
        app1_d1_replicas = [
            ReplicaID(unique_id=f"app1_d1_replica_{i}", deployment_id=app1_d1_id)
            for i in [1, 2]
        ]
        app1_d2_replicas = [
            ReplicaID(unique_id=f"app1_d2_replica_{i}", deployment_id=app1_d2_id)
            for i in [3, 4]
        ]
        asm.update_running_replica_ids(app1_d1_id, app1_d1_replicas)
        asm.update_running_replica_ids(app1_d2_id, app1_d2_replicas)

        app2_d1_replicas = [
            ReplicaID(unique_id=f"app2_d1_replica_{i}", deployment_id=app2_d1_id)
            for i in [5, 6]
        ]
        app2_d2_replicas = [
            ReplicaID(unique_id=f"app2_d2_replica_{i}", deployment_id=app2_d2_id)
            for i in [7, 8]
        ]
        asm.update_running_replica_ids(app2_d1_id, app2_d1_replicas)
        asm.update_running_replica_ids(app2_d2_id, app2_d2_replicas)

        # Simulate high load on app1, low load on app2
        current_time = time.time()
        timestamp_offset = current_time - 0.1

        # App1: High load
        for replica_id in app1_d1_replicas + app1_d2_replicas:
            replica_report = ReplicaMetricReport(
                replica_id=replica_id,
                aggregated_metrics={RUNNING_REQUESTS_KEY: 3},
                metrics={RUNNING_REQUESTS_KEY: [TimeStampedValue(timestamp_offset, 3)]},
                timestamp=time.time(),
            )
            asm.record_request_metrics_for_replica(replica_report)

        # App2: Low load
        for replica_id in app2_d1_replicas + app2_d2_replicas:
            replica_report = ReplicaMetricReport(
                replica_id=replica_id,
                aggregated_metrics={RUNNING_REQUESTS_KEY: 0},
                metrics={RUNNING_REQUESTS_KEY: [TimeStampedValue(timestamp_offset, 0)]},
                timestamp=time.time(),
            )
            asm.record_request_metrics_for_replica(replica_report)

        # Clear scaling decisions
        deployment_state_manager._scaling_decisions.clear()

        # Call update which triggers autoscaling for both apps
        app_state_manager.update()

        # Verify app1 deployments scaled up (high load)
        assert app1_d1_id in deployment_state_manager._scaling_decisions
        assert app1_d2_id in deployment_state_manager._scaling_decisions
        assert deployment_state_manager._scaling_decisions[app1_d1_id] > 2

        # Verify app2 deployments scaled down (low load)
        assert app2_d1_id in deployment_state_manager._scaling_decisions
        assert app2_d2_id in deployment_state_manager._scaling_decisions
        assert deployment_state_manager._scaling_decisions[app2_d1_id] == 1

    def test_autoscale_with_partial_deployment_details(
        self, mocked_application_state_manager
    ):
        """Test autoscale when some deployments have details and others return None."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Deploy app with autoscaling
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        d1_id, d2_id = self._deploy_test_deployments(
            app_state_manager, deployment_state_manager, autoscaling_config
        )

        # Register only d1 with autoscaling manager (d2 won't be registered)
        asm = app_state_manager._autoscaling_state_manager
        d1_info = deployment_info("d1", "/hi", autoscaling_config=autoscaling_config)
        asm.register_deployment(d1_id, d1_info, 1)

        # Create replicas for d1 only
        d1_replicas = [
            ReplicaID(unique_id=f"d1_replica_{i}", deployment_id=d1_id) for i in [1, 2]
        ]
        asm.update_running_replica_ids(d1_id, d1_replicas)

        # Simulate load for d1
        current_time = time.time()
        timestamp_offset = current_time - 0.1
        for i in [1, 2]:
            replica_report = ReplicaMetricReport(
                replica_id=ReplicaID(unique_id=f"d1_replica_{i}", deployment_id=d1_id),
                aggregated_metrics={RUNNING_REQUESTS_KEY: 3},
                metrics={RUNNING_REQUESTS_KEY: [TimeStampedValue(timestamp_offset, 3)]},
                timestamp=time.time(),
            )
            asm.record_request_metrics_for_replica(replica_report)

        # Mock get_deployment_target_num_replicas to return None for d2 only
        original_get_details = (
            deployment_state_manager.get_deployment_target_num_replicas
        )

        def selective_get_details(dep_id) -> Optional[int]:
            if dep_id == d2_id:
                return None
            return original_get_details(dep_id)

        deployment_state_manager.get_deployment_target_num_replicas = (
            selective_get_details
        )

        app_state = app_state_manager._application_states["test_app"]

        # Call autoscale
        result = app_state.autoscale()

        # Verify it returns True (d1 has scaling decision)
        assert result is True

        # Verify only d1 has scaling decision (d2 was skipped due to None details)
        assert d1_id in deployment_state_manager._scaling_decisions
        assert d2_id not in deployment_state_manager._scaling_decisions

    def test_autoscale_single_deployment_in_app(self, mocked_application_state_manager):
        """Test autoscaling with only one deployment in the app."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Create autoscaling configuration
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        # Deploy single deployment
        d1_id = DeploymentID(name="d1", app_name="test_app")
        d1_params = deployment_params(
            "d1", "/hi", autoscaling_config=autoscaling_config
        )

        app_state_manager.deploy_app("test_app", [d1_params])
        app_state_manager.update()
        deployment_state_manager.set_deployment_healthy(d1_id)
        app_state_manager.update()

        # Register with autoscaling manager
        asm = app_state_manager._autoscaling_state_manager
        d1_info = deployment_info("d1", "/hi", autoscaling_config=autoscaling_config)
        asm.register_deployment(d1_id, d1_info, 1)

        # Create replicas
        d1_replicas = [
            ReplicaID(unique_id=f"replica_{i}", deployment_id=d1_id) for i in [1, 2]
        ]
        asm.update_running_replica_ids(d1_id, d1_replicas)

        # Simulate high load
        current_time = time.time()
        timestamp_offset = current_time - 0.1
        for i in [1, 2]:
            replica_report = ReplicaMetricReport(
                replica_id=ReplicaID(unique_id=f"replica_{i}", deployment_id=d1_id),
                aggregated_metrics={RUNNING_REQUESTS_KEY: 4},
                metrics={RUNNING_REQUESTS_KEY: [TimeStampedValue(timestamp_offset, 4)]},
                timestamp=time.time(),
            )
            asm.record_request_metrics_for_replica(replica_report)

        app_state = app_state_manager._application_states["test_app"]

        # Call autoscale
        result = app_state.autoscale()

        # Verify it returns True
        assert result is True

        # Verify scaling decision was made
        assert d1_id in deployment_state_manager._scaling_decisions
        assert deployment_state_manager._scaling_decisions[d1_id] > 2

    def test_autoscale_during_app_deletion(self, mocked_application_state_manager):
        """Test autoscaling behavior when app is being deleted."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Deploy app with autoscaling
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        d1_id, d2_id = self._deploy_test_deployments(
            app_state_manager, deployment_state_manager, autoscaling_config
        )

        # Register with autoscaling manager and create replicas
        asm = app_state_manager._autoscaling_state_manager
        self._register_deployments_with_asm(asm, d1_id, d2_id, autoscaling_config)
        self._create_running_replicas(asm, d1_id, d2_id)

        # Simulate load
        self._simulate_load_metrics(asm, d1_id, d2_id, d1_load=5, d2_load=5)

        # Delete the app
        app_state_manager.delete_app("test_app")

        # Get app state
        app_state = app_state_manager._application_states["test_app"]

        # Verify app status is DELETING
        assert app_state.status == ApplicationStatus.DELETING

        # Clear scaling decisions
        deployment_state_manager._scaling_decisions.clear()

        # Call update (should not autoscale deleting apps)
        app_state_manager.update()

        # Verify no autoscaling decisions were made (app is deleting)
        assert len(deployment_state_manager._scaling_decisions) == 0

    def test_autoscale_many_deployments_in_app(self, mocked_application_state_manager):
        """Test autoscaling with many (15+) deployments in single app."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Create autoscaling configuration
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 3,
            "initial_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        # Deploy 15 deployments
        num_deployments = 15
        deployment_ids = []
        deployment_params_list = []

        for i in range(num_deployments):
            deployment_ids.append(DeploymentID(name=f"d{i}", app_name="test_app"))
            deployment_params_list.append(
                deployment_params(f"d{i}", autoscaling_config=autoscaling_config)
            )

        app_state_manager.deploy_app("test_app", deployment_params_list)
        app_state_manager.update()

        # Mark all as healthy
        for dep_id in deployment_ids:
            deployment_state_manager.set_deployment_healthy(dep_id)
        app_state_manager.update()

        # Register all with autoscaling manager
        asm = app_state_manager._autoscaling_state_manager
        for i, dep_id in enumerate(deployment_ids):
            info = deployment_info(f"d{i}", autoscaling_config=autoscaling_config)
            asm.register_deployment(dep_id, info, 1)

            # Create replicas
            replicas = [
                ReplicaID(unique_id=f"d{i}_replica_{j}", deployment_id=dep_id)
                for j in [1, 2]
            ]
            asm.update_running_replica_ids(dep_id, replicas)

            # Simulate load (alternating high/low)
            load = 3 if i % 2 == 0 else 0
            current_time = time.time()
            timestamp_offset = current_time - 0.1
            for replica in replicas:
                replica_report = ReplicaMetricReport(
                    replica_id=replica,
                    aggregated_metrics={RUNNING_REQUESTS_KEY: load},
                    metrics={
                        RUNNING_REQUESTS_KEY: [TimeStampedValue(timestamp_offset, load)]
                    },
                    timestamp=time.time(),
                )
                asm.record_request_metrics_for_replica(replica_report)

        # Clear scaling decisions
        deployment_state_manager._scaling_decisions.clear()

        # Call update
        app_state_manager.update()

        # Verify all deployments have scaling decisions
        assert len(deployment_state_manager._scaling_decisions) == num_deployments

        # Verify high-load deployments scaled up
        for i in range(0, num_deployments, 2):  # Even indices have high load
            assert deployment_state_manager._scaling_decisions[deployment_ids[i]] == 3

        # Verify low-load deployments scaled down
        for i in range(1, num_deployments, 2):  # Odd indices have low load
            assert deployment_state_manager._scaling_decisions[deployment_ids[i]] == 1

    def test_autoscale_with_min_equals_max_replicas(
        self, mocked_application_state_manager
    ):
        """Test autoscaling when min_replicas equals max_replicas (no room to scale)."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Create autoscaling configuration with no scaling room
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 3,
            "max_replicas": 3,  # Same as min
            "initial_replicas": 3,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        d1_id = DeploymentID(name="d1", app_name="test_app")
        d1_params = deployment_params(
            "d1", "/hi", autoscaling_config=autoscaling_config
        )

        app_state_manager.deploy_app("test_app", [d1_params])
        app_state_manager.update()
        deployment_state_manager.set_deployment_healthy(d1_id)
        app_state_manager.update()

        # Register with autoscaling manager
        asm = app_state_manager._autoscaling_state_manager
        d1_info = deployment_info("d1", "/hi", autoscaling_config=autoscaling_config)
        asm.register_deployment(d1_id, d1_info, 3)

        # Create replicas
        d1_replicas = [
            ReplicaID(unique_id=f"replica_{i}", deployment_id=d1_id) for i in range(3)
        ]
        asm.update_running_replica_ids(d1_id, d1_replicas)

        # Simulate extreme load (should want to scale up but can't)
        current_time = time.time()
        timestamp_offset = current_time - 0.1
        for i in range(3):
            replica_report = ReplicaMetricReport(
                replica_id=ReplicaID(unique_id=f"replica_{i}", deployment_id=d1_id),
                aggregated_metrics={RUNNING_REQUESTS_KEY: 10},
                metrics={
                    RUNNING_REQUESTS_KEY: [TimeStampedValue(timestamp_offset, 10)]
                },
                timestamp=time.time(),
            )
            asm.record_request_metrics_for_replica(replica_report)

        app_state = app_state_manager._application_states["test_app"]

        # Call autoscale
        _ = app_state.autoscale()

        # Decision should be made but capped at max_replicas (3)
        assert d1_id in deployment_state_manager._scaling_decisions
        assert deployment_state_manager._scaling_decisions[d1_id] == 3

    def test_autoscale_multiple_updates_stable_load(
        self, mocked_application_state_manager
    ):
        """Test multiple update() calls with stable load don't cause thrashing."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Setup: Deploy app with autoscaling
        autoscaling_config = {
            "target_ongoing_requests": 1,
            "min_replicas": 1,
            "max_replicas": 5,
            "initial_replicas": 2,
            "upscale_delay_s": 0,
            "downscale_delay_s": 0,
            "metrics_interval_s": 0.1,
        }

        d1_id, d2_id = self._deploy_test_deployments(
            app_state_manager, deployment_state_manager, autoscaling_config
        )

        # Register with autoscaling manager and create replicas
        asm = app_state_manager._autoscaling_state_manager
        self._register_deployments_with_asm(asm, d1_id, d2_id, autoscaling_config)
        self._create_running_replicas(asm, d1_id, d2_id)

        # Simulate stable load at target
        self._simulate_load_metrics(asm, d1_id, d2_id, d1_load=1, d2_load=1)

        # Clear scaling decisions
        deployment_state_manager._scaling_decisions.clear()

        # Call update multiple times
        for _ in range(5):
            app_state_manager.update()

        # Verify decisions are stable (should be 2 replicas - no change)
        # If decisions keep changing, that's thrashing
        if deployment_state_manager._scaling_decisions:
            assert deployment_state_manager._scaling_decisions.get(d1_id, 2) == 2
            assert deployment_state_manager._scaling_decisions.get(d2_id, 2) == 2

    def _deploy_test_deployments(
        self, app_state_manager, deployment_state_manager, autoscaling_config
    ):
        """Deploy two test deployments and mark them as healthy."""
        d1_id = DeploymentID(name="d1", app_name="test_app")
        d2_id = DeploymentID(name="d2", app_name="test_app")

        d1_params = deployment_params(
            "d1", "/hi", autoscaling_config=autoscaling_config
        )
        d2_params = deployment_params("d2", autoscaling_config=autoscaling_config)

        app_state_manager.deploy_app("test_app", [d1_params, d2_params])
        app_state_manager.update()

        deployment_state_manager.set_deployment_healthy(d1_id)
        deployment_state_manager.set_deployment_healthy(d2_id)
        app_state_manager.update()

        assert app_state_manager.get_app_status("test_app") == ApplicationStatus.RUNNING
        return d1_id, d2_id

    def _register_deployments_with_asm(self, asm, d1_id, d2_id, autoscaling_config):
        """Register deployments with the autoscaling state manager."""
        d1_info = deployment_info("d1", "/hi", autoscaling_config=autoscaling_config)
        d2_info = deployment_info("d2", autoscaling_config=autoscaling_config)

        asm.register_deployment(d1_id, d1_info, 1)
        asm.register_deployment(d2_id, d2_info, 1)

    def _create_running_replicas(self, asm, d1_id, d2_id):
        """Create running replicas for both deployments."""
        # d1 gets 2 replicas
        d1_replicas = [
            ReplicaID(unique_id=f"replica_{i}", deployment_id=d1_id) for i in [1, 2]
        ]
        asm.update_running_replica_ids(d1_id, d1_replicas)

        # d2 gets 2 replicas
        d2_replicas = [
            ReplicaID(unique_id=f"replica_{i}", deployment_id=d2_id) for i in [3, 4]
        ]
        asm.update_running_replica_ids(d2_id, d2_replicas)

    def _simulate_load_metrics(self, asm, d1_id, d2_id, d1_load=2, d2_load=1):
        current_time = time.time()
        timestamp_offset = current_time - 0.1

        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
            self._record_handle_metrics(
                asm, d1_id, d2_id, timestamp_offset, d1_load, d2_load
            )
        else:
            self._record_replica_metrics(
                asm, d1_id, d2_id, timestamp_offset, d1_load, d2_load
            )

    def _record_handle_metrics(
        self, asm, d1_id, d2_id, timestamp_offset, d1_load=2, d2_load=1
    ):
        """Record metrics using handle-based reporting."""
        # d1: Load based on d1_load parameter
        d1_handle_report = HandleMetricReport(
            deployment_id=d1_id,
            handle_id="random",
            actor_id="actor_id",
            handle_source=DeploymentHandleSource.UNKNOWN,
            queued_requests=[TimeStampedValue(timestamp_offset, 0)],
            aggregated_queued_requests=0,
            aggregated_metrics={
                RUNNING_REQUESTS_KEY: {
                    ReplicaID(unique_id="replica_1", deployment_id=d1_id): d1_load,
                    ReplicaID(unique_id="replica_2", deployment_id=d1_id): d1_load,
                }
            },
            metrics={
                RUNNING_REQUESTS_KEY: {
                    ReplicaID(unique_id="replica_1", deployment_id=d1_id): [
                        TimeStampedValue(timestamp_offset, d1_load)
                    ],
                    ReplicaID(unique_id="replica_2", deployment_id=d1_id): [
                        TimeStampedValue(timestamp_offset, d1_load)
                    ],
                }
            },
            timestamp=time.time(),
        )
        asm.record_request_metrics_for_handle(d1_handle_report)

        # d2: Load based on d2_load parameter
        d2_handle_report = HandleMetricReport(
            deployment_id=d2_id,
            handle_id="random",
            actor_id="actor_id",
            handle_source=DeploymentHandleSource.UNKNOWN,
            queued_requests=[TimeStampedValue(timestamp_offset, 0)],
            aggregated_queued_requests=0,
            aggregated_metrics={
                RUNNING_REQUESTS_KEY: {
                    ReplicaID(unique_id="replica_3", deployment_id=d2_id): d2_load,
                    ReplicaID(unique_id="replica_4", deployment_id=d2_id): d2_load,
                }
            },
            metrics={
                RUNNING_REQUESTS_KEY: {
                    ReplicaID(unique_id="replica_3", deployment_id=d2_id): [
                        TimeStampedValue(timestamp_offset, d2_load)
                    ],
                    ReplicaID(unique_id="replica_4", deployment_id=d2_id): [
                        TimeStampedValue(timestamp_offset, d2_load)
                    ],
                }
            },
            timestamp=time.time(),
        )
        asm.record_request_metrics_for_handle(d2_handle_report)

    def _record_replica_metrics(
        self, asm, d1_id, d2_id, timestamp_offset, d1_load=2, d2_load=1
    ):
        """Record metrics using replica-based reporting."""
        # d1: Load based on d1_load parameter
        for i in [1, 2]:
            replica_report = ReplicaMetricReport(
                replica_id=ReplicaID(unique_id=f"replica_{i}", deployment_id=d1_id),
                aggregated_metrics={RUNNING_REQUESTS_KEY: d1_load},
                metrics={
                    RUNNING_REQUESTS_KEY: [TimeStampedValue(timestamp_offset, d1_load)]
                },
                timestamp=time.time(),
            )
            asm.record_request_metrics_for_replica(replica_report)

        # d2: Load based on d2_load parameter
        for i in [3, 4]:
            replica_report = ReplicaMetricReport(
                replica_id=ReplicaID(unique_id=f"replica_{i}", deployment_id=d2_id),
                aggregated_metrics={RUNNING_REQUESTS_KEY: d2_load},
                metrics={
                    RUNNING_REQUESTS_KEY: [TimeStampedValue(timestamp_offset, d2_load)]
                },
                timestamp=time.time(),
            )
            asm.record_request_metrics_for_replica(replica_report)


def simple_app_level_policy(contexts):
    """Simple policy that scales all deployments to 3 replicas."""
    decisions = {}
    for deployment_id, _ in contexts.items():
        decisions[deployment_id] = 3
    return decisions, {}


class TestApplicationLevelAutoscaling:
    """Test application-level autoscaling policy registration, execution, and lifecycle."""

    def _create_app_config(
        self, app_name="test_app", has_policy=True, deployments=None
    ):
        """Helper to create a ServeApplicationSchema with optional autoscaling policy."""
        if deployments is None:
            deployments = [
                DeploymentSchema(
                    name="d1",
                    autoscaling_config={
                        "target_ongoing_requests": 1,
                        "min_replicas": 1,
                        "max_replicas": 5,
                        "initial_replicas": 1,
                    },
                )
            ]

        return ServeApplicationSchema(
            name=app_name,
            import_path="fake.import.path",
            route_prefix="/hi",
            autoscaling_policy={
                "policy_function": "ray.serve.tests.unit.test_application_state:simple_app_level_policy"
            }
            if has_policy
            else None,
            deployments=deployments,
        )

    def _deploy_app_with_mocks(self, app_state_manager, app_config):
        """Helper to deploy an app with proper mocking to avoid Ray initialization."""
        with patch(
            "ray.serve._private.application_state.build_serve_application"
        ) as mock_build:
            mock_build.return_value = Mock()
            app_state_manager.apply_app_configs([app_config])

        app_state = app_state_manager._application_states[app_config.name]
        app_state._build_app_task_info = Mock()
        app_state._build_app_task_info.code_version = "test_version"
        app_state._build_app_task_info.config = app_config
        app_state._build_app_task_info.target_capacity = None
        app_state._build_app_task_info.target_capacity_direction = None

        # Mock reconcile to succeed
        with patch.object(app_state, "_reconcile_build_app_task") as mock_reconcile:
            deployment_infos = {}
            for deployment in app_config.deployments:
                deployment_infos[deployment.name] = deployment_info(
                    deployment.name,
                    "/hi" if deployment.name == "d1" else None,
                    autoscaling_config={
                        "target_ongoing_requests": 1,
                        "min_replicas": 1,
                        "max_replicas": 5,
                        "initial_replicas": 1,
                    },
                )

            mock_reconcile.return_value = (
                None,
                deployment_infos,
                BuildAppStatus.SUCCEEDED,
                "",
            )
            app_state.update()

        return app_state

    def _register_deployments(self, app_state_manager, app_config):
        """Helper to register deployments with autoscaling manager."""
        asm = app_state_manager._autoscaling_state_manager
        for deployment in app_config.deployments:
            deployment_id = DeploymentID(name=deployment.name, app_name=app_config.name)
            deployment_info_obj = deployment_info(
                deployment.name,
                "/hi" if deployment.name == "d1" else None,
                autoscaling_config={
                    "target_ongoing_requests": 1,
                    "min_replicas": 1,
                    "max_replicas": 5,
                    "initial_replicas": 1,
                },
            )
            asm.register_deployment(deployment_id, deployment_info_obj, 1)
        return asm

    def _deploy_multiple_apps_with_mocks(self, app_state_manager, app_configs):
        """Helper to deploy multiple apps simultaneously with proper mocking."""
        # Deploy all apps at once
        with patch(
            "ray.serve._private.application_state.build_serve_application"
        ) as mock_build:
            mock_build.return_value = Mock()
            app_state_manager.apply_app_configs(app_configs)

        # Mock the build app tasks for all apps
        for app_config in app_configs:
            app_state = app_state_manager._application_states[app_config.name]
            app_state._build_app_task_info = Mock()
            app_state._build_app_task_info.code_version = "test_version"
            app_state._build_app_task_info.config = app_config
            app_state._build_app_task_info.target_capacity = None
            app_state._build_app_task_info.target_capacity_direction = None

            # Mock reconcile to succeed
            with patch.object(app_state, "_reconcile_build_app_task") as mock_reconcile:
                deployment_infos = {}
                for deployment in app_config.deployments:
                    deployment_infos[deployment.name] = deployment_info(
                        deployment.name,
                        "/hi" if deployment.name == "d1" else None,
                        autoscaling_config={
                            "target_ongoing_requests": 1,
                            "min_replicas": 1,
                            "max_replicas": 5,
                            "initial_replicas": 1,
                        },
                    )

                mock_reconcile.return_value = (
                    None,
                    deployment_infos,
                    BuildAppStatus.SUCCEEDED,
                    "",
                )
                app_state.update()

        return app_state_manager._autoscaling_state_manager

    def test_app_level_autoscaling_policy_registration_and_execution(
        self, mocked_application_state_manager
    ):
        """Test that application-level autoscaling policy is registered and executed when set in config."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Create app config with policy
        app_config = self._create_app_config()

        # Deploy app
        app_state = self._deploy_app_with_mocks(app_state_manager, app_config)

        # Register deployments
        asm = self._register_deployments(app_state_manager, app_config)

        # Verify policy was registered
        assert asm._application_has_policy("test_app") is True
        assert app_state.should_autoscale() is True
        assert asm.should_autoscale_application("test_app") is True

        # Create replicas and test autoscaling
        d1_id = DeploymentID(name="d1", app_name="test_app")
        d1_replicas = [
            ReplicaID(unique_id=f"d1_replica_{i}", deployment_id=d1_id) for i in [1, 2]
        ]
        asm.update_running_replica_ids(d1_id, d1_replicas)

        # Clear scaling decisions and test autoscaling
        deployment_state_manager._scaling_decisions.clear()

        app_state_manager.update()

        # Verify policy was executed (scales to 3 replicas)
        assert deployment_state_manager._scaling_decisions[d1_id] == 3

    def test_app_level_autoscaling_policy_recovery(
        self, mocked_application_state_manager
    ):
        """Test that application-level autoscaling policy is registered when recovered from checkpoint."""
        (
            app_state_manager,
            deployment_state_manager,
            kv_store,
        ) = mocked_application_state_manager

        # Deploy app with policy
        app_config = self._create_app_config()
        _ = self._deploy_app_with_mocks(app_state_manager, app_config)
        asm = self._register_deployments(app_state_manager, app_config)

        # Save checkpoint
        app_state_manager.update()

        # Simulate controller crash - create new managers
        new_deployment_state_manager = MockDeploymentStateManager(kv_store)
        new_app_state_manager = ApplicationStateManager(
            new_deployment_state_manager,
            asm,
            MockEndpointState(),
            kv_store,
            LoggingConfig(),
        )

        # Recovery happens automatically during initialization
        # Verify app-level policy was recovered
        assert asm._application_has_policy("test_app") is True

        # Test that recovered policy still works
        d1_id = DeploymentID(name="d1", app_name="test_app")
        d1_replicas = [
            ReplicaID(unique_id=f"d1_replica_{i}", deployment_id=d1_id) for i in [1, 2]
        ]
        asm.update_running_replica_ids(d1_id, d1_replicas)

        new_deployment_state_manager._scaling_decisions.clear()
        new_app_state_manager.update()

        assert new_deployment_state_manager._scaling_decisions[d1_id] == 3

    def test_app_level_autoscaling_policy_deregistration_on_deletion(
        self, mocked_application_state_manager
    ):
        """Test that application-level autoscaling policy is deregistered when application is deleted."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Deploy app with policy
        app_config = self._create_app_config()
        _ = self._deploy_app_with_mocks(app_state_manager, app_config)
        asm = self._register_deployments(app_state_manager, app_config)

        # Verify app is registered
        assert asm._application_has_policy("test_app") is True

        # Delete the application
        deployment_state_manager.delete_deployment(
            DeploymentID(name="d1", app_name="test_app")
        )
        deployment_state_manager.set_deployment_deleted(
            DeploymentID(name="d1", app_name="test_app")
        )
        app_state_manager.delete_app("test_app")
        app_state_manager.update()

        # Verify app-level policy is deregistered
        assert asm._application_has_policy("test_app") is False
        assert asm.should_autoscale_application("test_app") is False

    def test_app_level_autoscaling_policy_add_and_remove_from_config(
        self, mocked_application_state_manager
    ):
        """Test that application-level autoscaling policy is registered when added and deregistered when removed."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Deploy app without policy initially
        app_config_no_policy = self._create_app_config(has_policy=False)
        _ = self._deploy_app_with_mocks(app_state_manager, app_config_no_policy)
        asm = self._register_deployments(app_state_manager, app_config_no_policy)

        # Verify no app-level policy initially
        # Note: The app might be registered but without a policy
        assert asm._application_has_policy("test_app") is False

        # Now add app-level autoscaling policy
        app_config_with_policy = self._create_app_config(has_policy=True)
        _ = self._deploy_app_with_mocks(app_state_manager, app_config_with_policy)

        # Verify app-level policy is registered
        assert asm._application_has_policy("test_app") is True

        # Now remove app-level autoscaling policy
        app_config_no_policy_again = self._create_app_config(has_policy=False)
        _ = self._deploy_app_with_mocks(app_state_manager, app_config_no_policy_again)

        # Verify app-level policy is deregistered
        # Note: The app might still exist but without a policy
        assert asm._application_has_policy("test_app") is False
        assert asm.should_autoscale_application("test_app") is False

    def test_app_level_autoscaling_policy_with_multiple_deployments(
        self, mocked_application_state_manager
    ):
        """Test that app-level autoscaling policy works correctly with multiple deployments."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Create app with multiple deployments
        deployments = [
            DeploymentSchema(
                name="d1",
                autoscaling_config={
                    "target_ongoing_requests": 1,
                    "min_replicas": 1,
                    "max_replicas": 10,
                    "initial_replicas": 1,
                },
            ),
            DeploymentSchema(
                name="d2",
                autoscaling_config={
                    "target_ongoing_requests": 1,
                    "min_replicas": 1,
                    "max_replicas": 10,
                    "initial_replicas": 1,
                },
            ),
            DeploymentSchema(
                name="d3",
                autoscaling_config={
                    "target_ongoing_requests": 1,
                    "min_replicas": 1,
                    "max_replicas": 10,
                    "initial_replicas": 1,
                },
            ),
        ]

        app_config = self._create_app_config(deployments=deployments)
        _ = self._deploy_app_with_mocks(app_state_manager, app_config)
        asm = self._register_deployments(app_state_manager, app_config)

        # Verify policy was registered
        assert asm._application_has_policy("test_app") is True

        # Create replicas for all deployments
        deployment_ids = [
            DeploymentID(name=f"d{i}", app_name="test_app") for i in range(1, 4)
        ]
        for i, deployment_id in enumerate(deployment_ids):
            replicas = [
                ReplicaID(unique_id=f"d{i+1}_replica_{j}", deployment_id=deployment_id)
                for j in [1, 2]
            ]
            asm.update_running_replica_ids(deployment_id, replicas)

        # Test autoscaling
        deployment_state_manager._scaling_decisions.clear()
        app_state_manager.update()

        # Verify all deployments were scaled to 3 (our policy scales all to 3)
        assert asm.should_autoscale_application("test_app") is True
        for deployment_id in deployment_ids:
            assert deployment_id in deployment_state_manager._scaling_decisions
            assert deployment_state_manager._scaling_decisions[deployment_id] == 3

    def test_app_level_autoscaling_policy_state_persistence(
        self, mocked_application_state_manager
    ):
        """Test that app-level autoscaling policy state is maintained across multiple calls."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Deploy app with policy
        app_config = self._create_app_config()
        _ = self._deploy_app_with_mocks(app_state_manager, app_config)
        asm = self._register_deployments(app_state_manager, app_config)

        # Create replicas
        d1_id = DeploymentID(name="d1", app_name="test_app")
        d1_replicas = [
            ReplicaID(unique_id=f"d1_replica_{i}", deployment_id=d1_id) for i in [1, 2]
        ]
        asm.update_running_replica_ids(d1_id, d1_replicas)

        # Test multiple autoscaling calls
        for i in range(3):
            deployment_state_manager._scaling_decisions.clear()
            app_state_manager.update()
            assert asm.should_autoscale_application("test_app") is True
            assert deployment_state_manager._scaling_decisions[d1_id] == 3

    def test_autoscaling_state_manager_helper_methods(
        self, mocked_application_state_manager
    ):
        """Test the new helper methods in AutoscalingStateManager."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        asm = app_state_manager._autoscaling_state_manager

        # Test with no applications registered
        assert asm._application_has_policy("nonexistent_app") is False
        assert asm.should_autoscale_application("nonexistent_app") is False

        # Deploy app with policy
        app_config = self._create_app_config()
        _ = self._deploy_app_with_mocks(app_state_manager, app_config)
        asm = self._register_deployments(app_state_manager, app_config)

        # Test helper methods
        assert asm._application_has_policy("test_app") is True
        assert asm.should_autoscale_application("test_app") is True

        d1_id = DeploymentID(name="d1", app_name="test_app")
        assert asm.should_autoscale_deployment(d1_id) is True

        # Test with app without policy
        app_config_no_policy = self._create_app_config(has_policy=False)
        _ = self._deploy_app_with_mocks(app_state_manager, app_config_no_policy)
        asm_no_policy = self._register_deployments(
            app_state_manager, app_config_no_policy
        )

        assert asm_no_policy._application_has_policy("test_app") is False
        assert (
            asm_no_policy.should_autoscale_application("test_app") is True
        )  # App exists but no policy
        assert asm_no_policy.should_autoscale_deployment(d1_id) is True

    def test_get_decision_num_replicas_method(self, mocked_application_state_manager):
        """Test the get_decision_num_replicas method in AutoscalingStateManager."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Deploy app with policy
        app_config = self._create_app_config()
        _ = self._deploy_app_with_mocks(app_state_manager, app_config)
        asm = self._register_deployments(app_state_manager, app_config)

        # Create replicas
        d1_id = DeploymentID(name="d1", app_name="test_app")
        d1_replicas = [
            ReplicaID(unique_id=f"d1_replica_{i}", deployment_id=d1_id) for i in [1, 2]
        ]
        asm.update_running_replica_ids(d1_id, d1_replicas)

        # Test get_decision_num_replicas
        deployment_to_target_num_replicas = {d1_id: 2}
        decisions = asm.get_decision_num_replicas(
            "test_app", deployment_to_target_num_replicas
        )

        assert d1_id in decisions
        assert decisions[d1_id] == 3  # Our policy scales to 3

    def test_multiple_applications_autoscaling_isolation(
        self, mocked_application_state_manager
    ):
        """Test that autoscaling works correctly with multiple applications."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Deploy both apps simultaneously
        app_config1 = self._create_app_config(app_name="app1")
        app_config2 = self._create_app_config(app_name="app2", has_policy=False)

        # Deploy both apps using new helper
        asm = self._deploy_multiple_apps_with_mocks(
            app_state_manager, [app_config1, app_config2]
        )

        # Register deployments for both apps using existing helper
        asm = self._register_deployments(app_state_manager, app_config1)
        asm = self._register_deployments(app_state_manager, app_config2)

        # Test isolation
        assert asm._application_has_policy("app1") is True
        assert asm._application_has_policy("app2") is False
        assert asm.should_autoscale_application("app1") is True
        assert asm.should_autoscale_application("app2") is True

        # Test deployment-level isolation
        d1_app1_id = DeploymentID(name="d1", app_name="app1")
        d1_app2_id = DeploymentID(name="d1", app_name="app2")

        asm.update_running_replica_ids(
            d1_app1_id,
            [
                ReplicaID(unique_id=f"d1_app1_replica_{i}", deployment_id=d1_app1_id)
                for i in [1, 2]
            ],
        )
        asm.update_running_replica_ids(
            d1_app2_id,
            [
                ReplicaID(unique_id=f"d1_app2_replica_{i}", deployment_id=d1_app2_id)
                for i in [1, 2]
            ],
        )

        assert asm.should_autoscale_deployment(d1_app1_id) is True
        assert asm.should_autoscale_deployment(d1_app2_id) is True

        deployment_state_manager._scaling_decisions.clear()

        app_state_manager.update()

        # Both apps should be autoscaled, but with different behaviors:
        # app1 has an app-level policy, so it scales to 3 replicas
        # app2 doesn't have an app-level policy, so it uses deployment-level autoscaling (scales to 1)
        assert d1_app1_id in deployment_state_manager._scaling_decisions
        assert deployment_state_manager._scaling_decisions[d1_app1_id] == 3
        assert d1_app2_id in deployment_state_manager._scaling_decisions
        assert deployment_state_manager._scaling_decisions[d1_app2_id] == 1

    def test_autoscaling_state_manager_edge_cases(
        self, mocked_application_state_manager
    ):
        """Test edge cases for AutoscalingStateManager methods."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        asm = app_state_manager._autoscaling_state_manager

        # Test with empty app name
        assert asm._application_has_policy("") is False
        assert asm.should_autoscale_application("") is False

        # Test with None app name
        assert asm._application_has_policy(None) is False
        assert asm.should_autoscale_application(None) is False

        # Test get_decision_num_replicas with nonexistent app
        with pytest.raises(KeyError):
            asm.get_decision_num_replicas("nonexistent_app", {})

        # Test should_autoscale_deployment with nonexistent deployment
        nonexistent_deployment_id = DeploymentID(
            name="nonexistent", app_name="nonexistent_app"
        )
        assert asm.should_autoscale_deployment(nonexistent_deployment_id) is False

    def test_autoscaling_with_deployment_level_configs(
        self, mocked_application_state_manager
    ):
        """Test that app-level autoscaling respects deployment-level autoscaling configs."""
        (
            app_state_manager,
            deployment_state_manager,
            _,
        ) = mocked_application_state_manager

        # Create app with deployments that have different autoscaling configs
        deployments = [
            DeploymentSchema(
                name="d1",
                autoscaling_config={
                    "target_ongoing_requests": 1,
                    "min_replicas": 1,
                    "max_replicas": 3,  # Lower max
                    "initial_replicas": 1,
                },
            ),
            DeploymentSchema(
                name="d2",
                autoscaling_config={
                    "target_ongoing_requests": 1,
                    "min_replicas": 1,
                    "max_replicas": 10,  # Higher max
                    "initial_replicas": 1,
                },
            ),
        ]

        app_config = self._create_app_config(deployments=deployments)
        _ = self._deploy_app_with_mocks(app_state_manager, app_config)
        asm = self._register_deployments(app_state_manager, app_config)

        # Create replicas
        d1_id = DeploymentID(name="d1", app_name="test_app")
        d2_id = DeploymentID(name="d2", app_name="test_app")

        d1_replicas = [
            ReplicaID(unique_id=f"d1_replica_{i}", deployment_id=d1_id) for i in [1, 2]
        ]
        d2_replicas = [
            ReplicaID(unique_id=f"d2_replica_{i}", deployment_id=d2_id) for i in [1, 2]
        ]

        asm.update_running_replica_ids(d1_id, d1_replicas)
        asm.update_running_replica_ids(d2_id, d2_replicas)

        # Test autoscaling
        deployment_state_manager._scaling_decisions.clear()
        app_state_manager.update()

        # Verify both deployments were scaled, but d1 should be capped at max_replicas=3
        assert d1_id in deployment_state_manager._scaling_decisions
        assert d2_id in deployment_state_manager._scaling_decisions
        assert (
            deployment_state_manager._scaling_decisions[d1_id] == 3
        )  # Capped by max_replicas
        assert (
            deployment_state_manager._scaling_decisions[d2_id] == 3
        )  # Our policy scales to 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
