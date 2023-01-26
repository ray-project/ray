import sys
import pytest
from typing import List
import time

import ray
from ray._private.test_utils import SignalActor
from ray.serve._private.application_state import ApplicationStateManager
from ray.serve._private.common import ApplicationStatus
from ray.serve._private.common import DeploymentStatus, DeploymentStatusInfo


class MockDeploymentStateManager:
    def __init__(self):
        self.deployment_statuses = [
            DeploymentStatusInfo("d1", DeploymentStatus.UPDATING),
            DeploymentStatusInfo("d2", DeploymentStatus.UPDATING),
        ]

    def set_deployment_statuses_unhealthy(self, index: int = 0):
        self.deployment_statuses[index].status = DeploymentStatus.UNHEALTHY

    def set_deployment_statuses_healthy(self, index: int = 0):
        self.deployment_statuses[index].status = DeploymentStatus.HEALTHY

    def get_deployment_statuses(self, deployment_names: List[str]):
        return self.deployment_statuses

    def get_all_deployments(self):
        return ["d1", "d2"]


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
