import sys
from typing import Dict, List, Optional

import pytest

from ray.serve._private.application_state import ApplicationStatusInfo
from ray.serve._private.common import ConfigSnapshot, RolloutState
from ray.serve._private.rollout_supervisor import RolloutSupervisor
from ray.serve.schema import ApplicationStatus


class MockApplicationStateManager:
    """
    Lightweight mock that tracks per-app statuses for unit testing
    the RolloutSupervisor without a real ApplicationStateManager.
    """

    def __init__(self):
        self._app_statuses: Dict[str, ApplicationStatusInfo] = {}

    def set_app_status(self, app_name, status) -> ApplicationStatusInfo:
        self._app_statuses[app_name] = ApplicationStatusInfo(
            status,
            message="",
            deployment_timestamp=0.0,
        )

    def apply_app_configs(self, config: ConfigSnapshot):
        """Simulate apply_app_configs by setting all apps to DEPLOYING."""
        for app_name in config.config_dict:
            self.set_app_status(app_name, ApplicationStatus.DEPLOYING)

    def list_app_statuses(self) -> Dict[str, ApplicationStatusInfo]:
        return self._app_statuses


@pytest.fixture
def deployed_all_running():
    def _deploy(apps, auto_rollback=True):
        asm, rs, new_config = initialise(apps, auto_rollback)
        for app in apps:
            asm.set_app_status(app, ApplicationStatus.RUNNING)
        assert not rs.update()
        return asm, rs, new_config

    return _deploy


def make_config(apps: List, auto_rollback: bool = True) -> ConfigSnapshot:
    return ConfigSnapshot(
        config_dict={name: {} for name in apps},
        auto_rollback_enabled=auto_rollback,
    )


def check_supervisor_state(
    rs: RolloutSupervisor,
    current_config: ConfigSnapshot,
    last_good_config: Optional[ConfigSnapshot],
    state: RolloutState,
):
    """Assert that the supervisor's current_config, last_good_config, and
    state match the expected values."""
    assert rs.current_config == current_config, (
        f"Current config doesnt match: expected {current_config} "
        f"but got {rs.current_config}"
    )
    assert rs.last_good_config == last_good_config, (
        f"Last good config is incorrect: expected {last_good_config} "
        f"but got {rs.last_good_config}"
    )
    assert rs.state == state, f"States differ, expected {state} but got {rs.state}"


def initialise(apps: Optional[List] = None, auto_rollback=True):
    """
    1. Create a MockApplicationStateManager and RolloutSupervisor.
    2. Build a config from apps, and call on_new_config to enter WATCHING.
    3. Sets all apps to DEPLOYING to simulate apply_app_configs()."""
    if apps is None:
        apps = ["app1"]
    asm = MockApplicationStateManager()
    rs = RolloutSupervisor(asm)
    new_config = make_config(apps, auto_rollback)
    rs.on_new_config(new_config)
    asm.apply_app_configs(new_config)
    return asm, rs, new_config


def test_partially_deploying_then_running():
    """Test WATCHING -> WATCHING (partial progress) -> IDLE (all RUNNING).

    Verifies that the supervisor stays in WATCHING while only some apps are
    RUNNING and others still deploying, and transitions to IDLE once all apps reach RUNNING.
    """
    apps = ["app1", "app2"]
    asm, rs, config = initialise(apps)
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    asm.set_app_status("app1", ApplicationStatus.RUNNING)
    assert not rs.update()
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    asm.set_app_status("app2", ApplicationStatus.RUNNING)
    assert not rs.update()
    check_supervisor_state(rs, config, config, RolloutState.IDLE)


def test_unhealthy_then_running():
    """Test WATCHING ->WATCHING(one running, one deploying)->WATCHING(one unhealthy,one running)->IDLE(all running).

    Verifies that the supervisor stays in WATCHING while one app is unhealthy, and transitions to IDLE
    once all apps reach RUNNING.
    """
    apps = ["app1", "app2"]
    asm, rs, config = initialise(apps)
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    asm.set_app_status("app1", ApplicationStatus.RUNNING)
    asm.set_app_status("app2", ApplicationStatus.DEPLOYING)
    assert not rs.update()
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    asm.set_app_status("app1", ApplicationStatus.UNHEALTHY)
    asm.set_app_status("app2", ApplicationStatus.RUNNING)
    assert not rs.update()
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    asm.set_app_status("app1", ApplicationStatus.RUNNING)
    asm.set_app_status("app2", ApplicationStatus.RUNNING)
    assert not rs.update()
    check_supervisor_state(rs, config, config, RolloutState.IDLE)


def test_deploy_failed_takes_priority_over_missing_app(deployed_all_running):
    """Test that DEPLOY_FAILED is detected even when another app is missing
    from the status manager.

    A missing app should not mask a failure on a different app.
    """
    apps = ["app1", "app2"]
    asm, rs, config = deployed_all_running(apps)

    new_config = make_config(["app3", "app4"], auto_rollback=True)
    rs.on_new_config(new_config)
    # Only set app3 to DEPLOY_FAILED; app4 is never registered in the
    # status manager, simulating an app that hasn't appeared yet.
    asm.set_app_status("app3", ApplicationStatus.DEPLOY_FAILED)
    assert rs.update()
    check_supervisor_state(rs, config, config, RolloutState.ROLLING_BACK)


def test_no_rollback_without_last_good():
    """Test WATCHING -> IDLE when deploy fails with no last_good config.

    When no prior successful config exists, the supervisor cannot rollback
    and transitions directly to IDLE.
    """
    apps = ["app1", "app2"]
    asm, rs, config = initialise(apps)
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    asm.set_app_status("app1", ApplicationStatus.RUNNING)
    assert not rs.update()
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    asm.set_app_status("app2", ApplicationStatus.DEPLOY_FAILED)
    assert not rs.update()
    check_supervisor_state(rs, config, None, RolloutState.IDLE)


def test_rollback_on_failure(deployed_all_running):
    """Test IDLE -> WATCHING -> ROLLING_BACK -> ROLLED_BACK.

    A successful deploy establishes last_good, then a new deploy fails.
    The supervisor triggers rollback and transitions to ROLLED_BACK once
    the rollback config reaches RUNNING.
    """
    apps = ["app1", "app2"]
    asm, rs, config = deployed_all_running(apps)
    check_supervisor_state(rs, config, config, RolloutState.IDLE)

    apps_new = ["app3", "app4", "app5"]
    new_config = make_config(apps_new, auto_rollback=True)
    rs.on_new_config(new_config)
    asm.apply_app_configs(new_config)
    check_supervisor_state(rs, new_config, config, RolloutState.WATCHING)

    asm.set_app_status("app3", ApplicationStatus.DEPLOY_FAILED)
    asm.set_app_status("app5", ApplicationStatus.DEPLOY_FAILED)
    assert rs.update()
    check_supervisor_state(rs, config, config, RolloutState.ROLLING_BACK)

    for app in apps:
        asm.set_app_status(app, ApplicationStatus.RUNNING)
    assert not rs.update()
    check_supervisor_state(rs, config, config, RolloutState.ROLLED_BACK)


def test_rollback_ping_pong(deployed_all_running):
    """Test ROLLING_BACK -> ROLLBACK_FAILED -> (new config) -> WATCHING.

    After a deploy fails and the rollback also fails, the supervisor
    enters ROLLBACK_FAILED. A fresh config submission resets the supervisor
    to WATCHING.
    """
    apps = ["app1", "app2", "app3"]
    asm, rs, config = deployed_all_running(apps)
    check_supervisor_state(rs, config, config, RolloutState.IDLE)

    new_config = make_config(apps=["app3", "app4", "app5"], auto_rollback=True)
    rs.on_new_config(new_config)
    asm.apply_app_configs(new_config)
    check_supervisor_state(rs, new_config, config, RolloutState.WATCHING)

    asm.set_app_status("app3", ApplicationStatus.DEPLOY_FAILED)
    asm.set_app_status("app5", ApplicationStatus.DEPLOY_FAILED)
    assert rs.update()
    check_supervisor_state(rs, config, config, RolloutState.ROLLING_BACK)

    for app in apps:
        asm.set_app_status(app, ApplicationStatus.DEPLOY_FAILED)
    assert not rs.update()
    check_supervisor_state(rs, config, None, RolloutState.ROLLBACK_FAILED)

    fresh_config = make_config(apps=["app6"], auto_rollback=True)
    rs.on_new_config(fresh_config)
    asm.apply_app_configs(fresh_config)
    check_supervisor_state(rs, fresh_config, None, RolloutState.WATCHING)


def test_new_config_supersedes_watching():
    """Test that a new config submitted while WATCHING replaces the current
    config and stays in WATCHING.
    """
    apps = ["app1", "app2", "app3"]
    asm, rs, config = initialise(apps)
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)
    new_config = make_config(apps=["app3", "app4", "app5"])
    rs.on_new_config(new_config)
    asm.apply_app_configs(new_config)
    check_supervisor_state(rs, new_config, None, RolloutState.WATCHING)


def test_new_config_supersedes_rolling_back(deployed_all_running):
    """Test that a new config submitted while ROLLING_BACK cancels the
    rollback and resets the supervisor to WATCHING with the new config.
    """
    apps = ["app1", "app2", "app3"]
    asm, rs, config = deployed_all_running(apps)
    check_supervisor_state(rs, config, config, RolloutState.IDLE)

    new_config = make_config(apps=["app3", "app4", "app5"], auto_rollback=True)
    rs.on_new_config(new_config)
    asm.apply_app_configs(new_config)

    asm.set_app_status("app3", ApplicationStatus.DEPLOY_FAILED)
    asm.set_app_status("app5", ApplicationStatus.DEPLOY_FAILED)
    assert rs.update()
    check_supervisor_state(rs, config, config, RolloutState.ROLLING_BACK)

    supersede_config = make_config(apps=["app6"])
    rs.on_new_config(supersede_config)
    asm.apply_app_configs(supersede_config)
    check_supervisor_state(rs, supersede_config, config, RolloutState.WATCHING)


def test_disabled_rollback_promotes_on_success():
    """Test that auto_rollback=False config is still promoted to last_good
    when all apps reach RUNNING.
    """
    apps = ["app1", "app2", "app3"]
    asm, rs, config = initialise(apps, auto_rollback=False)
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    for app in apps:
        asm.set_app_status(app, ApplicationStatus.RUNNING)
    assert not rs.update()
    check_supervisor_state(rs, config, config, RolloutState.IDLE)


def test_disabled_rollback_no_rollback_on_failure():
    """Test that auto_rollback=False config that fails, transitions to IDLE
    without triggering a rollback.
    """
    apps = ["app1", "app2", "app3"]
    asm, rs, config = initialise(apps, auto_rollback=False)
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    for app in apps:
        asm.set_app_status(app, ApplicationStatus.DEPLOY_FAILED)
    assert not rs.update()
    check_supervisor_state(rs, config, None, RolloutState.IDLE)


def test_rollback_disabled_success_becomes_rollback_target():
    """Test that a successful auto_rollback=False config can serve as
    rollback target when a subsequent auto_rollback=True config fails.
    """
    apps = ["app1", "app2", "app3"]
    asm, rs, config = initialise(apps, auto_rollback=False)
    check_supervisor_state(rs, config, None, RolloutState.WATCHING)

    for app in apps:
        asm.set_app_status(app, ApplicationStatus.RUNNING)
    assert not rs.update()
    check_supervisor_state(rs, config, config, RolloutState.IDLE)

    apps2 = ["app4"]
    new_config = make_config(apps2, auto_rollback=True)
    rs.on_new_config(new_config)
    asm.apply_app_configs(new_config)
    check_supervisor_state(rs, new_config, config, RolloutState.WATCHING)

    asm.set_app_status("app4", ApplicationStatus.DEPLOY_FAILED)
    assert rs.update()
    check_supervisor_state(rs, config, config, RolloutState.ROLLING_BACK)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
