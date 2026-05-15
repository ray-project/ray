import logging
from typing import Optional

from ray.serve._private.application_state import ApplicationStateManager
from ray.serve._private.common import ConfigSnapshot, RolloutState
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.schema import ApplicationStatus

logger = logging.getLogger(SERVE_LOGGER_NAME)


class RolloutSupervisor:
    """Manages auto-rollback state for declarative Serve deployments.
    Tracks the current pending config, the last known good config, and the
    rollout state.
    On each control loop tick, checks application statuses and transitions
    the rollout state accordingly. Returns a signal to the controller when
    a rollback should be applied.
    """

    def __init__(self, application_state_manager: ApplicationStateManager):
        self._application_state_manager = application_state_manager
        self._current_config: Optional[ConfigSnapshot] = None
        self._last_good_config: Optional[ConfigSnapshot] = None
        self._state: RolloutState = RolloutState.IDLE

    @property
    def current_config(self) -> Optional[ConfigSnapshot]:
        return self._current_config

    @property
    def last_good_config(self) -> Optional[ConfigSnapshot]:
        return self._last_good_config

    @property
    def state(self) -> RolloutState:
        return self._state

    def on_new_config(self, new_config: ConfigSnapshot):
        """Called by the controller when a new user config is applied."""
        self._current_config = new_config
        self._state = RolloutState.WATCHING

    def _check_apps_status(self) -> ApplicationStatus:
        """Returns the aggregate status of apps in the current config.

        Precedence (first match wins):
        1. DEPLOY_FAILED — at least one app failed to deploy.
        2. DEPLOYING — at least one app is still deploying.
        3. UNHEALTHY — at least one app's health check is failing.
        4. RUNNING — all apps are running and healthy.
        """
        app_statuses = self._application_state_manager.list_app_statuses()
        atleast_one_deploying = False
        atleast_one_unhealthy = False
        for app_name in self._current_config.config_dict:
            status = app_statuses[app_name].status
            if status == ApplicationStatus.DEPLOY_FAILED:
                return ApplicationStatus.DEPLOY_FAILED
            if status == ApplicationStatus.DEPLOYING:
                atleast_one_deploying = True
            elif status == ApplicationStatus.UNHEALTHY:
                atleast_one_unhealthy = True
        if atleast_one_deploying:
            return ApplicationStatus.DEPLOYING
        if atleast_one_unhealthy:
            return ApplicationStatus.UNHEALTHY
        return ApplicationStatus.RUNNING

    def update(self) -> bool:
        """Checks application health and drives the rollout state machine.

        Returns True if the controller should apply a rollback, False otherwise.
        """

        # Terminal or idle states: nothing to do.
        if self._state not in (RolloutState.WATCHING, RolloutState.ROLLING_BACK):
            return False

        health = self._check_apps_status()

        # Apps still deploying or unhealthy, wait for the next tick.
        if health in (ApplicationStatus.DEPLOYING, ApplicationStatus.UNHEALTHY):
            return False

        # If all apps healthy.
        if health == ApplicationStatus.RUNNING:
            if self._state == RolloutState.WATCHING:
                # Deploy succeeded, promote to last_good.
                self._last_good_config = self._current_config
                self._state = RolloutState.IDLE
            elif self._state == RolloutState.ROLLING_BACK:
                # Rollback succeeded.
                self._state = RolloutState.ROLLED_BACK
            return False

        # --- At least one app failed---

        if self._state == RolloutState.ROLLING_BACK:
            logger.warning("Rollback also failed. The cluster needs a new config.")
            self._state = RolloutState.ROLLBACK_FAILED
            self._last_good_config = None
            return False

        # State is WATCHING, decide whether to rollback.
        if self._last_good_config is None:
            logger.warning("No prior good config to roll back to.")
            self._state = RolloutState.IDLE
            return False

        if not self._current_config.auto_rollback_enabled:
            logger.warning(
                "Deploy failed. Not rolling back since auto_rollback is not enabled."
            )
            self._state = RolloutState.IDLE
            return False

        # Trigger rollback: swap current config to last_good.
        logger.info("Deploy failed. Rolling back to last good config.")
        self._state = RolloutState.ROLLING_BACK
        self._current_config = self._last_good_config
        return True
