import logging
import os
import ray._private.ray_constants as ray_constants

logger = logging.getLogger(__name__)


class HeadNodeLeaderSelectorConfig:
    # Will exit after N consecutive failures
    max_failure_count = None
    # The interval time for selecting the leader or expire leader
    check_interval_s = None
    # The expiration time of the leader key
    key_expire_time_ms = None
    # The time to wait after becoming the active allows the gcs client to
    # have enough time to find out that the gcs address has changed.
    wait_time_after_be_active_s = None
    # Maximum time to wait pre gcs stop serving
    wait_pre_gcs_stop_max_time_s = None
    # Redis/socket connect time out(s)
    connect_timeout_s = None

    def __init__(self):
        self.max_failure_count = int(
            os.environ.get("RAY_HA_CHECK_MAX_FAILURE_COUNT", "5")
        )
        # Default 3s
        self.check_interval_s = (
            int(os.environ.get("RAY_HA_CHECK_INTERVAL_MS", "3000")) / 1000
        )
        # Default 60s
        self.key_expire_time_ms = int(
            os.environ.get("RAY_HA_KEY_EXPIRE_TIME_MS", "60000")
        )
        # Default 2s
        self.wait_time_after_be_active_s = (
            int(os.environ.get("RAY_HA_WAIT_TIME_AFTER_BE_ACTIVE_MS", "2000")) / 1000
        )
        # Default 3 day (1000 * 60 * 60 * 24 * 3).
        self.wait_pre_gcs_stop_max_time_s = (
            int(os.environ.get("RAY_HA_WAIT_PRE_GCS_STOP_MAX_TIME_MS", "259200000"))
            / 1000
        )
        # Default redis/socket connect time out is 5s.
        self.connect_timeout_s = (
            int(os.environ.get("RAY_HA_CONNECT_TIMEOUT_MS", "5000")) / 1000
        )

    def __repr__(self) -> str:
        return (
            f"HeadNodeLeaderSelectorConfig["
            f"max_failure_count={self.max_failure_count},"
            f"check_interval_s={self.check_interval_s},"
            f"key_expire_time_ms={self.key_expire_time_ms},"
            f"wait_time_after_be_active_s={self.wait_time_after_be_active_s},"
            f"wait_pre_gcs_stop_max_time_s="
            f"{self.wait_pre_gcs_stop_max_time_s},"
            f"connect_timeout_s={self.connect_timeout_s}]"
        )


class HeadNodeLeaderSelector:
    _role_type = ray_constants.HEAD_ROLE_STANDBY
    _config = None
    _is_running = False

    def __init__(self):
        self._config = HeadNodeLeaderSelectorConfig()

    def start(self):
        pass

    def stop(self):
        pass

    def set_role_type(self, role_type):
        if role_type != self._role_type:
            logger.info(
                "This head node changed from %s to %s.", self._role_type, role_type
            )
            self._role_type = role_type

    def get_role_type(self):
        return self._role_type

    def is_leader(self):
        return self.get_role_type() == ray_constants.HEAD_ROLE_ACTIVE

    def do_action_after_be_active(self):
        pass

    def is_running(self):
        return self._is_running
