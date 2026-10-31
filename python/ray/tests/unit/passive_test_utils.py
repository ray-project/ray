import logging
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

from ray._raylet import GRPC_STATUS_CODE_UNAVAILABLE
from ray.dashboard.subprocesses.module import SubprocessModuleConfig
from ray.dashboard.utils import DashboardHeadModuleConfig
from ray.exceptions import GcsPassiveError


def passive_gcs_rejection() -> GcsPassiveError:
    """The error a passive GCS raises, as check_status() translates it."""
    return GcsPassiveError(
        "GCS server is in passive (read-only) mode.",
        rpc_code=GRPC_STATUS_CODE_UNAVAILABLE,
    )


DEFAULT_SESSION_NAME = b"session_2026-01-01_00-00-00_000000_1"


def make_mock_gcs_client(
    *, leader: bool = True, session_name: bytes = DEFAULT_SESSION_NAME
) -> MagicMock:
    """Create a mock GcsClient configured for active or passive mode."""
    gcs_client = MagicMock()
    gcs_client.is_gcs_leader = MagicMock(return_value=leader)
    gcs_client.is_gcs_leader_local = MagicMock(return_value=leader)
    gcs_client.internal_kv_get = MagicMock(return_value=session_name)
    gcs_client.async_internal_kv_put = AsyncMock(
        side_effect=None if leader else passive_gcs_rejection()
    )
    gcs_client.async_internal_kv_del = AsyncMock(
        side_effect=None if leader else passive_gcs_rejection()
    )
    gcs_client.internal_kv_put = MagicMock(
        side_effect=None if leader else passive_gcs_rejection()
    )
    gcs_client.internal_kv_del = MagicMock(
        side_effect=None if leader else passive_gcs_rejection()
    )
    gcs_client.async_check_alive = AsyncMock(return_value=[])
    return gcs_client


def set_mock_leader(gcs_client: MagicMock, leader: bool) -> None:
    """Switch a mock GcsClient between active leader and passive standby mode."""
    gcs_client.is_gcs_leader.return_value = leader
    gcs_client.is_gcs_leader_local.return_value = leader
    gcs_client.async_internal_kv_put.side_effect = (
        None if leader else passive_gcs_rejection()
    )
    gcs_client.async_internal_kv_del.side_effect = (
        None if leader else passive_gcs_rejection()
    )
    gcs_client.internal_kv_put.side_effect = None if leader else passive_gcs_rejection()
    gcs_client.internal_kv_del.side_effect = None if leader else passive_gcs_rejection()


def attach_logger(
    caplog, logger_name: str, level: int = logging.INFO
) -> logging.Logger:
    """Attach caplog directly to a non-propagating Ray module logger."""
    logger = logging.getLogger(logger_name)
    logger.addHandler(caplog.handler)
    caplog.set_level(level, logger=logger_name)
    return logger


def count_logged(caplog, needle: str) -> int:
    """Count occurrences of needle in caplog records."""
    return sum(needle in record.getMessage() for record in caplog.records)


def make_dummy_subprocess_config(tmp_path, **overrides) -> SubprocessModuleConfig:
    """Build a minimal SubprocessModuleConfig for unit tests."""
    kwargs = {
        "cluster_id_hex": "f" * 56,
        "gcs_address": "127.0.0.1:6379",
        "session_name": "session",
        "temp_dir": str(tmp_path),
        "session_dir": str(tmp_path),
        "logging_level": logging.INFO,
        "logging_format": "%(message)s",
        "log_dir": str(tmp_path),
        "logging_filename": "dashboard.log",
        "logging_rotate_bytes": 1,
        "logging_rotate_backup_count": 1,
        "socket_dir": str(tmp_path),
    }
    kwargs.update(overrides)
    return SubprocessModuleConfig(**kwargs)


def make_dummy_head_config(
    tmp_path, gcs_client: Optional[MagicMock] = None, **overrides
) -> DashboardHeadModuleConfig:
    """Build a minimal DashboardHeadModuleConfig for unit tests."""
    kwargs = {
        "minimal": True,
        "cluster_id_hex": "f" * 56,
        "session_name": "session",
        "gcs_address": "127.0.0.1:6379",
        "log_dir": str(tmp_path),
        "temp_dir": str(tmp_path),
        "session_dir": str(tmp_path),
        "ip": "1.2.3.4",
        "http_host": "1.2.3.4",
        "http_port": 8265,
        "gcs_client": gcs_client,
    }
    kwargs.update(overrides)
    return DashboardHeadModuleConfig(**kwargs)
