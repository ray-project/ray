# Unit tests for ray._private.gcs_passive_utils.
import logging
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

import ray._private.ray_constants as ray_constants
from ray._private.gcs_passive_utils import (
    PassiveLatch,
    _async_is_gcs_leader,
    is_refused_by_passive_gcs,
    wait_until_gcs_leader,
)
from ray._raylet import GRPC_STATUS_CODE_UNAVAILABLE
from ray.exceptions import GcsPassiveError, RpcError

TEST_LOGGER_NAME = "ray.test.gcs_passive_utils"


def _passive_error():
    return GcsPassiveError(
        "GCS server is in passive (read-only) mode.",
        rpc_code=GRPC_STATUS_CODE_UNAVAILABLE,
    )


def _capture(caplog, logger_name):
    logger = logging.getLogger(logger_name)
    logger.addHandler(caplog.handler)
    caplog.set_level(logging.INFO, logger=logger_name)
    return logger


def _count_logged(caplog, needle):
    return sum(needle in record.getMessage() for record in caplog.records)


@pytest.fixture(autouse=True)
def leader_election_on(monkeypatch):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", True)


@pytest.fixture
def test_logger(caplog):
    logger = _capture(caplog, TEST_LOGGER_NAME)
    yield logger
    logger.removeHandler(caplog.handler)


# ==============================================================================
# is_refused_by_passive_gcs tests
# ==============================================================================


def test_is_refused_by_passive_gcs_when_flag_on():
    assert is_refused_by_passive_gcs(_passive_error()) is True


def test_is_refused_by_passive_gcs_when_flag_off(monkeypatch):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)
    assert is_refused_by_passive_gcs(_passive_error()) is False


def test_is_refused_by_passive_gcs_for_other_exceptions():
    assert is_refused_by_passive_gcs(RpcError("connection refused")) is False
    assert is_refused_by_passive_gcs(ValueError("key not found")) is False
    assert is_refused_by_passive_gcs(RuntimeError("boom")) is False


# ==============================================================================
# PassiveLatch tests
# ==============================================================================


def test_passive_latch_initial_state_and_properties(test_logger):
    latch = PassiveLatch("test component", test_logger)
    assert latch.waiting_for_promotion is False

    latch.waiting_for_promotion = True
    assert latch.waiting_for_promotion is True

    latch.waiting_for_promotion = False
    assert latch.waiting_for_promotion is False


def test_passive_latch_note_passive_edge_triggered(test_logger, caplog):
    latch = PassiveLatch("test component", test_logger)

    # First call: rising edge -> logs warning and returns True
    assert latch.note_passive() is True
    assert latch.waiting_for_promotion is True
    assert _count_logged(caplog, "GCS is in passive mode") == 1
    assert _count_logged(caplog, "test component") == 1

    # Second call: already waiting -> silent, returns False
    assert latch.note_passive() is False
    assert latch.waiting_for_promotion is True
    assert _count_logged(caplog, "GCS is in passive mode") == 1


def test_passive_latch_is_refused_by_passive_gcs(test_logger, caplog):
    latch = PassiveLatch("test component", test_logger)

    # Non-passive error: returns False, no logging or state change
    assert latch.is_refused_by_passive_gcs(ValueError("other")) is False
    assert latch.waiting_for_promotion is False
    assert _count_logged(caplog, "GCS is in passive mode") == 0

    # Passive error: returns True, logs warning once
    assert latch.is_refused_by_passive_gcs(_passive_error()) is True
    assert latch.waiting_for_promotion is True
    assert _count_logged(caplog, "GCS is in passive mode") == 1

    # Subsequent passive error: returns True, but no duplicate warning
    assert latch.is_refused_by_passive_gcs(_passive_error()) is True
    assert _count_logged(caplog, "GCS is in passive mode") == 1


def test_passive_latch_is_refused_by_passive_gcs_when_flag_off(
    test_logger, caplog, monkeypatch
):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)
    latch = PassiveLatch("test component", test_logger)

    assert latch.is_refused_by_passive_gcs(_passive_error()) is False
    assert latch.waiting_for_promotion is False
    assert _count_logged(caplog, "GCS is in passive mode") == 0


def test_passive_latch_promoted_edge_triggered(test_logger, caplog):
    latch = PassiveLatch("test component", test_logger)

    # Calling promoted when not waiting: returns False, no logging
    assert latch.promoted() is False
    assert _count_logged(caplog, "promoted to leader") == 0

    # Enter passive, then promote
    latch.note_passive()
    assert latch.waiting_for_promotion is True

    assert latch.promoted() is True
    assert latch.waiting_for_promotion is False
    assert _count_logged(caplog, "promoted to leader") == 1

    # Calling promoted again: returns False, no duplicate log
    assert latch.promoted() is False
    assert _count_logged(caplog, "promoted to leader") == 1


def test_passive_latch_promoted_with_format_args(test_logger, caplog):
    latch = PassiveLatch(
        "http address",
        test_logger,
        action_desc_promoted="Resolved address: %s on port %d",
    )
    latch.note_passive()

    assert latch.promoted("http://127.0.0.1", 8265) is True
    assert _count_logged(caplog, "Resolved address: http://127.0.0.1 on port 8265") == 1


def test_passive_latch_custom_action_descriptions(test_logger, caplog):
    latch = PassiveLatch(
        "custom service",
        test_logger,
        action_desc_passive="Custom passive warning.",
        action_desc_promoted="Custom promoted info.",
    )
    latch.note_passive()
    assert _count_logged(caplog, "Custom passive warning.") == 1

    latch.promoted()
    assert _count_logged(caplog, "Custom promoted info.") == 1


# ==============================================================================
# _async_is_gcs_leader tests
# ==============================================================================


@pytest.mark.asyncio
async def test_async_is_gcs_leader_when_flag_off(monkeypatch):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)
    client = MagicMock()
    # When flag is off, returns True immediately without calling any client method
    assert await _async_is_gcs_leader(client) is True
    client.async_check_alive.assert_not_called()
    client.is_gcs_leader_local.assert_not_called()


@pytest.mark.asyncio
async def test_async_is_gcs_leader_with_async_check_alive():
    client = MagicMock()
    client.async_check_alive = AsyncMock()
    client.is_gcs_leader_local = MagicMock(return_value=True)

    # Leader true
    assert await _async_is_gcs_leader(client, interval=3.0) is True
    client.async_check_alive.assert_awaited_once_with(node_ids=[], timeout=3.0)
    client.is_gcs_leader_local.assert_called_once()

    # Leader false
    client.is_gcs_leader_local.return_value = False
    assert await _async_is_gcs_leader(client) is False


@pytest.mark.asyncio
async def test_async_is_gcs_leader_with_async_check_alive_exception_falls_back():
    client = MagicMock()
    client.async_check_alive = AsyncMock(
        side_effect=RpcError("timeout during failover")
    )
    client.is_gcs_leader_local = MagicMock(return_value=True)

    # CheckAlive raises during failover -> falls back to cached local leadership
    assert await _async_is_gcs_leader(client) is True

    client.is_gcs_leader_local.return_value = False
    assert await _async_is_gcs_leader(client) is False


@pytest.mark.asyncio
async def test_async_is_gcs_leader_mock_fallbacks():
    # 1. Mock with only is_gcs_leader_local
    client1 = MagicMock(spec=["is_gcs_leader_local"])
    client1.is_gcs_leader_local = MagicMock(return_value=True)
    assert await _async_is_gcs_leader(client1) is True

    # 2. Mock with only is_gcs_leader
    client2 = MagicMock(spec=["is_gcs_leader"])
    client2.is_gcs_leader = MagicMock(return_value=True)
    assert await _async_is_gcs_leader(client2) is True

    # 3. Object with neither -> returns False as default
    client3 = object()
    assert await _async_is_gcs_leader(client3) is False


# ==============================================================================
# wait_until_gcs_leader tests
# ==============================================================================


@pytest.mark.asyncio
async def test_wait_until_gcs_leader_when_flag_off(monkeypatch, test_logger, caplog):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)
    client = MagicMock()
    latch = PassiveLatch("test", test_logger)

    await wait_until_gcs_leader(client, latch=latch)
    client.async_check_alive.assert_not_called()
    assert latch.waiting_for_promotion is False
    assert len(caplog.records) == 0


@pytest.mark.asyncio
async def test_wait_until_gcs_leader_already_leader(test_logger, caplog):
    client = MagicMock()
    client.async_check_alive = AsyncMock()
    client.is_gcs_leader_local = MagicMock(return_value=True)
    latch = PassiveLatch("test", test_logger)

    await wait_until_gcs_leader(client, latch=latch)
    assert latch.waiting_for_promotion is False
    assert _count_logged(caplog, "GCS is in passive mode") == 0


@pytest.mark.asyncio
async def test_wait_until_gcs_leader_pauses_and_promotes(test_logger, caplog):
    client = MagicMock()
    client.async_check_alive = AsyncMock()
    # Starts passive for 2 checks, then becomes leader on the 3rd
    client.is_gcs_leader_local = MagicMock(side_effect=[False, False, True])
    latch = PassiveLatch("test service", test_logger)

    await wait_until_gcs_leader(
        client, poll_interval_s=0.01, check_timeout_s=0.01, latch=latch
    )

    # Warning logged once on entering passive
    assert _count_logged(caplog, "GCS is in passive mode") == 1
    # Info logged once on promotion
    assert _count_logged(caplog, "GCS was promoted to leader") == 1
    assert latch.waiting_for_promotion is False
    assert client.async_check_alive.await_count == 3


@pytest.mark.asyncio
async def test_wait_until_gcs_leader_without_latch():
    client = MagicMock()
    client.async_check_alive = AsyncMock()
    client.is_gcs_leader_local = MagicMock(side_effect=[False, True])

    await wait_until_gcs_leader(client, poll_interval_s=0.01)
    assert client.async_check_alive.await_count == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
