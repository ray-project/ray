# Unit tests for ray._private.gcs_passive_utils.
import logging
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

import ray._private.ray_constants as ray_constants
from ray._private.gcs_passive_utils import (
    PassiveLatch,
    _async_is_gcs_leader,
    async_put_kv_passive_safe,
    del_kv_passive_safe,
    is_refused_by_passive_gcs,
    put_kv_passive_safe,
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


def test_is_refused_by_passive_gcs_with_latch(test_logger, caplog):
    latch = PassiveLatch("test component", test_logger)

    # Non-passive error: returns False, no logging or state change
    assert is_refused_by_passive_gcs(ValueError("other"), latch=latch) is False
    assert latch.waiting_for_promotion is False
    assert _count_logged(caplog, "GCS is in passive mode") == 0

    # Passive error: returns True, logs warning once
    assert is_refused_by_passive_gcs(_passive_error(), latch=latch) is True
    assert latch.waiting_for_promotion is True
    assert _count_logged(caplog, "GCS is in passive mode") == 1

    # Subsequent passive error: returns True, but no duplicate warning
    assert is_refused_by_passive_gcs(_passive_error(), latch=latch) is True
    assert _count_logged(caplog, "GCS is in passive mode") == 1


def test_is_refused_by_passive_gcs_with_latch_when_flag_off(
    test_logger, caplog, monkeypatch
):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)
    latch = PassiveLatch("test component", test_logger)

    assert is_refused_by_passive_gcs(_passive_error(), latch=latch) is False
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
async def test_async_is_gcs_leader_with_check_alive(monkeypatch):
    client = MagicMock()
    client.async_check_alive = AsyncMock()
    client.is_gcs_leader_local = MagicMock(return_value=True)

    # Leader true
    assert await _async_is_gcs_leader(client, interval=3.0) is True
    client.async_check_alive.assert_awaited_once_with(node_ids=[], timeout=3.0)

    # Leader false
    client.is_gcs_leader_local.return_value = False
    assert await _async_is_gcs_leader(client) is False

    # CheckAlive raises during failover -> falls back to cached local leadership
    client.async_check_alive.side_effect = RpcError("timeout during failover")
    client.is_gcs_leader_local.return_value = True
    assert await _async_is_gcs_leader(client) is True

    # Flag off: returns True immediately without calling check_alive
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)
    client.async_check_alive.reset_mock()
    assert await _async_is_gcs_leader(client) is True
    client.async_check_alive.assert_not_called()


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
    assert await _async_is_gcs_leader(object()) is False


# ==============================================================================
# wait_until_gcs_leader tests
# ==============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize("flag_on", [True, False])
async def test_wait_until_gcs_leader_immediate_return(
    monkeypatch, test_logger, caplog, flag_on
):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", flag_on)
    client = MagicMock()
    client.async_check_alive = AsyncMock()
    client.is_gcs_leader_local = MagicMock(return_value=True)
    latch = PassiveLatch("test", test_logger)

    await wait_until_gcs_leader(client, latch=latch)
    assert latch.waiting_for_promotion is False
    assert len(caplog.records) == 0
    if not flag_on:
        client.async_check_alive.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_latch", [True, False])
async def test_wait_until_gcs_leader_pauses_until_promoted(
    test_logger, caplog, use_latch
):
    client = MagicMock()
    client.async_check_alive = AsyncMock()
    # Starts passive for 2 checks, then becomes leader on the 3rd
    client.is_gcs_leader_local = MagicMock(side_effect=[False, False, True])
    latch = PassiveLatch("test service", test_logger) if use_latch else None

    await wait_until_gcs_leader(
        client, poll_interval_s=0.01, check_timeout_s=0.01, latch=latch
    )

    if use_latch:
        assert _count_logged(caplog, "GCS is in passive mode") == 1
        assert _count_logged(caplog, "GCS was promoted to leader") == 1
        assert latch.waiting_for_promotion is False
    assert client.async_check_alive.await_count == 3


# ==============================================================================
# put_kv_passive_safe & del_kv_passive_safe tests
# ==============================================================================


@pytest.mark.asyncio
async def test_kv_passive_safe_success():
    client = MagicMock()
    client.async_internal_kv_put = AsyncMock()
    client.async_internal_kv_del = AsyncMock()

    # Sync put & del
    assert (
        put_kv_passive_safe(
            client, "k", "v", overwrite=True, namespace="ns", timeout=2.0
        )
        is True
    )
    client.internal_kv_put.assert_called_once_with(
        b"k", b"v", True, namespace=b"ns", timeout=2.0
    )

    assert (
        del_kv_passive_safe(
            client, "k", del_by_prefix=True, namespace="ns", timeout=3.0
        )
        is True
    )
    client.internal_kv_del.assert_called_once_with(
        b"k", True, namespace=b"ns", timeout=3.0
    )

    # Async put
    assert (
        await async_put_kv_passive_safe(
            client, "k", "v", overwrite=False, namespace="ns", timeout=1.5
        )
        is True
    )
    client.async_internal_kv_put.assert_awaited_once_with(
        b"k", b"v", False, namespace=b"ns", timeout=1.5
    )


@pytest.mark.asyncio
async def test_kv_passive_safe_when_passive(test_logger, caplog):
    client = MagicMock()
    client.internal_kv_put.side_effect = _passive_error()
    client.internal_kv_del.side_effect = _passive_error()
    client.async_internal_kv_put = AsyncMock(side_effect=_passive_error())

    # Without latch: returns False, no log
    assert put_kv_passive_safe(client, b"k", b"v") is False

    # With latch: returns False, triggers latch warning
    latch = PassiveLatch("test service", test_logger)
    assert put_kv_passive_safe(client, b"k", b"v", latch=latch) is False
    assert await async_put_kv_passive_safe(client, b"k", b"v", latch=latch) is False
    assert del_kv_passive_safe(client, b"k", latch=latch) is False
    assert latch.waiting_for_promotion is True
    assert _count_logged(caplog, "GCS is in passive mode") == 1


def test_kv_passive_safe_warning_messages(test_logger, caplog):
    client = MagicMock()
    client.internal_kv_put.side_effect = _passive_error()
    client.internal_kv_del.side_effect = _passive_error()

    assert (
        put_kv_passive_safe(
            client, b"webui:url", b"v", warning_message=True, logger=test_logger
        )
        is False
    )
    assert (
        _count_logged(
            caplog, "GCS is in passive mode. Skipping writing webui:url to KV."
        )
        == 1
    )

    assert (
        put_kv_passive_safe(
            client, b"k", b"v", warning_message="Custom warning", logger=test_logger
        )
        is False
    )
    assert _count_logged(caplog, "Custom warning") == 1

    assert (
        del_kv_passive_safe(
            client, b"dead_node_key", warning_message=True, logger=test_logger
        )
        is False
    )
    assert (
        _count_logged(
            caplog, "GCS is in passive mode. Skipping deleting dead_node_key from KV."
        )
        == 1
    )


@pytest.mark.asyncio
async def test_kv_passive_safe_errors_and_flag_off(monkeypatch):
    client = MagicMock()
    client.internal_kv_put.side_effect = RpcError("network failed")
    client.async_internal_kv_put = AsyncMock(side_effect=ValueError("bad key"))

    with pytest.raises(RpcError, match="network failed"):
        put_kv_passive_safe(client, b"k", b"v")

    with pytest.raises(ValueError, match="bad key"):
        await async_put_kv_passive_safe(client, b"k", b"v")

    # Flag off: passive error is re-raised
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)
    client.internal_kv_put.side_effect = _passive_error()
    with pytest.raises(GcsPassiveError):
        put_kv_passive_safe(client, b"k", b"v")


def test_put_kv_passive_safe_client_resolution(monkeypatch):
    import ray.experimental.internal_kv as internal_kv

    # Fallback to internal_kv global client
    mock_client = MagicMock()
    internal_kv._initialize_internal_kv(mock_client)
    try:
        assert put_kv_passive_safe(None, b"k", b"v") is True
        mock_client.internal_kv_put.assert_called_once_with(
            b"k", b"v", True, namespace=None
        )
    finally:
        internal_kv._internal_kv_reset()

    # Uninitialized raises
    with pytest.raises(RuntimeError, match="No gcs_client provided"):
        put_kv_passive_safe(None, b"k", b"v")


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
