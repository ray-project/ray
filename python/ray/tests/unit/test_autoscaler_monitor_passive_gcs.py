# Unit tests for how the autoscaler monitors (v1 and v2) react to a passive GCS.
import logging
import sys
from unittest.mock import MagicMock

import pytest

import ray._private.ray_constants as ray_constants
from ray._raylet import GRPC_STATUS_CODE_UNAVAILABLE
from ray.autoscaler._private import monitor as v1_monitor_module
from ray.autoscaler._private.monitor import Monitor
from ray.autoscaler.v2 import monitor as v2_monitor_module
from ray.autoscaler.v2.monitor import AutoscalerMonitor
from ray.exceptions import AuthenticationError, GcsPassiveError, RpcError
from ray.experimental.internal_kv import _internal_kv_reset

V1_LOGGER = "ray.autoscaler._private.monitor"
V2_LOGGER = "ray.autoscaler.v2.monitor"
SESSION_NAME = b"session_2026-01-01_00-00-00_000000_1"


def _passive_gcs_rejection():
    """The error a passive GCS raises, as check_status() translates it."""
    return GcsPassiveError(
        "GCS server is in passive (read-only) mode.",
        rpc_code=GRPC_STATUS_CODE_UNAVAILABLE,
    )


def _gcs_client(*, leader):
    gcs_client = MagicMock()
    gcs_client.internal_kv_get.return_value = SESSION_NAME
    gcs_client.is_gcs_leader.return_value = leader
    if not leader:
        gcs_client.internal_kv_put.side_effect = _passive_gcs_rejection()
        gcs_client.internal_kv_del.side_effect = _passive_gcs_rejection()
    return gcs_client


def _capture(caplog, logger_name):
    """Attach caplog to a module logger.

    The "ray" logger does not propagate to the root logger caplog listens on, so
    caplog.at_level() alone records nothing.
    """
    logger = logging.getLogger(logger_name)
    logger.addHandler(caplog.handler)
    caplog.set_level(logging.INFO, logger=logger_name)
    return logger


def _count_logged(caplog, needle):
    return sum(needle in record.getMessage() for record in caplog.records)


def _metrics_address_writes(gcs_client):
    return [
        call
        for call in gcs_client.internal_kv_put.call_args_list
        if call.args and call.args[0] == b"AutoscalerMetricsAddress"
    ]


@pytest.fixture(autouse=True)
def leader_election_on(monkeypatch):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", True)


@pytest.fixture(autouse=True)
def reset_internal_kv():
    yield
    _internal_kv_reset()


@pytest.fixture
def v1_logs(caplog):
    logger = _capture(caplog, V1_LOGGER)
    yield caplog
    logger.removeHandler(caplog.handler)


@pytest.fixture
def v2_logs(caplog):
    logger = _capture(caplog, V2_LOGGER)
    yield caplog
    logger.removeHandler(caplog.handler)


@pytest.fixture
def make_v1_monitor(monkeypatch):
    def factory(*, leader):
        gcs_client = _gcs_client(leader=leader)
        monkeypatch.setattr(
            v1_monitor_module, "GcsClient", lambda *args, **kwargs: gcs_client
        )
        # Otherwise the constructor binds a real metrics HTTP server.
        monkeypatch.setattr(v1_monitor_module, "prometheus_client", None)
        return Monitor(
            "127.0.0.1:6379",
            autoscaling_config=None,
            monitor_ip="1.2.3.4",
        )

    return factory


@pytest.fixture
def make_v2_monitor(monkeypatch):
    def factory(*, leader):
        gcs_client = _gcs_client(leader=leader)
        monkeypatch.setattr(
            v2_monitor_module, "GcsClient", lambda *args, **kwargs: gcs_client
        )
        monkeypatch.setattr(v2_monitor_module, "prometheus_client", None)
        monkeypatch.setattr(
            v2_monitor_module, "Autoscaler", lambda *args, **kwargs: MagicMock()
        )
        return AutoscalerMonitor(
            "127.0.0.1:6379",
            config_reader=MagicMock(),
            monitor_ip="1.2.3.4",
        )

    return factory


class _StopLoop(Exception):
    pass


def _run_passes(monitor, monkeypatch, module, count):
    """Run `count` passes of the monitor loop, then break out of it."""
    passes = []

    def stop_after_count(seconds):
        passes.append(seconds)
        if len(passes) >= count:
            raise _StopLoop()

    monkeypatch.setattr(module.time, "sleep", stop_after_count)
    with pytest.raises(_StopLoop):
        monitor._run()
    return passes


# ---------------------------------------------------------------- autoscaler v1


def test_v1_monitor_starts_against_a_passive_gcs(make_v1_monitor):
    monitor = make_v1_monitor(leader=False)

    # The write is its own probe: attempted once, refused, no CheckAlive needed.
    assert len(_metrics_address_writes(monitor.gcs_client)) == 1
    monitor.gcs_client.is_gcs_leader.assert_not_called()
    assert monitor._waiting_for_promotion


def test_v1_monitor_registers_its_metrics_address_when_leading(make_v1_monitor):
    monitor = make_v1_monitor(leader=True)

    assert not monitor._waiting_for_promotion
    assert len(_metrics_address_writes(monitor.gcs_client)) == 1


def test_v1_monitor_still_fails_to_start_against_an_unreachable_gcs(monkeypatch):
    # An unreachable GCS also yields UNAVAILABLE; only the passive one is benign.
    gcs_client = MagicMock()
    gcs_client.internal_kv_put.side_effect = RpcError(
        "Unavailable", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE
    )
    monkeypatch.setattr(
        v1_monitor_module, "GcsClient", lambda *args, **kwargs: gcs_client
    )
    monkeypatch.setattr(v1_monitor_module, "prometheus_client", None)

    with pytest.raises(RpcError):
        Monitor("127.0.0.1:6379", autoscaling_config=None, monitor_ip="1.2.3.4")


def test_v1_monitor_leaves_a_passive_rejection_alone_without_the_feature_flag(
    make_v1_monitor, monkeypatch
):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)

    with pytest.raises(GcsPassiveError):
        make_v1_monitor(leader=False)


def test_v1_monitor_does_not_scale_while_the_gcs_is_passive(
    make_v1_monitor, monkeypatch
):
    monitor = make_v1_monitor(leader=False)
    monitor.autoscaler = MagicMock()

    _run_passes(monitor, monkeypatch, v1_monitor_module, 3)

    monitor.autoscaler.update.assert_not_called()
    # Not even the read the cloud provider decision is based on.
    monitor.gcs_client.get_all_resource_usage.assert_not_called()


def test_v1_monitor_reports_a_passive_gcs_once_not_once_per_pass(
    make_v1_monitor, monkeypatch, v1_logs
):
    monitor = make_v1_monitor(leader=False)
    monitor.autoscaler = MagicMock()

    _run_passes(monitor, monkeypatch, v1_monitor_module, 3)

    assert _count_logged(v1_logs, "GCS is in passive mode") == 1
    assert _count_logged(v1_logs, "Monitor: Execution exception") == 0


def test_v1_monitor_takes_over_the_active_head_keys_on_promotion(
    make_v1_monitor, monkeypatch, v1_logs
):
    monitor = make_v1_monitor(leader=False)
    monitor.autoscaler = None
    _run_passes(monitor, monkeypatch, v1_monitor_module, 1)
    assert monitor._waiting_for_promotion
    refused_writes = len(_metrics_address_writes(monitor.gcs_client))

    monitor.gcs_client.is_gcs_leader.return_value = True
    monitor.gcs_client.internal_kv_put.side_effect = None
    monitor.gcs_client.internal_kv_del.side_effect = None
    monkeypatch.setattr(
        v1_monitor_module, "get_cluster_resource_state", lambda _: MagicMock()
    )

    _run_passes(monitor, monkeypatch, v1_monitor_module, 2)

    assert not monitor._waiting_for_promotion
    # Both keys describe the autoscaler currently running the cluster, and both
    # are taken over exactly once however many passes follow the promotion.
    assert len(_metrics_address_writes(monitor.gcs_client)) == refused_writes + 1
    assert monitor.gcs_client.internal_kv_del.call_count == 1
    assert _count_logged(v1_logs, "Resuming autoscaling") == 1


def test_v1_monitor_takes_the_keys_over_on_each_promotion(make_v1_monitor, monkeypatch):
    # Both keys name whoever is leading right now, so every promotion rewrites them.
    monitor = make_v1_monitor(leader=False)
    monitor.autoscaler = None
    monkeypatch.setattr(
        v1_monitor_module, "get_cluster_resource_state", lambda _: MagicMock()
    )
    _run_passes(monitor, monkeypatch, v1_monitor_module, 1)
    refused_writes = len(_metrics_address_writes(monitor.gcs_client))

    for leader in (True, False, True):
        monitor.gcs_client.is_gcs_leader.return_value = leader
        if leader:
            monitor.gcs_client.internal_kv_put.side_effect = None
            monitor.gcs_client.internal_kv_del.side_effect = None
        else:
            monitor.gcs_client.internal_kv_put.side_effect = _passive_gcs_rejection()
            monitor.gcs_client.internal_kv_del.side_effect = _passive_gcs_rejection()
        _run_passes(monitor, monkeypatch, v1_monitor_module, 1)

    assert monitor.gcs_client.internal_kv_del.call_count == 2
    assert len(_metrics_address_writes(monitor.gcs_client)) == refused_writes + 2


def test_v1_monitor_reports_every_leadership_change(
    make_v1_monitor, monkeypatch, v1_logs
):
    monitor = make_v1_monitor(leader=False)
    monitor.autoscaler = None
    _run_passes(monitor, monkeypatch, v1_monitor_module, 1)

    monitor.gcs_client.is_gcs_leader.return_value = True
    monitor.gcs_client.internal_kv_put.side_effect = None
    monitor.gcs_client.internal_kv_del.side_effect = None
    monkeypatch.setattr(
        v1_monitor_module, "get_cluster_resource_state", lambda _: MagicMock()
    )
    _run_passes(monitor, monkeypatch, v1_monitor_module, 1)

    monitor.gcs_client.is_gcs_leader.return_value = False
    _run_passes(monitor, monkeypatch, v1_monitor_module, 1)

    assert _count_logged(v1_logs, "GCS is in passive mode") == 2
    assert _count_logged(v1_logs, "Resuming autoscaling") == 1
    assert monitor._waiting_for_promotion


def test_v1_monitor_still_retries_other_failures(make_v1_monitor, monkeypatch, v1_logs):
    monitor = make_v1_monitor(leader=True)
    monitor.gcs_client.get_all_resource_usage.side_effect = RpcError(
        "Unavailable", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE
    )
    monitor.autoscaler = MagicMock()

    _run_passes(monitor, monkeypatch, v1_monitor_module, 2)

    assert _count_logged(v1_logs, "Monitor: Execution exception") == 2
    assert not monitor._waiting_for_promotion


def test_v1_monitor_run_reaches_the_loop_on_a_passive_gcs(make_v1_monitor):
    monitor = make_v1_monitor(leader=False)
    monitor._initialize_autoscaler = MagicMock()
    monitor._run = MagicMock(side_effect=_StopLoop())
    monitor._handle_failure = MagicMock()

    with pytest.raises(_StopLoop):
        monitor.run()

    # The refused DEBUG_AUTOSCALING_ERROR reset is not fatal; it replays on promotion.
    monitor.gcs_client.internal_kv_del.assert_called_once()
    monitor._initialize_autoscaler.assert_called_once()


def test_v1_monitor_does_not_report_a_parked_autoscaler_as_dead(make_v1_monitor):
    # Defensive: nothing under run() is expected to leak a passive rejection.
    monitor = make_v1_monitor(leader=False)
    monitor._initialize_autoscaler = MagicMock(side_effect=_passive_gcs_rejection())
    monitor._handle_failure = MagicMock()

    with pytest.raises(GcsPassiveError):
        monitor.run()

    monitor._handle_failure.assert_not_called()


def test_v1_monitor_reports_a_real_failure_even_when_the_gcs_refuses_the_record(
    make_v1_monitor, monkeypatch
):
    monitor = make_v1_monitor(leader=False)
    monitor.autoscaler = None
    published = MagicMock()
    monkeypatch.setattr("ray._private.utils.publish_error_to_driver", published)

    monitor._handle_failure("boom")

    # The refused DEBUG_AUTOSCALING_ERROR write must not swallow the report.
    published.assert_called_once()


def test_v1_monitor_leaves_the_leaders_workers_alone_when_passive(
    make_v1_monitor, monkeypatch
):
    monkeypatch.setenv("RAY_AUTOSCALER_FATESHARE_WORKERS", "1")
    monitor = make_v1_monitor(leader=False)
    monitor.autoscaler = MagicMock()
    monitor.destroy_autoscaler_workers = MagicMock()
    monkeypatch.setattr("ray._private.utils.publish_error_to_driver", MagicMock())

    monitor._handle_failure("boom")

    monitor.autoscaler.kill_workers.assert_not_called()
    monitor.destroy_autoscaler_workers.assert_not_called()


# ---------------------------------------------------------------- autoscaler v2


def test_v2_monitor_starts_against_a_passive_gcs(make_v2_monitor):
    monitor = make_v2_monitor(leader=False)

    # The write is its own probe: attempted once, refused, no CheckAlive needed.
    assert len(_metrics_address_writes(monitor.gcs_client)) == 1
    monitor.gcs_client.is_gcs_leader.assert_not_called()
    assert monitor._waiting_for_promotion


def test_v2_monitor_registers_its_metrics_address_when_leading(make_v2_monitor):
    monitor = make_v2_monitor(leader=True)

    assert not monitor._waiting_for_promotion
    assert len(_metrics_address_writes(monitor.gcs_client)) == 1


def test_v2_monitor_still_fails_to_start_against_an_unreachable_gcs(monkeypatch):
    gcs_client = MagicMock()
    gcs_client.internal_kv_put.side_effect = RpcError(
        "Unavailable", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE
    )
    monkeypatch.setattr(
        v2_monitor_module, "GcsClient", lambda *args, **kwargs: gcs_client
    )
    monkeypatch.setattr(v2_monitor_module, "prometheus_client", None)

    with pytest.raises(RpcError):
        AutoscalerMonitor(
            "127.0.0.1:6379", config_reader=MagicMock(), monitor_ip="1.2.3.4"
        )


def test_v2_monitor_does_not_scale_while_the_gcs_is_passive(
    make_v2_monitor, monkeypatch, v2_logs
):
    monitor = make_v2_monitor(leader=False)

    passes = _run_passes(monitor, monkeypatch, v2_monitor_module, 3)

    assert len(passes) == 3
    monitor.autoscaler.update_autoscaling_state.assert_not_called()
    assert _count_logged(v2_logs, "GCS is in passive mode") == 1
    assert _count_logged(v2_logs, "No autoscaling state to report") == 0


def test_v2_monitor_takes_over_the_metrics_address_on_promotion(
    make_v2_monitor, monkeypatch, v2_logs
):
    monitor = make_v2_monitor(leader=False)
    _run_passes(monitor, monkeypatch, v2_monitor_module, 1)
    assert monitor._waiting_for_promotion
    refused_writes = len(_metrics_address_writes(monitor.gcs_client))

    monitor.gcs_client.is_gcs_leader.return_value = True
    monitor.gcs_client.internal_kv_put.side_effect = None

    _run_passes(monitor, monkeypatch, v2_monitor_module, 2)

    assert not monitor._waiting_for_promotion
    assert len(_metrics_address_writes(monitor.gcs_client)) == refused_writes + 1
    assert _count_logged(v2_logs, "Resuming autoscaling") == 1


def test_v2_monitor_survives_a_demotion_racing_the_promotion_write(
    make_v2_monitor, monkeypatch, v2_logs
):
    # Nothing restarts the monitor, so a write refused between the CheckAlive that
    # said "leader" and the take-over must not be fatal.
    monitor = make_v2_monitor(leader=False)
    _run_passes(monitor, monkeypatch, v2_monitor_module, 1)

    monitor.gcs_client.is_gcs_leader.return_value = True
    passes = _run_passes(monitor, monkeypatch, v2_monitor_module, 2)

    # Recognized as a demotion rather than a crash, so the next promotion retries.
    assert len(passes) == 2
    assert _count_logged(v2_logs, "Monitor: Execution exception") == 0
    assert monitor._waiting_for_promotion


def test_v2_monitor_still_restarts_on_an_authentication_error(
    make_v2_monitor, monkeypatch
):
    monitor = make_v2_monitor(leader=True)
    monitor.autoscaler.update_autoscaling_state.side_effect = AuthenticationError(
        "WrongClusterID"
    )

    monkeypatch.setattr(v2_monitor_module.time, "sleep", MagicMock())
    with pytest.raises(AuthenticationError):
        monitor._run()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
