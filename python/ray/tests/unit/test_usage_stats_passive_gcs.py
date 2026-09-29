# Unit tests for how usage reporting behaves on a passive GCS.
import json
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import ray._common.usage.usage_lib as ray_usage_lib
import ray._private.ray_constants as ray_constants
import ray.experimental.internal_kv as internal_kv
from ray.dashboard.modules.usage_stats.usage_stats_head import UsageStatsHead
from ray.dashboard.utils import DashboardHeadModuleConfig


@pytest.fixture(autouse=True)
def leader_election_on(monkeypatch):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", True)


@pytest.fixture(autouse=True)
def reset_internal_kv():
    ray_usage_lib.reset_global_state()
    yield
    internal_kv._internal_kv_reset()
    ray_usage_lib.reset_global_state()


@pytest.fixture
def make_usage_head(tmp_path):
    def factory(*, leader, usage_stats_enabled=True):
        gcs_client = MagicMock()
        gcs_client.is_gcs_leader_local.return_value = leader
        with patch.object(
            ray_usage_lib, "usage_stats_enabled", return_value=usage_stats_enabled
        ):
            return UsageStatsHead(
                DashboardHeadModuleConfig(
                    minimal=True,
                    cluster_id_hex="f" * 56,
                    session_name="session",
                    gcs_address="127.0.0.1:6379",
                    log_dir=str(tmp_path),
                    temp_dir=str(tmp_path),
                    session_dir=str(tmp_path),
                    ip="1.2.3.4",
                    http_host="1.2.3.4",
                    http_port=8265,
                    gcs_client=gcs_client,
                )
            )

    return factory


#
# Usage reporting
#


async def test_a_leader_reports(make_usage_head):
    head = make_usage_head(leader=True)

    with patch.object(head, "_report_usage_sync") as report:
        await head._report_usage_async()

    report.assert_called_once()


async def test_a_standby_does_not_report(make_usage_head):
    """Its report would carry the leader's session id but almost no data.

    Every GCS query behind the report is gated and falls back to a default, so
    without this the report is still generated and still sent -- just wrong.
    """
    head = make_usage_head(leader=False)

    with patch.object(head, "_report_usage_sync") as report:
        await head._report_usage_async()

    report.assert_not_called()


async def test_a_leader_sends_the_disabled_ping(make_usage_head):
    head = make_usage_head(leader=True, usage_stats_enabled=False)

    with patch.object(head, "_report_disabled_usage_sync") as report:
        await head._report_disabled_usage_async()

    report.assert_called_once()


async def test_a_standby_does_not_send_the_disabled_ping(make_usage_head):
    """The leader already sent it, and it is a one-shot, not a heartbeat."""
    head = make_usage_head(leader=False, usage_stats_enabled=False)

    with patch.object(head, "_report_disabled_usage_sync") as report:
        await head._report_disabled_usage_async()

    report.assert_not_called()


async def test_reporting_resumes_after_a_promotion(make_usage_head):
    head = make_usage_head(leader=False)

    with patch.object(head, "_report_usage_sync") as report:
        await head._report_usage_async()
        head.gcs_client.is_gcs_leader_local.return_value = True
        await head._report_usage_async()

    report.assert_called_once()


#
# Cluster metadata reads
#


def _reader(stored):
    gcs_client = MagicMock()
    gcs_client.internal_kv_get.return_value = stored
    return gcs_client


def test_get_cluster_metadata_returns_none_when_absent():
    assert ray_usage_lib.get_cluster_metadata(_reader(None)) is None


def test_get_cluster_metadata_decodes_a_stored_value():
    gcs_client = _reader(json.dumps({"ray_version": "1.2.3"}).encode())

    assert ray_usage_lib.get_cluster_metadata(gcs_client) == {"ray_version": "1.2.3"}


def test_is_ray_init_cluster_is_false_when_metadata_is_absent():
    assert ray_usage_lib.is_ray_init_cluster(_reader(None)) is False


#
# Usage tag replay
#


def _tag_writes(gcs_client):
    return [call.args[0] for call in gcs_client.internal_kv_put.call_args_list]


def _async_tag_writes(gcs_client):
    return [call.args[0] for call in gcs_client.async_internal_kv_put.call_args_list]


def test_a_refused_tag_is_replayed():
    """The recording sites run once, so the replay is the only second chance."""
    from ray._common.usage.usage_lib import TagKey

    gcs_client = MagicMock()
    gcs_client.internal_kv_put.side_effect = RuntimeError("passive rejection")
    ray_usage_lib.record_extra_usage_tag(TagKey._TEST1, "v1", gcs_client)
    gcs_client.internal_kv_put.reset_mock(side_effect=True)

    ray_usage_lib.put_recorded_extra_usage_tags(gcs_client)

    assert _tag_writes(gcs_client) == [b"extra_usage_tag__test1"]


def test_every_recorded_tag_is_replayed():
    from ray._common.usage.usage_lib import TagKey

    gcs_client = MagicMock()
    gcs_client.internal_kv_put.side_effect = RuntimeError("passive rejection")
    ray_usage_lib.record_extra_usage_tag(TagKey._TEST1, "v1", gcs_client)
    ray_usage_lib.record_extra_usage_tag(TagKey._TEST2, "v2", gcs_client)
    gcs_client.internal_kv_put.reset_mock(side_effect=True)

    ray_usage_lib.put_recorded_extra_usage_tags(gcs_client)

    assert sorted(_tag_writes(gcs_client)) == [
        b"extra_usage_tag__test1",
        b"extra_usage_tag__test2",
    ]


def test_the_replay_is_a_no_op_without_recorded_tags():
    gcs_client = MagicMock()

    ray_usage_lib.put_recorded_extra_usage_tags(gcs_client)

    assert _tag_writes(gcs_client) == []


@pytest.mark.asyncio
async def test_async_a_refused_tag_is_replayed():
    from ray._common.usage.usage_lib import TagKey

    gcs_client = MagicMock()
    gcs_client.internal_kv_put.side_effect = RuntimeError("passive rejection")
    ray_usage_lib.record_extra_usage_tag(TagKey._TEST1, "v1", gcs_client)
    gcs_client.async_internal_kv_put = AsyncMock()

    await ray_usage_lib.async_put_recorded_extra_usage_tags(gcs_client)

    assert _async_tag_writes(gcs_client) == [b"extra_usage_tag__test1"]


@pytest.mark.asyncio
async def test_async_every_recorded_tag_is_replayed():
    from ray._common.usage.usage_lib import TagKey

    gcs_client = MagicMock()
    gcs_client.internal_kv_put.side_effect = RuntimeError("passive rejection")
    ray_usage_lib.record_extra_usage_tag(TagKey._TEST1, "v1", gcs_client)
    ray_usage_lib.record_extra_usage_tag(TagKey._TEST2, "v2", gcs_client)
    gcs_client.async_internal_kv_put = AsyncMock()

    await ray_usage_lib.async_put_recorded_extra_usage_tags(gcs_client)

    assert sorted(_async_tag_writes(gcs_client)) == [
        b"extra_usage_tag__test1",
        b"extra_usage_tag__test2",
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
