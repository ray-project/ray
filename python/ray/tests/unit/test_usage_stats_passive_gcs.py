# Unit tests for how usage reporting behaves on a passive GCS.
import json
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import ray._common.usage.usage_lib as ray_usage_lib
from ray.dashboard.modules.usage_stats.usage_stats_head import UsageStatsHead
from ray.tests.unit.passive_test_utils import make_dummy_head_config

pytestmark = pytest.mark.usefixtures("leader_election_on")


@pytest.fixture
def make_usage_head(tmp_path):
    def factory(*, leader, usage_stats_enabled=True):
        gcs_client = MagicMock()
        gcs_client.is_gcs_leader_local.return_value = leader
        with patch.object(
            ray_usage_lib, "usage_stats_enabled", return_value=usage_stats_enabled
        ):
            return UsageStatsHead(
                make_dummy_head_config(tmp_path, gcs_client=gcs_client)
            )

    return factory


# ==============================================================================
# Usage reporting
# ==============================================================================


@pytest.mark.parametrize("leader, should_report", [(True, True), (False, False)])
async def test_usage_reporting_respects_leadership(
    make_usage_head, leader, should_report
):
    head = make_usage_head(leader=leader)

    with patch.object(head, "_report_usage_sync") as report:
        await head._report_usage_async()

    assert report.called is should_report


@pytest.mark.parametrize("leader, should_send", [(True, True), (False, False)])
async def test_disabled_usage_ping_respects_leadership(
    make_usage_head, leader, should_send
):
    head = make_usage_head(leader=leader, usage_stats_enabled=False)

    with patch.object(head, "_report_disabled_usage_sync") as report:
        await head._report_disabled_usage_async()

    assert report.called is should_send


async def test_reporting_resumes_after_a_promotion(make_usage_head):
    head = make_usage_head(leader=False)

    with patch.object(head, "_report_usage_sync") as report:
        await head._report_usage_async()
        head.gcs_client.is_gcs_leader_local.return_value = True
        await head._report_usage_async()

    report.assert_called_once()


# ==============================================================================
# Cluster metadata reads
# ==============================================================================


def _reader(stored):
    gcs_client = MagicMock()
    gcs_client.internal_kv_get.return_value = stored
    return gcs_client


def test_cluster_metadata_helpers():
    assert ray_usage_lib.get_cluster_metadata(_reader(None)) is None
    assert ray_usage_lib.is_ray_init_cluster(_reader(None)) is False

    stored = json.dumps({"ray_version": "1.2.3"}).encode()
    assert ray_usage_lib.get_cluster_metadata(_reader(stored)) == {
        "ray_version": "1.2.3"
    }


# ==============================================================================
# Usage tag replay
# ==============================================================================


def _tag_writes(gcs_client):
    return [call.args[0] for call in gcs_client.internal_kv_put.call_args_list]


def _async_tag_writes(gcs_client):
    return [call.args[0] for call in gcs_client.async_internal_kv_put.call_args_list]


@pytest.mark.parametrize(
    "tags",
    [
        [],
        [ray_usage_lib.TagKey._TEST1],
        [ray_usage_lib.TagKey._TEST1, ray_usage_lib.TagKey._TEST2],
    ],
)
def test_recorded_extra_usage_tags_replay(tags):
    gcs_client = MagicMock()
    gcs_client.internal_kv_put.side_effect = RuntimeError("passive rejection")
    for tag in tags:
        ray_usage_lib.record_extra_usage_tag(tag, "val", gcs_client)
    gcs_client.internal_kv_put.reset_mock(side_effect=True)

    ray_usage_lib.put_recorded_extra_usage_tags(gcs_client)

    expected = sorted(
        [
            f"extra_usage_tag_{ray_usage_lib.TagKey.Name(tag).lower()}".encode()
            for tag in tags
        ]
    )
    assert sorted(_tag_writes(gcs_client)) == expected


@pytest.mark.parametrize(
    "tags",
    [
        [ray_usage_lib.TagKey._TEST1],
        [ray_usage_lib.TagKey._TEST1, ray_usage_lib.TagKey._TEST2],
    ],
)
@pytest.mark.asyncio
async def test_async_recorded_extra_usage_tags_replay(tags):
    gcs_client = MagicMock()
    gcs_client.internal_kv_put.side_effect = RuntimeError("passive rejection")
    for tag in tags:
        ray_usage_lib.record_extra_usage_tag(tag, "val", gcs_client)
    gcs_client.async_internal_kv_put = AsyncMock()

    await ray_usage_lib.async_put_recorded_extra_usage_tags(gcs_client)

    expected = sorted(
        [
            f"extra_usage_tag_{ray_usage_lib.TagKey.Name(tag).lower()}".encode()
            for tag in tags
        ]
    )
    assert sorted(_async_tag_writes(gcs_client)) == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
