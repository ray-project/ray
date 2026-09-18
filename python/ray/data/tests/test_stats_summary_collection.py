import pytest

import ray
from ray.data import list_stats_summaries
from ray.data._internal.stats_summary_server import clear_stats_summaries


@pytest.fixture
def enable_stats_summary_collection(restore_data_context):
    ray.data.DataContext.get_current().enable_stats_summary_collection = True
    clear_stats_summaries()


def test_list_stats_summaries_ordered_by_completion(
    ray_start_regular_shared, enable_stats_summary_collection, tmp_path
):
    ray.data.range(1).write_parquet(str(tmp_path / "a"))
    ray.data.range(2).write_parquet(str(tmp_path / "b"))

    # Use row counts to identify datasets
    summaries = list_stats_summaries()
    assert len(summaries) == 2
    assert summaries[-1].operators_stats[-1].output_num_rows.sum == 2
    assert summaries[-2].operators_stats[-1].output_num_rows.sum == 1


def test_list_stats_summaries_records_failed_execution(
    ray_start_regular_shared, enable_stats_summary_collection
):
    def _boom(batch):
        raise ValueError("boom")

    with pytest.raises(Exception):
        ray.data.range(1).map_batches(_boom).materialize()

    assert len(list_stats_summaries()) == 1


def test_list_stats_summaries_records_streaming_split_execution(
    ray_start_regular_shared, enable_stats_summary_collection
):
    # `streaming_split` runs its executor inside the `SplitCoordinator` actor, so the
    # summary is reported from a different process than the one reading it back.
    splits = ray.data.range(1).streaming_split(2)

    @ray.remote
    def consume(it):
        return list(it.iter_batches())

    ray.get([consume.remote(split) for split in splits])

    assert len(list_stats_summaries()) > 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
