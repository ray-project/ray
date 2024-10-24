import pytest

import ray
from ray.data import StreamingAggFn
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize(
    "num_input_blocks,num_samples_per_id,num_aggregators",
    [
        (1, 1, 1),
        (10, 10, 2),
        (100, 100, 5),
        (500, 500, 10),
    ],
)
def test_streaming_aggregate(
    shutdown_only,
    num_input_blocks,
    num_samples_per_id,
    num_aggregators,
):
    ray.init(num_cpus=num_aggregators * 3)
    ds = ray.data.range(num_input_blocks, override_num_blocks=num_input_blocks)

    def generate_samples(batch):
        assert len(batch["id"]) == 1
        id = batch["id"][0]
        for i in range(num_samples_per_id):
            yield {"id": [id], "value": [i * id]}

    ds = ds.map_batches(generate_samples, batch_size=None)

    class AverageAggFn(StreamingAggFn):
        def init_state(self, key):
            return {"id": key, "count": 0, "sum": 0}

        def aggregate_row(self, key, state, row):
            assert key == state["id"]
            state["count"] += 1
            state["sum"] += row["value"]
            if state["count"] == num_samples_per_id:
                state = {"id": state["id"], "avg": state["sum"] / state["count"]}
                return state, True
            return state, False

    ds = ds.streaming_aggregate("id", AverageAggFn(), num_aggregators=num_aggregators)
    res = ds.take_all()
    assert len(res) == num_input_blocks
    res = sorted(res, key=lambda item: item["id"])
    expected_avg = (num_samples_per_id - 1) / 2
    assert res == [{"id": i, "avg": i * expected_avg} for i in range(num_input_blocks)]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
