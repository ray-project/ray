import pytest
import sys

import ray

try:
    import pyspark  # noqa
except ImportError:
    pyspark = None


@pytest.mark.skipif(pyspark is None, reason="PySpark dependency not found")
@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --num-cpus=1 --min-worker-port=0 "
        "--max-worker-port=0 --port 0 --ray-client-server-port 10002",
    ],
    indirect=True,
)
def test_client_data_get(call_ray_start):
    """PySpark import changes NamedTuple pickling behavior, leading
    to inconpatibilities with the Ray client and Ray Data. This test
    makes sure that our fix in the ClientPickler works."""
    address = call_ray_start
    ip = address.split(":")[0]

    ray.util.connect(f"{ip}:10002")

    ray_pipeline = ray.data.from_items(list(range(1_000)))
    ray.get(ray_pipeline.to_numpy_refs()[0])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
