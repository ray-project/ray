from mock import patch
import pytest
import os
from ray.util.accelerators.tpu import pod_name, pod_worker_count


def test_pod_name_smoke():
    with patch("ray._private.accelerator.get_tpu_id", return_value="my-tpu"):
       name = pod_name()
    assert name == "my-tpu"


def test_empty_pod_name_returns_none():
    with patch("ray._private.accelerator.get_tpu_id", return_value=""):
        name = pod_name()
    assert name == None


def test_worker_count():
    with patch("ray._private.accelerator.num_workers_in_tpu_pod", return_value=4):
        worker_count = pod_worker_count()
    assert worker_count == 4


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))

