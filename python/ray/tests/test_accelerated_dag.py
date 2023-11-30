# coding: utf-8
import logging
import os
import sys

import pytest

import ray
import ray.cluster_utils

logger = logging.getLogger(__name__)


def test_put_local_get(ray_start_regular):
    ref = ray._create_channel(1000)

    num_writes = 1000
    for i in range(num_writes):
        val = i.to_bytes(8, "little")
        ray._write_channel(val, ref, num_readers=1)
        assert ray.get(ref) == val
        ray._end_read_channel(ref)


@pytest.mark.parametrize("num_readers", [1, 4])
def test_put_remote_get(ray_start_regular, num_readers):
    ref = ray._create_channel(1000)

    @ray.remote(num_cpus=0)
    class Reader:
        def __init__(self):
            pass

        def read(self, ref, num_writes):
            for i in range(num_writes):
                val = i.to_bytes(8, "little")
                assert ray.get(ref[0]) == val
                ray._end_read_channel(ref)

    num_writes = 1000
    readers = [Reader.remote() for _ in range(num_readers)]
    done = [reader.read.remote([ref], num_writes) for reader in readers]
    for i in range(num_writes):
        val = i.to_bytes(8, "little")
        ray._write_channel(val, ref, num_readers=num_readers)

    ray.get(done)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
