# coding: utf-8
import sys

import pytest

import ray
import ray.workers.setup_worker


def test_parse_allocated_resource():
    cpu_share = ray.workers.setup_worker.parse_allocated_resource(
        r'{"CPU":20000,"memory":10737418240000}'
    )
    assert cpu_share == ["--cpu-shares=2048", "--memory=1073741824"]
    cpu_set = ray.workers.setup_worker.parse_allocated_resource(
        r'{"CPU":[10000,0,10000],"memory":10737418240000}'
    )
    assert cpu_set == ["--cpu-shares=2048", "--cpuset-cpus=0,2", "--memory=1073741824"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
