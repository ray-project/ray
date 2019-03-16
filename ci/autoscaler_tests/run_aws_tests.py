from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import yaml
import subprocess
import pytest
from ray.autoscaler.aws.node_provider import AWSNodeProvider
from ray.autoscaler.tags import TAG_RAY_NODE_TYPE
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import time

import ray


def _check(cond, timeout=60):
    for i in timeout:
        time.sleep(1)
        if cond():
            return True
    return False


@pytest.fixture
def setup_file_path():
    cluster_file_path = os.path.join(
        os.path.dirname(ray.__file__), "autoscaler/aws/example-full.yaml")
    yield cluster_file_path
    subprocess.check_call(("ray down {cluster_file_path} -y".format(
        cluster_file_path=cluster_file_path).split()))
    subprocess.check_call(["ray", "stop"])


def test_ray_up_down(setup_file_path):
    cluster_file_path = setup_file_path
    subprocess.check_call(("ray up {cluster_file_path} -y".format(
        cluster_file_path=cluster_file_path).split()))
    with open(cluster_file_path) as f:
        cfg = yaml.load(f)

    provider = AWSNodeProvider(cfg["provider"], cfg["cluster_name"])

    assert len(provider.nodes({TAG_RAY_NODE_TYPE: "head"})) == 1
    assert not provider.nodes({TAG_RAY_NODE_TYPE: "worker"})

    subprocess.check_call(
        list("ray down {cluster_file_path} -y".format(
            cluster_file_path=cluster_file_path).split()))

    assert not provider.nodes({TAG_RAY_NODE_TYPE: "head"})
    assert not provider.nodes({TAG_RAY_NODE_TYPE: "worker"})


def test_ray_autoscale(setup_file_path):
    cluster_file_path = setup_file_path
    subprocess.check_call(("ray up {cluster_file_path} -y".format(
        cluster_file_path=cluster_file_path).split()))
    with open(cluster_file_path) as f:
        cfg = yaml.load(f)

    provider = AWSNodeProvider(cfg["provider"], cfg["cluster_name"])
    assert len(list(provider.nodes({TAG_RAY_NODE_TYPE: "head"}))) == 1
    assert len(list(provider.nodes({TAG_RAY_NODE_TYPE: "worker"}))) == 0

    workload_file_path = os.path.join(
        os.path.dirname(ray.__file__), "../tune_workload.py")

    subprocess.check_call(
        list("ray submit {cluster_file_path} --tmux {script}".format(
            cluster_file_path=cluster_file_path,
            script=workload_file_path).split()))

    assert _check(lambda: list(
        provider.nodes({TAG_RAY_NODE_TYPE: "worker"})) == 2)

    # TODO(rliaw): Implement a scaledown check

    subprocess.check_call(
        list("ray down {cluster_file_path} -y".format(
            cluster_file_path=cluster_file_path).split()))

    assert len(list(provider.nodes({TAG_RAY_NODE_TYPE: "head"}))) == 0
    assert len(list(provider.nodes({TAG_RAY_NODE_TYPE: "worker"}))) == 0


def test_ray_exec(setup_file_path):
    cluster_file_path = setup_file_path
    subprocess.check_call(
        ("ray exec {cluster_file_path} 'ls && sleep 10' --start --stop --tmux".
         format(cluster_file_path=cluster_file_path).split()))

    with open(cluster_file_path) as f:
        cfg = yaml.load(f)

    provider = AWSNodeProvider(cfg["provider"], cfg["cluster_name"])

    assert _check(
        lambda: list(provider.nodes({TAG_RAY_NODE_TYPE: "head"})), timeout=30)
    # Stop runs after
    assert _check(lambda: not list(provider.nodes(
        {TAG_RAY_NODE_TYPE: "head"})))
