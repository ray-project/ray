import pytest

import horovod.torch as hvd_torch

import ray
from ray.autoscaler._private.fake_multi_node.test_utils import DockerCluster

from ray.train import Trainer


@pytest.fixture
def ray_start_2_node_2_cpu_each():
    cluster = DockerCluster()
    cluster.setup()
    cluster.update_config(
        {
            "provider": {"head_resources": {"CPU": 2, "GPU": 0}},
            "available_node_types": {
                "ray.head.default": {"resources": {"CPU": 2}},
                "ray.worker.cpu": {
                    "resources": {"CPU": 2},
                    "min_workers": 1,
                    "max_workers": 1,
                },
                "ray.worker.gpu": {
                    "min_workers": 0,
                    "max_workers": 0,  # No GPU nodes
                },
            },
        }
    )
    cluster.start()
    cluster.connect(client=True, timeout=500, runtime_env={"pip": ["torch"]})
    # ray.init("127.0.0.1:16379")
    cluster.wait_for_resources({"CPU": 4})
    assert len(ray.nodes()) >= 2
    yield
    cluster.stop()
    cluster.teardown()


def test_horovod_simple_multi_node(ray_start_2_node_2_cpu_each):
    def simple_fn():
        # hvd_torch.init()
        # return hvd_torch.rank()
        return 1

    num_workers = 4
    trainer = Trainer("torch", num_workers)
    trainer.start()
    result = trainer.run(simple_fn)
    trainer.shutdown()

    assert result == list(range(num_workers))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
