from typing import Dict, Optional

import pytest
import ray
from ray import tune
from ray.cluster_utils import Cluster
from ray.tune.integration.tensorflow import DistributedTrainableCreator
from ray.tune.examples.tf_distributed_keras_example import train_mnist


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_4_node():
    cluster = Cluster()
    for _ in range(4):
        cluster.add_node(num_cpus=1)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_4_node_gpu():
    cluster = Cluster()
    for _ in range(4):
        cluster.add_node(num_cpus=2, num_gpus=2)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_connect_cluster():
    try:
        address_info = ray.init(address="auto")
    except Exception as e:
        pytest.skip(str(e))
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def _train_check_global(config: Dict, checkpoint_dir: Optional[str] = None):
    """For testing only. Putting this here because Ray has problems
    serializing within the test file."""
    import time

    time.sleep(0.1)
    tune.report(is_distributed=True)


def _train_validate_session(config: Dict, checkpoint_dir: Optional[str] = None):
    current_session = tune.session.get_session()
    assert current_session is not None
    assert current_session.trial_id != "default"
    assert current_session.trial_name != "default"


def test_single_step(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(train_mnist, num_workers=2)
    trainer = trainable_cls()
    trainer.train()
    trainer.stop()


def test_step_after_completion(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(train_mnist, num_workers=2)
    trainer = trainable_cls(config={"epochs": 1})
    with pytest.raises(RuntimeError):
        for i in range(10):
            trainer.train()


def test_validation(ray_start_2_cpus):  # noqa: F811
    def bad_func(a, b, c):
        return 1

    with pytest.raises(ValueError):
        DistributedTrainableCreator(bad_func)


def test_colocated(ray_4_node):  # noqa: F811
    assert ray.available_resources()["CPU"] == 4
    trainable_cls = DistributedTrainableCreator(
        _train_check_global, num_workers=4, num_workers_per_host=1
    )
    trainable = trainable_cls()
    assert ray.available_resources().get("CPU", 0) == 0
    trainable.train()
    trainable.stop()


def test_colocated_gpu(ray_4_node_gpu):  # noqa: F811
    assert ray.available_resources()["GPU"] == 8
    trainable_cls = DistributedTrainableCreator(
        _train_check_global,
        num_workers=4,
        num_gpus_per_worker=2,
        num_workers_per_host=1,
    )
    trainable = trainable_cls()
    assert ray.available_resources().get("GPU", 0) == 0
    trainable.train()
    trainable.stop()


def test_colocated_gpu_double(ray_4_node_gpu):  # noqa: F811
    assert ray.available_resources()["GPU"] == 8
    trainable_cls = DistributedTrainableCreator(
        _train_check_global,
        num_workers=8,
        num_gpus_per_worker=1,
        num_cpus_per_worker=1,
        num_workers_per_host=2,
    )
    trainable = trainable_cls()
    assert ray.available_resources().get("GPU", 0) == 0
    trainable.train()
    trainable.stop()


def test_validate_session(ray_start_2_cpus):
    trainable_cls = DistributedTrainableCreator(_train_validate_session)
    tune.run(trainable_cls)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
