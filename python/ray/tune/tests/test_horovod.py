import os
import pytest

import ray
from ray import tune
from ray.tune.integration.horovod import (DistributedTrainableCreator,
                                          _train_simple)

try:
    import horovod
except ImportError:
    horovod = None
# pytest.importorskip("horovod")


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
def ray_start_4_cpus_4_gpus():
    os.environ["CUDA_VISIBLE_DEVICES"] = "0,1,2,3"
    address_info = ray.init(num_cpus=4, num_gpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    del os.environ["CUDA_VISIBLE_DEVICES"]


@pytest.fixture
def ray_connect_cluster():
    try:
        address_info = ray.init(address="auto")
    except Exception as e:
        pytest.skip(str(e))
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
def test_single_step(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_nodes=1, num_workers_per_node=2)
    trainer = trainable_cls()
    trainer.train()
    trainer.stop()


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
def test_step_after_completion(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_nodes=1, num_workers_per_node=2)
    trainer = trainable_cls(config={"epochs": 1})
    with pytest.raises(RuntimeError):
        for i in range(10):
            trainer.train()


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
def test_validation(ray_start_2_cpus):  # noqa: F811
    def bad_func(a, b, c):
        return 1

    with pytest.raises(ValueError):
        DistributedTrainableCreator(bad_func, num_workers_per_node=2)


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
def test_set_global(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_workers_per_node=2)
    trainable = trainable_cls()
    result = trainable.train()
    trainable.stop()
    assert result["rank"] == 0


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
def test_simple_tune(ray_start_4_cpus):
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_workers_per_node=2)
    analysis = tune.run(
        trainable_cls, num_samples=2, stop={"training_iteration": 2})
    assert analysis.trials[0].last_result["training_iteration"] == 2


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
@pytest.mark.parametrize("use_gpu", [True, False])
def test_resource_tune(ray_connect_cluster, use_gpu):
    if use_gpu and not ray.cluster_resources().get("GPU", 0):
        pytest.skip("No GPU available.")
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_workers_per_node=2, use_gpu=use_gpu)
    analysis = tune.run(
        trainable_cls, num_samples=2, stop={"training_iteration": 2})
    assert analysis.trials[0].last_result["training_iteration"] == 2


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
