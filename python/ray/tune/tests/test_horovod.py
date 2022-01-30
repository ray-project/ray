import pytest

import ray
from ray import tune

pytest.importorskip("horovod")

try:
    from ray.tune.integration.horovod import (
        DistributedTrainableCreator,
        _train_simple,
        _train_validate_session,
    )
except ImportError:
    pass  # This shouldn't be reached - the test should be skipped.


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
def ray_connect_cluster():
    try:
        address_info = ray.init(address="auto")
    except Exception as e:
        pytest.skip(str(e))
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_single_step(ray_start_2_cpus):
    trainable_cls = DistributedTrainableCreator(_train_simple, num_hosts=1, num_slots=2)
    trainer = trainable_cls()
    trainer.train()
    trainer.stop()


def test_step_after_completion(ray_start_2_cpus):
    trainable_cls = DistributedTrainableCreator(_train_simple, num_hosts=1, num_slots=2)
    trainer = trainable_cls(config={"epochs": 1})
    with pytest.raises(RuntimeError):
        for i in range(10):
            trainer.train()


def test_validation(ray_start_2_cpus):
    def bad_func(a, b, c):
        return 1

    t_cls = DistributedTrainableCreator(bad_func, num_slots=2)
    with pytest.raises(ValueError):
        t_cls()


def test_set_global(ray_start_2_cpus):
    trainable_cls = DistributedTrainableCreator(_train_simple, num_slots=2)
    trainable = trainable_cls()
    result = trainable.train()
    trainable.stop()
    assert result["rank"] == 0


@pytest.mark.parametrize("enabled_checkpoint", [True, False])
def test_simple_tune(ray_start_4_cpus, enabled_checkpoint):
    trainable_cls = DistributedTrainableCreator(_train_simple, num_slots=2)
    analysis = tune.run(
        trainable_cls,
        config={"enable_checkpoint": enabled_checkpoint},
        num_samples=2,
        stop={"training_iteration": 2},
    )
    assert analysis.trials[0].last_result["training_iteration"] == 2
    assert analysis.trials[0].has_checkpoint() == enabled_checkpoint


@pytest.mark.parametrize("use_gpu", [True, False])
def test_resource_tune(ray_connect_cluster, use_gpu):
    if use_gpu and ray.cluster_resources().get("GPU", 0) == 0:
        pytest.skip("No GPU available.")
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_slots=2, use_gpu=use_gpu
    )
    analysis = tune.run(trainable_cls, num_samples=2, stop={"training_iteration": 2})
    assert analysis.trials[0].last_result["training_iteration"] == 2


def test_validate_session(ray_start_2_cpus):
    trainable_cls = DistributedTrainableCreator(_train_validate_session)
    tune.run(trainable_cls)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
