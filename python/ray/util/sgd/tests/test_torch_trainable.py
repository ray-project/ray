import pytest
import torch
import torch.distributed as dist

import ray
from ray import tune
from ray.util.sgd.torch.func_trainable import (
    DistributedTrainableCreator, distributed_checkpoint, _train_simple)


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Ensure that tests don't ALL fail
    if dist.is_initialized():
        dist.destroy_process_group()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Ensure that tests don't ALL fail
    if dist.is_initialized():
        dist.destroy_process_group()


# def test_simple(ray_start_2_cpus):
#     _train_simple(None)


def test_single_step(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(_train_simple, num_workers=2)
    trainer = trainable_cls()
    result = trainer.train()
    print(result)
    trainer.stop()


def test_step_after_completion(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(_train_simple, num_workers=2)
    print("start")
    trainer = trainable_cls(config={"epochs": 1})
    with pytest.raises(RuntimeError):
        for i in range(10):
            result = trainer.train()


def test_save_checkpoint(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(_train_simple, num_workers=2)
    trainer = trainable_cls(config={"epochs": 1})
    result = trainer.train()
    path = trainer.save()
    model_state_dict, opt_state_dict = torch.load(path)
    print(result)
    trainer.stop()


@pytest.mark.parametrize("enabled_checkpoint", [True, False])
def test_simple_tune(ray_start_4_cpus, enabled_checkpoint):
    trainable_cls = DistributedTrainableCreator(_train_simple, num_workers=2)
    analysis = tune.run(
        trainable_cls,
        config={"enable_checkpoint": enabled_checkpoint},
        num_samples=2,
        stop={"training_iteration": 2})
    assert analysis.trials[0].last_result["training_iteration"] == 2
    assert analysis.trials[0].has_checkpoint() == enabled_checkpoint


# def test_resize(ray_start_2_cpus):  # noqa: F811
#     trainer = TorchTrainer(
#         model_creator=model_creator,
#         data_creator=data_creator,
#         optimizer_creator=optimizer_creator,
#         loss_creator=lambda config: nn.MSELoss(),
#         num_workers=1)
#     trainer.train(num_steps=1)
#     trainer.max_replicas = 2
#     results = trainer.train(num_steps=1, reduce_results=False)
#     assert len(results) == 2

if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
