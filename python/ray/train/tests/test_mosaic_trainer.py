import pytest

# torch libraries
import torch
import torch.utils.data

# torchvision libraries
import torchvision
from torchvision import transforms, datasets

# Import Ray libraries
import ray
from ray.air.config import ScalingConfig
import ray.train as train
from ray.air import session

# import ray-mosaic integration libraries
from ray.train.mosaic import MosaicTrainer

# import composer training libraries
from torchmetrics.classification.accuracy import Accuracy
from composer.core.evaluator import Evaluator
from composer.models.tasks import ComposerClassifier
import composer.optim
from composer.loggers import InMemoryLogger
from composer.algorithms import LabelSmoothing


mean = (0.507, 0.487, 0.441)
std = (0.267, 0.256, 0.276)
cifar10_transforms = transforms.Compose(
    [transforms.ToTensor(), transforms.Normalize(mean, std)]
)
# data_directory = "./data" ## TODO : remove the following line
data_directory = "~/Desktop/workspace/data"
train_dataset = datasets.CIFAR10(
    data_directory, train=True, download=True, transform=cifar10_transforms
)
test_dataset = datasets.CIFAR10(
    data_directory, train=False, download=True, transform=cifar10_transforms
)

scaling_config = ScalingConfig(num_workers=2, use_gpu=False)


@pytest.fixture(autouse=True, scope="session")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def trainer_init_per_worker(**config):
    BATCH_SIZE = 1024
    # prepare the model for distributed training and wrap with ComposerClassifier for
    # Composer Trainer compatibility
    model = config.pop("model", torchvision.models.resnet18(num_classes=10))
    model = ComposerClassifier(ray.train.torch.prepare_model(model))

    # prepare train/test dataset
    train_dataset = config.pop("train_dataset")
    test_dataset = config.pop("test_dataset")

    batch_size_per_worker = BATCH_SIZE // session.get_world_size()
    train_dataloader = torch.utils.data.DataLoader(
        train_dataset, batch_size=batch_size_per_worker, shuffle=True
    )
    test_dataloader = torch.utils.data.DataLoader(
        test_dataset, batch_size=batch_size_per_worker, shuffle=True
    )

    train_dataloader = train.torch.prepare_data_loader(train_dataloader)
    test_dataloader = train.torch.prepare_data_loader(test_dataloader)

    evaluator = Evaluator(
        dataloader=test_dataloader, label="my_evaluator", metrics=Accuracy()
    )

    # prepare optimizer
    optimizer = composer.optim.DecoupledSGDW(
        model.parameters(),
        lr=0.05,
        momentum=0.9,
        weight_decay=2.0e-3,
    )

    if config.pop("eval", False):
        config["eval_dataloader"] = evaluator

    return composer.trainer.Trainer(
        model=model, train_dataloader=train_dataloader, optimizers=optimizer, **config
    )


trainer_init_per_worker.__test__ = False


def test_mosaic_e2e():
    """Tests if the basic MosaicTrainer with minimum configuration runs and reports correct
    Checkpoint dictionary.
    """
    trainer_init_config = {
        "max_duration": "1ba",
        "train_dataset": train_dataset,
        "test_dataset": test_dataset,
        "loggers": [InMemoryLogger()],
        "algorithms": [LabelSmoothing()],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
