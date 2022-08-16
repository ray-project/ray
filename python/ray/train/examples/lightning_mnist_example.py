import argparse
import pytorch_lightning
import torch


# model adapted from
# https://pytorch-lightning.readthedocs.io/en/stable/common/lightning_module.html#starter-example
class LitModel(pytorch_lightning.LightningModule):
    def __init__(self, learning_rate=0.02):
        super().__init__()
        self.learning_rate = learning_rate
        self.layer1 = torch.nn.Linear(28 * 28, 10)

    def forward(self, x):
        return torch.relu(self.layer1(x.view(x.size(0), -1)))

    def training_step(self, batch, batch_idx):
        x = batch["image"]
        y = batch["label"]
        y_hat = self(x)
        loss = torch.nn.functional.cross_entropy(y_hat, y)
        self.log("train_loss", loss, on_step=False, on_epoch=True, sync_dist=True)
        return loss

    def test_step(self, batch, batch_idx):
        x = batch["image"]
        y = batch["label"]
        y_hat = self(x)
        loss = torch.nn.functional.cross_entropy(y_hat, y)
        self.log("test_loss", loss)

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.learning_rate)


def get_datasets():
    import ray.data
    from ray.data.extensions import TensorArray
    from pandas import DataFrame
    from torchvision.datasets import MNIST
    from torchvision.transforms import ToTensor

    def convert_batch_to_pandas(batch):
        images = TensorArray([image.numpy() for image, _ in batch])
        labels = [label for _, label in batch]
        return DataFrame({"image": images, "label": labels})

    train_dataset = ray.data.read_datasource(
        ray.data.datasource.SimpleTorchDatasource(),
        parallelism=1,
        dataset_factory=lambda: MNIST(
            "~/data", train=True, download=True, transform=ToTensor()
        ),
    ).map_batches(convert_batch_to_pandas)
    test_dataset = ray.data.read_datasource(
        ray.data.datasource.SimpleTorchDatasource(),
        parallelism=1,
        dataset_factory=lambda: MNIST(
            "~/data", train=False, download=True, transform=ToTensor()
        ),
    ).map_batches(convert_batch_to_pandas)
    return train_dataset, test_dataset


def train_lightning_mnist(num_workers=4, use_gpu=False, epochs=40):
    from ray.train.lightning import LightningTrainer
    from ray.air.config import ScalingConfig

    train_dataset, test_dataset = get_datasets()

    trainer = LightningTrainer(
        LitModel,
        lightning_module_init_config={
            "learning_rate": 0.02
        },  # arguments that will be passed to `LitModel.__init__`
        trainer_init_config={"max_epochs": epochs},
        ddp_strategy_init_config={"find_unused_parameters": False},
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        datasets={"train": train_dataset, "test": test_dataset},
    )
    results = trainer.fit()
    print(f"Results: {results.metrics}")
    checkpoint = results.checkpoint
    print(f"Checkpoint: {checkpoint}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    parser.add_argument(
        "--epochs", type=int, default=3, help="Number of epochs to train for."
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )

    args, _ = parser.parse_known_args()

    import ray

    if args.smoke_test:
        ray.init(num_cpus=6)
        train_lightning_mnist()
    else:
        ray.init(address=args.address)
        train_lightning_mnist(
            num_workers=args.num_workers, use_gpu=args.use_gpu, epochs=args.epochs
        )
