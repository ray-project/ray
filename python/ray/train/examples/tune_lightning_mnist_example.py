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
        # TODO: should users pass `sync_dist=True` when `on_epoch=True`?
        # I think so, b/c as of now Ray just reports the first worker rather than
        # aggregating first and then reporting.
        self.log("train_loss", loss, on_epoch=True, sync_dist=True)
        return loss

    def test_step(self, batch, batch_idx):
        x = batch["image"]
        y = batch["label"]
        y_hat = self(x)
        loss = torch.nn.functional.cross_entropy(y_hat, y)
        self.log("test_loss", loss)

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.learning_rate)

    # don't set `train_dataloader`, `val_data_loader`,
    # `test_dataloader`, or `predict_dataloader` hooks here.


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


def tune_lightning_mnist(num_workers=3, use_gpu=False):
    from ray.train.lightning import LightningTrainer
    from ray.air.config import ScalingConfig
    from ray.tune.tuner import Tuner
    from ray import tune
    from ray.tune.tune_config import TuneConfig

    train_dataset, test_dataset = get_datasets()

    # don't set `trainer_init_config["devices"]`
    trainer = LightningTrainer(
        LitModel,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        datasets={"train": train_dataset, "test": test_dataset},
    )
    tuner = Tuner(
        trainer,
        param_space={
            "lightning_module_init_config": {
                "learning_rate": tune.loguniform(1e-4, 1e-1),
            },
            # for valid keywords to pass to `trainer_init_config`, see
            # https://pytorch-lightning.readthedocs.io/en/stable/common/trainer.html#init
            "trainer_init_config": {
                "max_epochs": tune.grid_search([4, 10, 16]),
            },
        },
        tune_config=TuneConfig(num_samples=6),
    )
    analysis = tuner.fit()
    best_loss = analysis.get_best_result(metric="train_loss_epoch", mode="min")
    print(f"Best loss result: {best_loss}")


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
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )

    args, _ = parser.parse_known_args()

    import ray

    if args.smoke_test:
        ray.init(num_cpus=6)
        tune_lightning_mnist()
    else:
        ray.init(address=args.address)
        tune_lightning_mnist(num_workers=args.num_workers, use_gpu=args.use_gpu)
