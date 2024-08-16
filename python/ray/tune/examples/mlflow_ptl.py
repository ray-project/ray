"""An example showing how to use Pytorch Lightning training, Ray Tune
HPO, and MLflow autologging all together."""

import os
import tempfile

import mlflow
import pytorch_lightning as pl

from ray import train, tune
from ray.air.integrations.mlflow import setup_mlflow
from ray.tune.examples.mnist_ptl_mini import LightningMNISTClassifier, MNISTDataModule
from ray.tune.integration.pytorch_lightning import TuneReportCallback


def train_mnist_tune(config, data_dir=None, num_epochs=10, num_gpus=0):
    setup_mlflow(
        config,
        experiment_name=config.get("experiment_name", None),
        tracking_uri=config.get("tracking_uri", None),
    )

    model = LightningMNISTClassifier(config, data_dir)
    dm = MNISTDataModule(
        data_dir=data_dir, num_workers=1, batch_size=config["batch_size"]
    )
    metrics = {"loss": "ptl/val_loss", "acc": "ptl/val_accuracy"}
    mlflow.pytorch.autolog()
    trainer = pl.Trainer(
        max_epochs=num_epochs,
        gpus=num_gpus,
        progress_bar_refresh_rate=0,
        callbacks=[TuneReportCallback(metrics, on="validation_end")],
    )
    trainer.fit(model, dm)


def tune_mnist(
    num_samples=10,
    num_epochs=10,
    gpus_per_trial=0,
    tracking_uri=None,
    experiment_name="ptl_autologging_example",
):
    data_dir = os.path.join(tempfile.gettempdir(), "mnist_data_")
    # Download data
    MNISTDataModule(data_dir=data_dir, batch_size=32).prepare_data()

    # Set the MLflow experiment, or create it if it does not exist.
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    config = {
        "layer_1": tune.choice([32, 64, 128]),
        "layer_2": tune.choice([64, 128, 256]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([32, 64, 128]),
        "experiment_name": experiment_name,
        "tracking_uri": mlflow.get_tracking_uri(),
        "data_dir": os.path.join(tempfile.gettempdir(), "mnist_data_"),
        "num_epochs": num_epochs,
    }

    trainable = tune.with_parameters(
        train_mnist_tune,
        data_dir=data_dir,
        num_epochs=num_epochs,
        num_gpus=gpus_per_trial,
    )

    tuner = tune.Tuner(
        tune.with_resources(trainable, resources={"cpu": 1, "gpu": gpus_per_trial}),
        tune_config=tune.TuneConfig(
            metric="loss",
            mode="min",
            num_samples=num_samples,
        ),
        run_config=train.RunConfig(
            name="tune_mnist",
        ),
        param_space=config,
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        tune_mnist(
            num_samples=1,
            num_epochs=1,
            gpus_per_trial=0,
            tracking_uri=os.path.join(tempfile.gettempdir(), "mlruns"),
        )
    else:
        tune_mnist(num_samples=10, num_epochs=10, gpus_per_trial=0)
