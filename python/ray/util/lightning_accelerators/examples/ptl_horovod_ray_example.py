import os
import tempfile

import ray
from ray.tune.integration.pytorch_lightning import TuneReportCallback
from ray.util.lightning_accelerators import HorovodRayAccelerator

import pytorch_lightning as pl
from pl_bolts.datamodules.mnist_datamodule import MNISTDataModule

from ray import tune
from ray.tune.examples.mnist_ptl_mini import LightningMNISTClassifier


def train_mnist(config,
                data_dir=None,
                num_epochs=10,
                num_hosts=1,
                num_slots=4,
                use_gpu=False,
                callbacks=None):
    model = LightningMNISTClassifier(config, data_dir)
    dm = MNISTDataModule(
        data_dir=data_dir, num_workers=1, batch_size=config["batch_size"])

    callbacks = callbacks or []

    trainer = pl.Trainer(
        max_epochs=num_epochs,
        #gpus=num_slots * int(use_gpu),
        #num_nodes=num_hosts,
        #num_processes=num_slots,
        callbacks=callbacks,
        accelerator=HorovodRayAccelerator(num_hosts=num_hosts,
                                          num_slots=num_slots, use_gpu=use_gpu))
    trainer.fit(model, dm)


def tune_mnist(data_dir,
               num_samples=10,
               num_epochs=10,
               num_hosts=1,
               num_slots=4,
               use_gpu=False):
    config = {
        "layer_1": tune.choice([32, 64, 128]),
        "layer_2": tune.choice([64, 128, 256]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([32, 64, 128]),
    }

    # Add Tune callback.
    metrics = {"loss": "ptl/val_loss", "acc": "ptl/val_accuracy"}
    callbacks = [TuneReportCallback(metrics, on="validation_end")]
    trainable = tune.with_parameters(
        train_mnist,
        data_dir=data_dir,
        num_epochs=num_epochs,
        num_hosts=num_hosts,
        num_slots=num_slots,
        use_gpu=use_gpu,
        callbacks=callbacks)
    analysis = tune.run(
        trainable,
        metric="loss",
        mode="min",
        config=config,
        num_samples=num_samples,
        resources_per_trial={
            "cpu": 1,
            # Assume 1 cpu per slot.
            "extra_cpu": num_hosts * num_slots,
            # Assume 1 gpu per slot.
            "extra_gpu": num_hosts * num_slots * int(use_gpu)
        },
        name="tune_mnist")

    print("Best hyperparameters found were: ", analysis.best_config)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-hosts",
        type=int,
        help=
        "Number of machines to train on. If using Tune, then each trial will use this many machines.",
        default=1)
    parser.add_argument(
        "--num-slots",
        type=int,
        help="Number of workers to "
        "place on each "
        "machine. If using "
        "Tune, then each trial will use this many slots per machine.",
        default=1)
    parser.add_argument(
        "--use-gpu", action="store_true", help="Use GPU for "
        "training.")
    parser.add_argument(
        "--tune",
        action="store_true",
        help="Use Ray Tune "
        "for "
        "hyperparameter "
        "tuning.")
    parser.add_argument(
        "--num-samples",
        type=int,
        default=10,
        help="Number "
        "of "
        "samples to tune.")
    parser.add_argument(
        "--num-epochs",
        type=int,
        default=10,
        help="Number "
        "of "
        "epochs "
        "to train for.")
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="the address to use for Ray")
    args, _ = parser.parse_known_args()

    num_epochs = 1 if args.smoke_test else args.num_epochs
    num_hosts = 1 if args.smoke_test else args.num_hosts
    num_slots = 1 if args.smoke_test else args.num_slots
    use_gpu = False if args.smoke_test else args.use_gpu
    num_samples = 1 if args.smoke_test else args.num_samples

    if args.smoke_test:
        ray.init(num_cpus=2)
    else:
        ray.init(address=args.address)

    data_dir = os.path.join(tempfile.gettempdir(), "mnist_data_")
    # Download data
    MNISTDataModule(data_dir=data_dir).prepare_data()
    if args.tune:
        tune_mnist(data_dir, num_samples, num_epochs, num_hosts, num_slots,
                   use_gpu)
    else:
        config = {"layer_1": 32, "layer_2": 64, "lr": 1e-1, "batch_size": 32}
        train_mnist(config, data_dir, num_epochs, num_hosts, num_slots,
                    use_gpu)
