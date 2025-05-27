#!/usr/bin/env python
"""
Example of training DCGAN on MNIST using PBT with Tune's function API.
"""
import argparse
import os
import tempfile

import numpy as np
import torch
import torch.nn as nn
import torch.nn.parallel
import torch.optim as optim
import torch.utils.data
from filelock import FileLock

import ray
from ray import tune
from ray.tune import Checkpoint
from ray.tune.examples.pbt_dcgan_mnist.common import (
    MODEL_PATH,
    Discriminator,
    Generator,
    Net,
    beta1,
    demo_gan,
    get_data_loader,
    plot_images,
    train_func,
    weights_init,
)
from ray.tune.schedulers import PopulationBasedTraining


# __Train_begin__
def dcgan_train(config):
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    netD = Discriminator().to(device)
    netD.apply(weights_init)
    netG = Generator().to(device)
    netG.apply(weights_init)
    criterion = nn.BCELoss()
    optimizerD = optim.Adam(
        netD.parameters(), lr=config.get("lr", 0.01), betas=(beta1, 0.999)
    )
    optimizerG = optim.Adam(
        netG.parameters(), lr=config.get("lr", 0.01), betas=(beta1, 0.999)
    )
    with FileLock(os.path.expanduser("~/ray_results/.data.lock")):
        dataloader = get_data_loader()

    step = 1
    checkpoint = tune.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            checkpoint_dict = torch.load(os.path.join(checkpoint_dir, "checkpoint.pt"))
        netD.load_state_dict(checkpoint_dict["netDmodel"])
        netG.load_state_dict(checkpoint_dict["netGmodel"])
        optimizerD.load_state_dict(checkpoint_dict["optimD"])
        optimizerG.load_state_dict(checkpoint_dict["optimG"])
        # Note: Make sure to increment the loaded step by 1 to get the
        # current step.
        last_step = checkpoint_dict["step"]
        step = last_step + 1

        # NOTE: It's important to set the optimizer learning rates
        # again, since we want to explore the parameters passed in by PBT.
        # Without this, we would continue using the exact same
        # configuration as the trial whose checkpoint we are exploiting.
        if "netD_lr" in config:
            for param_group in optimizerD.param_groups:
                param_group["lr"] = config["netD_lr"]
        if "netG_lr" in config:
            for param_group in optimizerG.param_groups:
                param_group["lr"] = config["netG_lr"]

    while True:
        lossG, lossD, is_score = train_func(
            netD,
            netG,
            optimizerG,
            optimizerD,
            criterion,
            dataloader,
            step,
            device,
            config["mnist_model_ref"],
        )
        metrics = {"lossg": lossG, "lossd": lossD, "is_score": is_score}

        if step % config["checkpoint_interval"] == 0:
            with tempfile.TemporaryDirectory() as tmpdir:
                torch.save(
                    {
                        "netDmodel": netD.state_dict(),
                        "netGmodel": netG.state_dict(),
                        "optimD": optimizerD.state_dict(),
                        "optimG": optimizerG.state_dict(),
                        "step": step,
                    },
                    os.path.join(tmpdir, "checkpoint.pt"),
                )
                tune.report(metrics, checkpoint=Checkpoint.from_directory(tmpdir))
        else:
            tune.report(metrics)

        step += 1


# __Train_end__


def download_mnist_cnn():
    import urllib.request

    # Download a pre-trained MNIST model for inception score calculation.
    # This is a tiny model (<100kb).
    if not os.path.exists(MODEL_PATH):
        print("downloading model")
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        urllib.request.urlretrieve(
            "https://github.com/ray-project/ray/raw/master/python/ray/tune/"
            "examples/pbt_dcgan_mnist/mnist_cnn.pt",
            MODEL_PATH,
        )
    return MODEL_PATH


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--data-dir", type=str, default="~/data/", help="Set the path of the dataset."
    )
    args, _ = parser.parse_known_args()
    ray.init()

    download_mnist_cnn()

    dataloader = get_data_loader(args.data_dir)
    if not args.smoke_test:
        plot_images(dataloader)

    # __tune_begin__

    # load the pretrained mnist classification model for inception_score
    mnist_cnn = Net()
    mnist_cnn.load_state_dict(torch.load(MODEL_PATH))
    mnist_cnn.eval()
    # Put the model in Ray object store.
    mnist_model_ref = ray.put(mnist_cnn)

    scheduler = PopulationBasedTraining(
        perturbation_interval=5,
        hyperparam_mutations={
            # distribution for resampling
            "netG_lr": lambda: np.random.uniform(1e-2, 1e-5),
            "netD_lr": lambda: np.random.uniform(1e-2, 1e-5),
        },
    )

    tune_iter = 5 if args.smoke_test else 300
    tuner = tune.Tuner(
        dcgan_train,
        run_config=tune.RunConfig(
            name="pbt_dcgan_mnist",
            stop={"training_iteration": tune_iter},
            verbose=1,
        ),
        tune_config=tune.TuneConfig(
            metric="is_score",
            mode="max",
            num_samples=8,
            scheduler=scheduler,
        ),
        param_space={
            "netG_lr": tune.choice([0.0001, 0.0002, 0.0005]),
            "netD_lr": tune.choice([0.0001, 0.0002, 0.0005]),
            "mnist_model_ref": mnist_model_ref,
        },
    )
    results = tuner.fit()
    # __tune_end__

    # demo of the trained Generators
    if not args.smoke_test:
        checkpoint_paths = [result.checkpoint.to_directory() for result in results]
        demo_gan(checkpoint_paths)
