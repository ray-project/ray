#!/usr/bin/env python
"""
Example of training DCGAN on MNIST using PBT with Tune's function API.
"""
import ray
from ray import tune
from ray.tune.schedulers import PopulationBasedTraining

import argparse
import os
from filelock import FileLock
import torch
import torch.nn as nn
import torch.nn.parallel
import torch.optim as optim
import torch.utils.data
import numpy as np

from common import beta1, MODEL_PATH
from common import demo_gan, get_data_loader, plot_images, train, weights_init
from common import Discriminator, Generator, Net


# __Train_begin__
def dcgan_train(config, checkpoint_dir=None):
    step = 0
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    netD = Discriminator().to(device)
    netD.apply(weights_init)
    netG = Generator().to(device)
    netG.apply(weights_init)
    criterion = nn.BCELoss()
    optimizerD = optim.Adam(
        netD.parameters(), lr=config.get("lr", 0.01), betas=(beta1, 0.999))
    optimizerG = optim.Adam(
        netG.parameters(), lr=config.get("lr", 0.01), betas=(beta1, 0.999))
    with FileLock(os.path.expanduser("~/.data.lock")):
        dataloader = get_data_loader()

    if checkpoint_dir is not None:
        path = os.path.join(checkpoint_dir, "checkpoint")
        checkpoint = torch.load(path)
        netD.load_state_dict(checkpoint["netDmodel"])
        netG.load_state_dict(checkpoint["netGmodel"])
        optimizerD.load_state_dict(checkpoint["optimD"])
        optimizerG.load_state_dict(checkpoint["optimG"])
        step = checkpoint["step"]

        if "netD_lr" in config:
            for param_group in optimizerD.param_groups:
                param_group["lr"] = config["netD_lr"]
        if "netG_lr" in config:
            for param_group in optimizerG.param_groups:
                param_group["lr"] = config["netG_lr"]

    while True:
        lossG, lossD, is_score = train(netD, netG, optimizerG, optimizerD,
                                       criterion, dataloader, step, device,
                                       config["mnist_model_ref"])
        step += 1
        with tune.checkpoint_dir(step=step) as checkpoint_dir:
            path = os.path.join(checkpoint_dir, "checkpoint")
            torch.save({
                "netDmodel": netD.state_dict(),
                "netGmodel": netG.state_dict(),
                "optimD": optimizerD.state_dict(),
                "optimG": optimizerG.state_dict(),
                "step": step,
            }, path)
        tune.report(lossg=lossG, lossd=lossD, is_score=is_score)


# __Train_end__

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    import urllib.request
    # Download a pre-trained MNIST model for inception score calculation.
    # This is a tiny model (<100kb).
    if not os.path.exists(MODEL_PATH):
        print("downloading model")
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        urllib.request.urlretrieve(
            "https://github.com/ray-project/ray/raw/master/python/ray/tune/"
            "examples/pbt_dcgan_mnist/mnist_cnn.pt", MODEL_PATH)

    dataloader = get_data_loader()
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
        })

    tune_iter = 5 if args.smoke_test else 300
    analysis = tune.run(
        dcgan_train,
        name="pbt_dcgan_mnist",
        scheduler=scheduler,
        verbose=1,
        stop={
            "training_iteration": tune_iter,
        },
        metric="is_score",
        mode="max",
        num_samples=8,
        config={
            "netG_lr": tune.choice([0.0001, 0.0002, 0.0005]),
            "netD_lr": tune.choice([0.0001, 0.0002, 0.0005]),
            "mnist_model_ref": mnist_model_ref
        })
    # __tune_end__

    # demo of the trained Generators
    if not args.smoke_test:
        all_trials = analysis.trials
        checkpoint_paths = [
            os.path.join(analysis.get_best_checkpoint(t), "checkpoint")
            for t in all_trials
        ]
        demo_gan(analysis, checkpoint_paths)
