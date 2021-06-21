#!/usr/bin/env python
"""
Example of training DCGAN on MNIST using PBT with Tune's Trainable Class
API.
"""
import ray
from ray import tune
from ray.tune.trial import ExportFormat
from ray.tune.schedulers import PopulationBasedTraining

import argparse
import os
from filelock import FileLock
import random
import torch
import torch.nn as nn
import torch.nn.parallel
import torch.optim as optim
import torch.utils.data
import numpy as np

from common import beta1, MODEL_PATH
from common import demo_gan, get_data_loader, plot_images, train, weights_init
from common import Discriminator, Generator, Net


# __Trainable_begin__
class PytorchTrainable(tune.Trainable):
    def setup(self, config):
        use_cuda = config.get("use_gpu") and torch.cuda.is_available()
        self.device = torch.device("cuda" if use_cuda else "cpu")
        self.netD = Discriminator().to(self.device)
        self.netD.apply(weights_init)
        self.netG = Generator().to(self.device)
        self.netG.apply(weights_init)
        self.criterion = nn.BCELoss()
        self.optimizerD = optim.Adam(
            self.netD.parameters(),
            lr=config.get("lr", 0.01),
            betas=(beta1, 0.999))
        self.optimizerG = optim.Adam(
            self.netG.parameters(),
            lr=config.get("lr", 0.01),
            betas=(beta1, 0.999))
        with FileLock(os.path.expanduser("~/.data.lock")):
            self.dataloader = get_data_loader()
        self.mnist_model_ref = config["mnist_model_ref"]

    def step(self):
        lossG, lossD, is_score = train(self.netD, self.netG, self.optimizerG,
                                       self.optimizerD, self.criterion,
                                       self.dataloader, self._iteration,
                                       self.device, self.mnist_model_ref)
        return {"lossg": lossG, "lossd": lossD, "is_score": is_score}

    def save_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        torch.save({
            "netDmodel": self.netD.state_dict(),
            "netGmodel": self.netG.state_dict(),
            "optimD": self.optimizerD.state_dict(),
            "optimG": self.optimizerG.state_dict(),
        }, path)

        return checkpoint_dir

    def load_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        checkpoint = torch.load(path)
        self.netD.load_state_dict(checkpoint["netDmodel"])
        self.netG.load_state_dict(checkpoint["netGmodel"])
        self.optimizerD.load_state_dict(checkpoint["optimD"])
        self.optimizerG.load_state_dict(checkpoint["optimG"])

    def reset_config(self, new_config):
        if "netD_lr" in new_config:
            for param_group in self.optimizerD.param_groups:
                param_group["lr"] = new_config["netD_lr"]
        if "netG_lr" in new_config:
            for param_group in self.optimizerG.param_groups:
                param_group["lr"] = new_config["netG_lr"]

        self.config = new_config
        return True

    def _export_model(self, export_formats, export_dir):
        if export_formats == [ExportFormat.MODEL]:
            path = os.path.join(export_dir, "exported_models")
            torch.save({
                "netDmodel": self.netD.state_dict(),
                "netGmodel": self.netG.state_dict()
            }, path)
            return {ExportFormat.MODEL: path}
        else:
            raise ValueError("unexpected formats: " + str(export_formats))


# __Trainable_end__

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

    # load the pretrained mnist classification model for inception_score
    mnist_cnn = Net()
    mnist_cnn.load_state_dict(torch.load(MODEL_PATH))
    mnist_cnn.eval()
    mnist_model_ref = ray.put(mnist_cnn)

    # __tune_begin__
    scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        perturbation_interval=5,
        hyperparam_mutations={
            # distribution for resampling
            "netG_lr": lambda: np.random.uniform(1e-2, 1e-5),
            "netD_lr": lambda: np.random.uniform(1e-2, 1e-5),
        })

    tune_iter = 5 if args.smoke_test else 300
    analysis = tune.run(
        PytorchTrainable,
        name="pbt_dcgan_mnist",
        scheduler=scheduler,
        reuse_actors=True,
        verbose=1,
        metric="is_score",
        mode="max",
        checkpoint_at_end=True,
        stop={
            "training_iteration": tune_iter,
        },
        num_samples=8,
        export_formats=[ExportFormat.MODEL],
        config={
            "netG_lr": tune.sample_from(
                lambda spec: random.choice([0.0001, 0.0002, 0.0005])),
            "netD_lr": tune.sample_from(
                lambda spec: random.choice([0.0001, 0.0002, 0.0005])),
            "mnist_model_ref": mnist_model_ref
        })
    # __tune_end__

    # demo of the trained Generators
    if not args.smoke_test:
        logdirs = analysis.results_df["logdir"].tolist()
        model_paths = [os.path.join(d, "exported_models") for d in logdirs]
        demo_gan(analysis, model_paths)
