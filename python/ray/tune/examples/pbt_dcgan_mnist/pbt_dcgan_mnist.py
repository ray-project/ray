#!/usr/bin/env python

import ray
from ray import tune
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.trial import ExportFormat

import argparse
import os
from filelock import FileLock
import random
import torch
import torch.nn as nn
import torch.nn.parallel
import torch.optim as optim
import torch.utils.data
import torchvision.datasets as dset
import torchvision.transforms as transforms
import torchvision.utils as vutils
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation

from torch.autograd import Variable
from torch.nn import functional as F
from scipy.stats import entropy

# Training parameters
dataroot = ray.utils.get_user_temp_dir() + os.sep
workers = 2
batch_size = 64
image_size = 32

# Number of channels in the training images. For color images this is 3
nc = 1

# Size of z latent vector (i.e. size of generator input)
nz = 100

# Size of feature maps in generator
ngf = 32

# Size of feature maps in discriminator
ndf = 32

# Beta1 hyperparam for Adam optimizers
beta1 = 0.5

# iterations of actual training in each Trainable _train
train_iterations_per_step = 5

MODEL_PATH = os.path.expanduser("~/.ray/models/mnist_cnn.pt")


def get_data_loader():
    dataset = dset.MNIST(
        root=dataroot,
        download=True,
        transform=transforms.Compose([
            transforms.Resize(image_size),
            transforms.ToTensor(),
            transforms.Normalize((0.5, ), (0.5, )),
        ]))

    # Create the dataloader
    dataloader = torch.utils.data.DataLoader(
        dataset, batch_size=batch_size, shuffle=True, num_workers=workers)

    return dataloader


# __GANmodel_begin__
# custom weights initialization called on netG and netD
def weights_init(m):
    classname = m.__class__.__name__
    if classname.find("Conv") != -1:
        nn.init.normal_(m.weight.data, 0.0, 0.02)
    elif classname.find("BatchNorm") != -1:
        nn.init.normal_(m.weight.data, 1.0, 0.02)
        nn.init.constant_(m.bias.data, 0)


# Generator Code
class Generator(nn.Module):
    def __init__(self):
        super(Generator, self).__init__()
        self.main = nn.Sequential(
            # input is Z, going into a convolution
            nn.ConvTranspose2d(nz, ngf * 4, 4, 1, 0, bias=False),
            nn.BatchNorm2d(ngf * 4),
            nn.ReLU(True),
            nn.ConvTranspose2d(ngf * 4, ngf * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(ngf * 2),
            nn.ReLU(True),
            nn.ConvTranspose2d(ngf * 2, ngf, 4, 2, 1, bias=False),
            nn.BatchNorm2d(ngf),
            nn.ReLU(True),
            nn.ConvTranspose2d(ngf, nc, 4, 2, 1, bias=False),
            nn.Tanh())

    def forward(self, input):
        return self.main(input)


class Discriminator(nn.Module):
    def __init__(self):
        super(Discriminator, self).__init__()
        self.main = nn.Sequential(
            nn.Conv2d(nc, ndf, 4, 2, 1, bias=False),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(ndf, ndf * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(ndf * 2), nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(ndf * 2, ndf * 4, 4, 2, 1, bias=False),
            nn.BatchNorm2d(ndf * 4), nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(ndf * 4, 1, 4, 1, 0, bias=False), nn.Sigmoid())

    def forward(self, input):
        return self.main(input)


# __GANmodel_end__


# __INCEPTION_SCORE_begin__
class Net(nn.Module):
    """
    LeNet for MNist classification, used for inception_score
    """

    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
        self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
        self.conv2_drop = nn.Dropout2d()
        self.fc1 = nn.Linear(320, 50)
        self.fc2 = nn.Linear(50, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 2))
        x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
        x = x.view(-1, 320)
        x = F.relu(self.fc1(x))
        x = F.dropout(x, training=self.training)
        x = self.fc2(x)
        return F.log_softmax(x, dim=1)


def inception_score(imgs, batch_size=32, splits=1):
    N = len(imgs)
    dtype = torch.FloatTensor
    dataloader = torch.utils.data.DataLoader(imgs, batch_size=batch_size)
    cm = ray.get(mnist_model_ref)
    up = nn.Upsample(size=(28, 28), mode="bilinear").type(dtype)

    def get_pred(x):
        x = up(x)
        x = cm(x)
        return F.softmax(x).data.cpu().numpy()

    preds = np.zeros((N, 10))
    for i, batch in enumerate(dataloader, 0):
        batch = batch.type(dtype)
        batchv = Variable(batch)
        batch_size_i = batch.size()[0]
        preds[i * batch_size:i * batch_size + batch_size_i] = get_pred(batchv)

    # Now compute the mean kl-div
    split_scores = []
    for k in range(splits):
        part = preds[k * (N // splits):(k + 1) * (N // splits), :]
        py = np.mean(part, axis=0)
        scores = []
        for i in range(part.shape[0]):
            pyx = part[i, :]
            scores.append(entropy(pyx, py))
        split_scores.append(np.exp(np.mean(scores)))

    return np.mean(split_scores), np.std(split_scores)


# __INCEPTION_SCORE_end__


def train(netD, netG, optimG, optimD, criterion, dataloader, iteration,
          device):
    real_label = 1
    fake_label = 0

    for i, data in enumerate(dataloader, 0):
        if i >= train_iterations_per_step:
            break

        netD.zero_grad()
        real_cpu = data[0].to(device)
        b_size = real_cpu.size(0)
        label = torch.full((b_size, ), real_label, device=device)
        output = netD(real_cpu).view(-1)
        errD_real = criterion(output, label)
        errD_real.backward()
        D_x = output.mean().item()

        noise = torch.randn(b_size, nz, 1, 1, device=device)
        fake = netG(noise)
        label.fill_(fake_label)
        output = netD(fake.detach()).view(-1)
        errD_fake = criterion(output, label)
        errD_fake.backward()
        D_G_z1 = output.mean().item()
        errD = errD_real + errD_fake
        optimD.step()

        netG.zero_grad()
        label.fill_(real_label)
        output = netD(fake).view(-1)
        errG = criterion(output, label)
        errG.backward()
        D_G_z2 = output.mean().item()
        optimG.step()

        is_score, is_std = inception_score(fake)

        # Output training stats
        if iteration % 10 == 0:
            print("[%d/%d]\tLoss_D: %.4f\tLoss_G: %.4f\tD(x): %.4f\tD(G(z))"
                  ": %.4f / %.4f \tInception score: %.4f" %
                  (iteration, len(dataloader), errD.item(), errG.item(), D_x,
                   D_G_z1, D_G_z2, is_score))

    return errG.item(), errD.item(), is_score


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

    def step(self):
        lossG, lossD, is_score = train(
            self.netD, self.netG, self.optimizerG, self.optimizerD,
            self.criterion, self.dataloader, self._iteration, self.device)
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
        # Plot some training images
        real_batch = next(iter(dataloader))
        plt.figure(figsize=(8, 8))
        plt.axis("off")
        plt.title("Original Images")
        plt.imshow(
            np.transpose(
                vutils.make_grid(
                    real_batch[0][:64], padding=2, normalize=True).cpu(),
                (1, 2, 0)))

        plt.show()

    # load the pretrained mnist classification model for inception_score
    mnist_cnn = Net()
    mnist_cnn.load_state_dict(torch.load(MODEL_PATH))
    mnist_cnn.eval()
    mnist_model_ref = ray.put(mnist_cnn)

    # __tune_begin__
    scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        metric="is_score",
        mode="max",
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
                lambda spec: random.choice([0.0001, 0.0002, 0.0005]))
        })
    # __tune_end__

    # demo of the trained Generators
    if not args.smoke_test:
        logdirs = analysis.dataframe()["logdir"].tolist()
        img_list = []
        fixed_noise = torch.randn(64, nz, 1, 1)
        for d in logdirs:
            netG_path = os.path.join(d, "exported_models")
            loadedG = Generator()
            loadedG.load_state_dict(torch.load(netG_path)["netGmodel"])
            with torch.no_grad():
                fake = loadedG(fixed_noise).detach().cpu()
            img_list.append(vutils.make_grid(fake, padding=2, normalize=True))

        fig = plt.figure(figsize=(8, 8))
        plt.axis("off")
        ims = [[plt.imshow(np.transpose(i, (1, 2, 0)), animated=True)]
               for i in img_list]
        ani = animation.ArtistAnimation(
            fig, ims, interval=1000, repeat_delay=1000, blit=True)
        ani.save("./generated.gif", writer="imagemagick", dpi=72)
        plt.show()
