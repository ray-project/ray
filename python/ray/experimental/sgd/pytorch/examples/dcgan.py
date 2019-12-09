#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray import tune
from ray.tune.schedulers import PopulationBasedTraining

import argparse
import os
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
import torch.utils.data
from scipy.stats import entropy

# Set random seed for reproducibility
manualSeed = 999
# manualSeed = random.randint(1, 10000) # use if you want new results
print("Random Seed: ", manualSeed)
random.seed(manualSeed)
torch.manual_seed(manualSeed)

# Training parameters
dataroot = "/tmp/"
workers = 2
batch_size = 64
image_size = 32
device = 'cpu'

# Number of channels in the training images. For color images this is 3
num_channels = 1

# Size of z latent vector (i.e. size of generator input)
latent_vector_size = 100

# Size of feature maps in generator
features_g = 32

# Size of feature maps in discriminator
features_d = 32


def get_data_loader():
    dataset = dset.MNIST(root=dataroot, download=True,
                         transform=transforms.Compose([
                             transforms.Resize(image_size),
                             transforms.ToTensor(),
                             transforms.Normalize((0.5,), (0.5,)),
                         ]))

    # Create the dataloader
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=batch_size,
                                             shuffle=True, num_workers=workers)

    return dataloader


# __GANmodel_begin__
# custom weights initialization called on netG and netD
def weights_init(m):
    classname = m.__class__.__name__
    if classname.find('Conv') != -1:
        nn.init.normal_(m.weight.data, 0.0, 0.02)
    elif classname.find('BatchNorm') != -1:
        nn.init.normal_(m.weight.data, 1.0, 0.02)
        nn.init.constant_(m.bias.data, 0)


# Generator Code
class Generator(nn.Module):
    def __init__(self):
        super(Generator, self).__init__()
        self.main = nn.Sequential(
            # input is Z, going into a convolution
            nn.ConvTranspose2d( latent_vector_size, features_g * 4, 4, 1, 0, bias=False),
            nn.BatchNorm2d(features_g * 4),
            nn.ReLU(True),
            nn.ConvTranspose2d(features_g * 4, features_g * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features_g * 2),
            nn.ReLU(True),
            nn.ConvTranspose2d( features_g * 2, features_g, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features_g),
            nn.ReLU(True),
            nn.ConvTranspose2d(features_g, num_channels, 4, 2, 1, bias=False),
            nn.Tanh()
        )

    def forward(self, input):
        return self.main(input)


class Discriminator(nn.Module):
    def __init__(self):
        super(Discriminator, self).__init__()
        self.main = nn.Sequential(
            nn.Conv2d(num_channels, features_d, 4, 2, 1, bias=False),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(features_d, features_d * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features_d * 2),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(features_d * 2, features_d * 4, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features_d * 4),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(features_d * 4, 1, 4, 1, 0, bias=False),
            nn.Sigmoid()
        )

    def forward(self, input):
        return self.main(input)


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
    up = nn.Upsample(size=(28, 28), mode='bilinear').type(dtype)

    def get_pred(x):
        x = up(x)
        x = cm(x)
        return F.softmax(x).data.cpu().numpy()

    preds = np.zeros((N, 10))
    for i, batch in enumerate(dataloader, 0):
        batch = batch.type(dtype)
        batchv = Variable(batch)
        batch_size_i = batch.size()[0]
        preds[i*batch_size:i*batch_size + batch_size_i] = get_pred(batchv)

    # Now compute the mean kl-div
    split_scores = []
    for k in range(splits):
        part = preds[k * (N // splits): (k+1) * (N // splits), :]
        py = np.mean(part, axis=0)
        scores = []
        for i in range(part.shape[0]):
            pyx = part[i, :]
            scores.append(entropy(pyx, py))
        split_scores.append(np.exp(np.mean(scores)))

    return np.mean(split_scores), np.std(split_scores)
# __INCEPTION_SCORE_end__


def train(netD, netG, optimG, optimD, criterion, dataloader, iteration):
    real_label = 1
    fake_label = 0

    for i, data in enumerate(dataloader, 0):
        if i >= 5:
            break

        netD.zero_grad()
        real_cpu = data[0].to(device)
        b_size = real_cpu.size(0)
        label = torch.full((b_size,), real_label, device=device)
        output = netD(real_cpu).view(-1)
        errD_real = criterion(output, label)
        errD_real.backward()
        D_x = output.mean().item()

        noise = torch.randn(b_size, latent_vector_size, 1, 1, device=device)
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
            print('[%d/%d]\tLoss_D: %.4f\tLoss_G: %.4f\tD(x): %.4f\tD(G(z))'
                  ': %.4f / %.4f \tInception score: %.4f'
                  % (iteration, len(dataloader),
                     errD.item(), errG.item(), D_x, D_G_z1, D_G_z2, is_score))

    return errG.item(), errD.item(), is_score

def optimizer_creator(models, config):
    net_d, net_g = models
    discriminator_opt = optim.Adam(net_d.parameters(),
                                     lr=config.get("lr", 0.01),
                                     betas=(0.5, 0.999))
    generator_opt = optim.Adam(net_g.parameters(),
                                     lr=config.get("lr", 0.01),
                                     betas=(0.5, 0.999))
    return discriminator_opt, generator_opt


# __Trainable_begin__
class PytorchTrainable(tune.Trainable):
    def _setup(self, config):
        self.netD = Discriminator().to(device)
        self.netD.apply(weights_init)
        self.netG = Generator().to(device)
        self.netG.apply(weights_init)
        self.criterion = nn.BCELoss()
        self.dataloader = get_data_loader()

    def _train(self):
        lossG, lossD, is_score = train(self.netD, self.netG, self.optimizerG,
                                       self.optimizerD, self.criterion,
                                       self.dataloader, self._iteration)
        return {"lossg": lossG, "lossd": lossD, "is_score": is_score}
# __Trainable_end__


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    dataloader = get_data_loader()
    # Plot some training images
    real_batch = next(iter(dataloader))
    plt.figure(figsize=(8, 8))
    plt.axis("off")
    plt.title("Original Images")
    plt.imshow(np.transpose(vutils.make_grid(
        real_batch[0].to(device)[:64], padding=2, normalize=True).cpu(), (1, 2, 0)))

    if not args.smoke_test:
        plt.show()

    # load the pretrained mnist classification model for inception_score
    mnist_cnn = Net()
    mnist_cnn.load_state_dict(torch.load("./mnist_cnn.pt"))
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
        name="dcgan_mnist_pbt",
        scheduler=scheduler,
        reuse_actors=True,
        verbose=1,
        checkpoint_at_end=True,
        stop={
            "training_iteration": tune_iter,
        },
        num_samples=8,
        config={
            "netG_lr": tune.sample_from(
                lambda spec: random.choice([0.0001, 0.0002, 0.0005])
            ),
            "netD_lr": tune.sample_from(
                lambda spec: random.choice([0.0001, 0.0002, 0.0005])
            )
        })
    # __tune_end__

    # demo of the trained Generators
    if not args.smoke_test:
        logdirs = analysis.dataframe()['logdir'].tolist()
        img_list = []
        fixed_noise = torch.randn(64, latent_vector_size, 1, 1, device=device)
        for d in logdirs:
            netG_path = d + '/checkpoint_' + str(tune_iter) + '/netGmodel.pth'
            loadedG = Generator().to(device)
            loadedG.load_state_dict(torch.load(netG_path))
            with torch.no_grad():
                fake = loadedG(fixed_noise).detach().cpu()
            img_list.append(vutils.make_grid(fake, padding=2, normalize=True))

        fig = plt.figure(figsize=(8, 8))
        plt.axis("off")
        ims = [[plt.imshow(np.transpose(i, (1, 2, 0)), animated=True)]
               for i in img_list]
        ani = animation.ArtistAnimation(
            fig, ims, interval=1000, repeat_delay=1000, blit=True)
        ani.save("./generated.gif", writer='imagemagick', dpi=72)
        plt.show()
