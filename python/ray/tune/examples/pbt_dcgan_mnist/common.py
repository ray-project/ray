import os

import matplotlib.animation as animation
import matplotlib.pyplot as plt
import numpy as np
import torch
import torch.nn as nn
import torch.nn.parallel
import torch.utils.data
import torchvision.datasets as dset
import torchvision.transforms as transforms
import torchvision.utils as vutils
from scipy.stats import entropy
from torch.autograd import Variable
from torch.nn import functional as F

import ray

# Training parameters
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


def get_data_loader(data_dir="~/data"):
    dataset = dset.MNIST(
        root=data_dir,
        download=True,
        transform=transforms.Compose(
            [
                transforms.Resize(image_size),
                transforms.ToTensor(),
                transforms.Normalize((0.5,), (0.5,)),
            ]
        ),
    )

    # Create the dataloader
    dataloader = torch.utils.data.DataLoader(
        dataset, batch_size=batch_size, shuffle=True, num_workers=workers
    )

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
            nn.Tanh(),
        )

    def forward(self, input):
        return self.main(input)


class Discriminator(nn.Module):
    def __init__(self):
        super(Discriminator, self).__init__()
        self.main = nn.Sequential(
            nn.Conv2d(nc, ndf, 4, 2, 1, bias=False),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(ndf, ndf * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(ndf * 2),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(ndf * 2, ndf * 4, 4, 2, 1, bias=False),
            nn.BatchNorm2d(ndf * 4),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(ndf * 4, 1, 4, 1, 0, bias=False),
            nn.Sigmoid(),
        )

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


def inception_score(imgs, mnist_model_ref, batch_size=32, splits=1):
    N = len(imgs)
    dtype = torch.FloatTensor
    dataloader = torch.utils.data.DataLoader(imgs, batch_size=batch_size)
    cm = ray.get(mnist_model_ref)  # Get the mnist model from Ray object store.
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
        preds[i * batch_size : i * batch_size + batch_size_i] = get_pred(batchv)

    # Now compute the mean kl-div
    split_scores = []
    for k in range(splits):
        part = preds[k * (N // splits) : (k + 1) * (N // splits), :]
        py = np.mean(part, axis=0)
        scores = []
        for i in range(part.shape[0]):
            pyx = part[i, :]
            scores.append(entropy(pyx, py))
        split_scores.append(np.exp(np.mean(scores)))

    return np.mean(split_scores), np.std(split_scores)


# __INCEPTION_SCORE_end__


def train_func(
    netD,
    netG,
    optimG,
    optimD,
    criterion,
    dataloader,
    iteration,
    device,
    mnist_model_ref,
):
    real_label = 1
    fake_label = 0

    for i, data in enumerate(dataloader, 0):
        if i >= train_iterations_per_step:
            break

        netD.zero_grad()
        real_cpu = data[0].to(device)
        b_size = real_cpu.size(0)
        label = torch.full((b_size,), real_label, dtype=torch.float, device=device)
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

        is_score, is_std = inception_score(fake, mnist_model_ref)

        # Output training stats
        if iteration % 10 == 0:
            print(
                "[%d/%d]\tLoss_D: %.4f\tLoss_G: %.4f\tD(x): %.4f\tD(G(z))"
                ": %.4f / %.4f \tInception score: %.4f"
                % (
                    iteration,
                    len(dataloader),
                    errD.item(),
                    errG.item(),
                    D_x,
                    D_G_z1,
                    D_G_z2,
                    is_score,
                )
            )

    return errG.item(), errD.item(), is_score


def plot_images(dataloader):
    # Plot some training images
    real_batch = next(iter(dataloader))
    plt.figure(figsize=(8, 8))
    plt.axis("off")
    plt.title("Original Images")
    plt.imshow(
        np.transpose(
            vutils.make_grid(real_batch[0][:64], padding=2, normalize=True).cpu(),
            (1, 2, 0),
        )
    )

    plt.show()


def demo_gan(checkpoint_paths):
    img_list = []
    fixed_noise = torch.randn(64, nz, 1, 1)
    for path in checkpoint_paths:
        checkpoint_dict = torch.load(os.path.join(path, "checkpoint.pt"))

        loadedG = Generator()
        loadedG.load_state_dict(checkpoint_dict["netGmodel"])
        with torch.no_grad():
            fake = loadedG(fixed_noise).detach().cpu()
        img_list.append(vutils.make_grid(fake, padding=2, normalize=True))

    fig = plt.figure(figsize=(8, 8))
    plt.axis("off")
    ims = [[plt.imshow(np.transpose(i, (1, 2, 0)), animated=True)] for i in img_list]
    ani = animation.ArtistAnimation(
        fig, ims, interval=1000, repeat_delay=1000, blit=True
    )
    ani.save("./generated.gif", writer="imagemagick", dpi=72)
    plt.show()
