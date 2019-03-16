# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
from __future__ import print_function

import argparse
import os
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms

from ray.tune import Trainable

# Training settings
parser = argparse.ArgumentParser(description='PyTorch MNIST Example')
parser.add_argument(
    '--batch-size',
    type=int,
    default=64,
    metavar='N',
    help='input batch size for training (default: 64)')
parser.add_argument(
    '--test-batch-size',
    type=int,
    default=1000,
    metavar='N',
    help='input batch size for testing (default: 1000)')
parser.add_argument(
    '--epochs',
    type=int,
    default=1,
    metavar='N',
    help='number of epochs to train (default: 1)')
parser.add_argument(
    '--lr',
    type=float,
    default=0.01,
    metavar='LR',
    help='learning rate (default: 0.01)')
parser.add_argument(
    '--momentum',
    type=float,
    default=0.5,
    metavar='M',
    help='SGD momentum (default: 0.5)')
parser.add_argument(
    '--no-cuda',
    action='store_true',
    default=False,
    help='disables CUDA training')
parser.add_argument(
    '--seed',
    type=int,
    default=1,
    metavar='S',
    help='random seed (default: 1)')
parser.add_argument(
    '--smoke-test', action="store_true", help="Finish quickly for testing")


class Net(nn.Module):
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


class TrainMNIST(Trainable):
    def _setup(self, config):
        args = config.pop("args")
        vars(args).update(config)
        args.cuda = not args.no_cuda and torch.cuda.is_available()

        torch.manual_seed(args.seed)
        if args.cuda:
            torch.cuda.manual_seed(args.seed)

        kwargs = {'num_workers': 1, 'pin_memory': True} if args.cuda else {}
        self.train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                '~/data',
                train=True,
                download=False,
                transform=transforms.Compose([
                    transforms.ToTensor(),
                    transforms.Normalize((0.1307, ), (0.3081, ))
                ])),
            batch_size=args.batch_size,
            shuffle=True,
            **kwargs)
        self.test_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                '~/data',
                train=False,
                transform=transforms.Compose([
                    transforms.ToTensor(),
                    transforms.Normalize((0.1307, ), (0.3081, ))
                ])),
            batch_size=args.test_batch_size,
            shuffle=True,
            **kwargs)

        self.model = Net()
        if args.cuda:
            self.model.cuda()

        self.optimizer = optim.SGD(
            self.model.parameters(), lr=args.lr, momentum=args.momentum)
        self.args = args

    def _train_iteration(self):
        self.model.train()
        for batch_idx, (data, target) in enumerate(self.train_loader):
            if self.args.cuda:
                data, target = data.cuda(), target.cuda()
            self.optimizer.zero_grad()
            output = self.model(data)
            loss = F.nll_loss(output, target)
            loss.backward()
            self.optimizer.step()

    def _test(self):
        self.model.eval()
        test_loss = 0
        correct = 0
        with torch.no_grad():
            for data, target in self.test_loader:
                if self.args.cuda:
                    data, target = data.cuda(), target.cuda()
                output = self.model(data)
                # sum up batch loss
                test_loss += F.nll_loss(output, target, reduction='sum').item()
                # get the index of the max log-probability
                pred = output.argmax(dim=1, keepdim=True)
                correct += pred.eq(
                    target.data.view_as(pred)).long().cpu().sum()

        test_loss = test_loss / len(self.test_loader.dataset)
        accuracy = correct.item() / len(self.test_loader.dataset)
        return {"mean_loss": test_loss, "mean_accuracy": accuracy}

    def _train(self):
        self._train_iteration()
        return self._test()

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir, "model.pth")
        torch.save(self.model.state_dict(), checkpoint_path)
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.model.load_state_dict(checkpoint_path)


if __name__ == "__main__":
    datasets.MNIST('~/data', train=True, download=True)
    args = parser.parse_args()

    import numpy as np
    import ray
    from ray import tune
    from ray.tune.schedulers import HyperBandScheduler

    ray.init()
    sched = HyperBandScheduler(
        time_attr="training_iteration", reward_attr="neg_mean_loss")
    tune.run(
        TrainMNIST,
        verbose=0,
        scheduler=sched,
        **{
            "stop": {
                "mean_accuracy": 0.95,
                "training_iteration": 1 if args.smoke_test else 20,
            },
            "resources_per_trial": {
                "cpu": 3,
                "gpu": int(not args.no_cuda)
            },
            "num_samples": 1 if args.smoke_test else 20,
            "checkpoint_at_end": True,
            "config": {
                "args": args,
                "lr": tune.sample_from(
                    lambda spec: np.random.uniform(0.001, 0.1)),
                "momentum": tune.sample_from(
                    lambda spec: np.random.uniform(0.1, 0.9)),
            }
        })
