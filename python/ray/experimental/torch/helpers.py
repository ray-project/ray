import math
import time
import sys
import torch
import torchvision

# if __name__ == '__main__':
#     torch.multiprocessing.set_start_method('spawn')

import torch.nn as nn
import torch.nn.parallel
import torch.distributed as dist
import torch.optim
import torch.utils.data
import torch.utils.data.distributed
import torchvision.transforms as transforms
import torchvision.datasets as datasets
import torchvision.models as models

from torch.multiprocessing import Pool, Process

import torch
import torch.nn as nn
import torch.nn.init as init
import torch.nn.functional as F
from torch.autograd import Variable

import ray
import sys
import numpy as np

from ray.rllib.utils import TimerStat


def mass_download():
    transform_train = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465),
                             (0.2023, 0.1994, 0.2010)),
    ])  # meanstd transformation

    transform_test = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465),
                             (0.2023, 0.1994, 0.2010)),
    ])
    from filelock import FileLock
    with FileLock("./data.lock"):
        trainset = torchvision.datasets.CIFAR10(
            root='./data',
            train=True,
            download=True,
            transform=transform_train)


def prefetch(resource="GPU"):
    devices_in_cluster = int(
        ray.global_state.cluster_resources().get(resource))
    remote_download = ray.remote(num_gpus=1)(mass_download)
    ray.get([remote_download.remote() for i in range(devices_in_cluster)])


class AverageMeter(object):
    """Computes and stores the average and current value"""

    def __init__(self):
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count


def accuracy(output, target, topk=(1, )):
    """Computes the precision@k for the specified values of k"""
    with torch.no_grad():
        maxk = max(topk)
        batch_size = target.size(0)

        _, pred = output.topk(maxk, 1, True, True)
        pred = pred.t()
        correct = pred.eq(target.view(1, -1).expand_as(pred))

        res = []
        for k in topk:
            correct_k = correct[:k].view(-1).float().sum(0, keepdim=True)
            res.append(correct_k.mul_(100.0 / batch_size))
        return res


def train(train_loader, model, criterion, optimizer, epoch):

    batch_time = AverageMeter()
    data_time = AverageMeter()
    losses = AverageMeter()
    top1 = AverageMeter()

    timers = {k: TimerStat() for k in ["d2h", "fwd", "grad", "apply"]}

    # switch to train mode
    model.train()

    end = time.time()
    for i, (features, target) in enumerate(train_loader):

        # measure data loading time
        data_time.update(time.time() - end)

        # Create non_blocking tensors for distributed training
        with timers["d2h"]:
            features = features.cuda(non_blocking=True)
            target = target.cuda(non_blocking=True)

        # compute output
        with timers["fwd"]:
            output = model(features)
            loss = criterion(output, target)

            # measure accuracy and record loss
            prec1, prec5 = accuracy(output, target, topk=(1, 5))
            losses.update(loss.item(), features.size(0))
            top1.update(prec1[0], features.size(0))

        with timers["grad"]:
            # compute gradients in a backward pass
            optimizer.zero_grad()
            loss.backward()

        with timers["apply"]:
            # Call step of optimizer to update model params
            optimizer.step()

        # measure elapsed time
        batch_time.update(time.time() - end)
        end = time.time()

    stats = {
        "train_accuracy": top1.avg.cpu(),
        "batch_time": batch_time.avg,
        "train_loss": losses.avg,
        "data_time": data_time.avg
    }
    stats.update({k: t.mean for k, t in timers.items()})
    return stats


def adjust_learning_rate(initial_lr, optimizer, epoch):
    """Sets the learning rate to the initial LR decayed by 10 every 30 epochs"""

    optim_factor = 0
    if (epoch > 160):
        optim_factor = 3
    elif (epoch > 120):
        optim_factor = 2
    elif (epoch > 60):
        optim_factor = 1

    lr = initial_lr * math.pow(0.2, optim_factor)

    for param_group in optimizer.param_groups:
        param_group['lr'] = lr


######################################################################
# Validation Function
# ~~~~~~~~~~~~~~~~~~~
#
# To track generalization performance and simplify the main loop further
# we can also extract the validation step into a function called
# ``validate``. This function runs a full validation step of the features
# model on the input validation dataloader and returns the top-1 accuracy
# of the model on the validation set. Again, you will notice the only
# distributed training feature here is setting ``non_blocking=True`` for
# the training data and labels before they are passed to the model.
#


def validate(val_loader, model, criterion):

    batch_time = AverageMeter()
    losses = AverageMeter()
    top1 = AverageMeter()
    # top5 = AverageMeter()

    # switch to evaluate mode
    model.eval()

    with torch.no_grad():
        end = time.time()
        for i, (features, target) in enumerate(val_loader):

            features = features.cuda(non_blocking=True)
            target = target.cuda(non_blocking=True)

            # compute output
            output = model(features)
            loss = criterion(output, target)

            # measure accuracy and record loss
            prec1, prec5 = accuracy(output, target, topk=(1, 5))
            losses.update(loss.item(), features.size(0))
            top1.update(prec1[0], features.size(0))
            # top5.update(prec5[0], features.size(0))

            # measure elapsed time
            batch_time.update(time.time() - end)
            end = time.time()

            # if i % 100 == 0:
            #     print('Test: [{0}/{1}]\t'
            #           'Time {batch_time.val:.3f} ({batch_time.avg:.3f})\t'
            #           'Loss {loss.val:.4f} ({loss.avg:.4f})\t'
            #           'Prec@1 {top1.val:.3f} ({top1.avg:.3f})\t'.format(
            #            i, len(val_loader), batch_time=batch_time, loss=losses,
            #            top1=top1))

    stats = {
        "batch_time": batch_time.avg,
        "mean_accuracy": top1.avg.cpu(),
        "mean_loss": losses.avg
    }
    return stats
