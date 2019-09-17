from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import torch
import torch.nn as nn

from ray.experimental.sgd import utils


def train(train_iterator, model, criterion, optimizer):
    """Runs 1 training epoch"""
    batch_time = utils.AverageMeter()
    data_time = utils.AverageMeter()
    losses = utils.AverageMeter()

    timers = {k: utils.TimerStat() for k in ["d2h", "fwd", "grad", "apply"]}

    # switch to train mode
    model.train()

    end = time.time()

    for i, (features, target) in enumerate(train_iterator):
        # measure data loading time
        data_time.update(time.time() - end)

        # Create non_blocking tensors for distributed training
        with timers["d2h"]:
            if torch.cuda.is_available():
                features = features.cuda(non_blocking=True)
                target = target.cuda(non_blocking=True)

        # compute output
        with timers["fwd"]:
            output = model(features)
            loss = criterion(output, target)

            # measure accuracy and record loss
            losses.update(loss.item(), features.size(0))

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
        "batch_time": batch_time.avg,
        "batch_processed": losses.count,
        "train_loss": losses.avg,
        "data_time": data_time.avg,
    }
    stats.update({k: t.mean for k, t in timers.items()})
    return stats


def validate(val_loader, model, criterion):
    batch_time = utils.AverageMeter()
    losses = utils.AverageMeter()

    # switch to evaluate mode
    model.eval()

    with torch.no_grad():
        end = time.time()
        for i, (features, target) in enumerate(val_loader):

            if torch.cuda.is_available():
                features = features.cuda(non_blocking=True)
                target = target.cuda(non_blocking=True)

            # compute output
            output = model(features)
            loss = criterion(output, target)

            # measure accuracy and record loss
            losses.update(loss.item(), features.size(0))

            # measure elapsed time
            batch_time.update(time.time() - end)
            end = time.time()

    stats = {"batch_time": batch_time.avg, "validation_loss": losses.avg}
    return stats


def sgd_mse_optimizer(model, config):
    """Returns the mean squared error criterion and SGD optimizer.

    Args:
        model (torch.nn.Module): the model to optimize.
        config (dict): configuration for the optimizer.
            lr (float): the learning rate. defaults to 0.01.
    """
    learning_rate = config.get("lr", 0.01)
    criterion = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learning_rate)
    return criterion, optimizer
