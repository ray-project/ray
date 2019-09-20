from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from contextlib import closing
import numpy as np
import socket
import time
import torch
import torch.nn as nn


def train(model, train_iterator, criterion, optimizer):
    """Runs 1 training epoch"""
    batch_time = AverageMeter()
    data_time = AverageMeter()
    losses = AverageMeter()

    timers = {k: TimerStat() for k in ["d2h", "fwd", "grad", "apply"]}

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


def validate(model, val_iterator, criterion):
    batch_time = AverageMeter()
    losses = AverageMeter()

    # switch to evaluate mode
    model.eval()

    with torch.no_grad():
        end = time.time()
        for i, (features, target) in enumerate(val_iterator):

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


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class AverageMeter(object):
    """Computes and stores the average and current value."""

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
