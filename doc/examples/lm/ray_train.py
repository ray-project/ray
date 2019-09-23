#!/usr/bin/env python3 -u
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
"""
Train a new model on one or across multiple GPUs.
"""

import collections
import math
import random
import copy
import socket
import time

import ray
import torch

from fairseq import checkpoint_utils, distributed_utils, options, progress_bar, tasks, utils
from fairseq.data import iterators
from fairseq.trainer import Trainer
from fairseq.meters import AverageMeter, StopwatchMeter
from fairseq_cli.train import validate, get_training_stats
from contextlib import closing


def check_new_resources_availability(args):
    if args.cpu:
        n_cpus = int(ray.cluster_resources()["CPU"])
        if n_cpus > args.distributed_world_size:
            raise Exception("New CPUs find (original %d CPUs, now %d CPUs)" % (args.distributed_world_size, n_cpus))
    else:
        n_gpus = int(ray.cluster_resources().get("GPU", 0))
        if n_gpus > args.distributed_world_size:
            raise Exception("New GPUs find (original %d GPUs, now %d GPUs)" % (args.distributed_world_size, n_gpus))


def main(args, init_distributed=False):
    utils.import_user_module(args)

    assert args.max_tokens is not None or args.max_sentences is not None, \
        'Must specify batch size either with --max-tokens or --max-sentences'

    torch.manual_seed(args.seed)
    if init_distributed:
        args.distributed_rank = distributed_utils.distributed_init(args)

    if distributed_utils.is_master(args):
        checkpoint_utils.verify_checkpoint_directory(args.save_dir)

    # Print args
    print(args)

    # Setup task, e.g., translation, language modeling, etc.
    task = tasks.setup_task(args)

    # Load valid dataset (we load training data below, based on the latest checkpoint)
    for valid_sub_split in args.valid_subset.split(','):
        task.load_dataset(valid_sub_split, combine=False, epoch=0)

    # Build model and criterion
    model = task.build_model(args)
    criterion = task.build_criterion(args)
    print(model)
    print('| model {}, criterion {}'.format(args.arch, criterion.__class__.__name__))
    print('| num. model params: {} (num. trained: {})'.format(
        sum(p.numel() for p in model.parameters()),
        sum(p.numel() for p in model.parameters() if p.requires_grad),
    ))

    # Build trainer
    trainer = Trainer(args, task, model, criterion)
    print('| training on {} GPUs'.format(args.distributed_world_size))
    print('| max tokens per GPU = {} and max sentences per GPU = {}'.format(
        args.max_tokens,
        args.max_sentences,
    ))

    # Load the latest checkpoint if one is available and restore the
    # corresponding train iterator
    extra_state, epoch_itr = checkpoint_utils.load_checkpoint(args, trainer)

    # Train until the learning rate gets too small
    max_epoch = args.max_epoch or math.inf
    max_update = args.max_update or math.inf
    lr = trainer.get_lr()
    train_meter = StopwatchMeter()
    train_meter.start()
    valid_losses = [None]
    valid_subsets = args.valid_subset.split(',')
    while lr > args.min_lr and epoch_itr.epoch < max_epoch and trainer.get_num_updates() < max_update:
        # train for one epoch
        train(args, trainer, task, epoch_itr)

        if not args.disable_validation and epoch_itr.epoch % args.validate_interval == 0:
            valid_losses = validate(args, trainer, task, epoch_itr, valid_subsets)
        else:
            valid_losses = [None]

        # only use first validation loss to update the learning rate
        lr = trainer.lr_step(epoch_itr.epoch, valid_losses[0])

        # save checkpoint
        if epoch_itr.epoch % args.save_interval == 0:
            checkpoint_utils.save_checkpoint(args, trainer, epoch_itr, valid_losses[0])
            check_new_resources_availability(args)

        if ':' in getattr(args, 'data', ''):
            # sharded data: get train iterator for next epoch
            epoch_itr = trainer.get_train_iterator(epoch_itr.epoch)
    train_meter.stop()
    print('| done training in {:.1f} seconds'.format(train_meter.sum))


def train(args, trainer, task, epoch_itr):
    """Train the model for one epoch."""
    # Update parameters every N batches
    update_freq = args.update_freq[epoch_itr.epoch - 1] \
        if epoch_itr.epoch <= len(args.update_freq) else args.update_freq[-1]

    # Initialize data iterator
    itr = epoch_itr.next_epoch_itr(
        fix_batches_to_gpus=args.fix_batches_to_gpus,
        shuffle=(epoch_itr.epoch >= args.curriculum),
    )
    itr = iterators.GroupedIterator(itr, update_freq)
    progress = progress_bar.build_progress_bar(
        args, itr, epoch_itr.epoch, no_progress_bar='simple',
    )

    extra_meters = collections.defaultdict(lambda: AverageMeter())
    valid_subsets = args.valid_subset.split(',')
    max_update = args.max_update or math.inf
    for i, samples in enumerate(progress, start=epoch_itr.iterations_in_epoch):
        log_output = trainer.train_step(samples)
        if log_output is None:
            continue

        # log mid-epoch stats
        stats = get_training_stats(trainer)
        for k, v in log_output.items():
            if k in ['loss', 'nll_loss', 'ntokens', 'nsentences', 'sample_size']:
                continue  # these are already logged above
            if 'loss' in k or k == 'accuracy':
                extra_meters[k].update(v, log_output['sample_size'])
            else:
                extra_meters[k].update(v)
            stats[k] = extra_meters[k].avg
        progress.log(stats, tag='train', step=stats['num_updates'])

        # ignore the first mini-batch in words-per-second calculation
        if i == 0:
            trainer.get_meter('wps').reset()

        num_updates = trainer.get_num_updates()
        if (
            not args.disable_validation
            and args.save_interval_updates > 0
            and num_updates % args.save_interval_updates == 0
            and num_updates > 0
        ):
            valid_losses = validate(args, trainer, task, epoch_itr, valid_subsets)
            checkpoint_utils.save_checkpoint(args, trainer, epoch_itr, valid_losses[0])
            check_new_resources_availability(args)

        if num_updates >= max_update:
            break

    # log end-of-epoch stats
    stats = get_training_stats(trainer)
    for k, meter in extra_meters.items():
        stats[k] = meter.avg
    progress.print(stats, tag='train', step=stats['num_updates'])

    # reset training meters
    for k in [
        'train_loss', 'train_nll_loss', 'wps', 'ups', 'wpb', 'bsz', 'gnorm', 'clip',
    ]:
        meter = trainer.get_meter(k)
        if meter is not None:
            meter.reset()


class RayDistributedActor:
    def run(self, url, world_rank, args):
        print("Ray worker at {url} rank {rank}".format(url=url, rank=world_rank))
        self.url = url
        self.world_rank = world_rank
        args.distributed_rank = world_rank
        args.distributed_init_method = url
        main(args, init_distributed=(args.distributed_world_size > 1))

    def get_node_ip(self):
        """Returns the IP address of the current node."""
        return ray.services.get_node_ip_address()

    def find_free_port(self):
        """Finds a free port on the current node."""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]


def add_ray_args(parser):
    group = parser.add_argument_group('Ray related arguments')
    # fmt: off
    group.add_argument('--ray-address', default="auto", type=str,
                       help='address for ray initialization')
    group.add_argument('--fix-batch-size', default=None, type=int,
                       help='fix batch size (max_sentences * update_freq * n_GPUs) to be '
                            'a fixed input value for different number of GPUs or CPUs')
    # fmt: on
    return group


def ray_main():
    parser = options.get_training_parser()
    add_ray_args(parser)
    args = options.parse_args_and_arch(parser)
    original_args = copy.deepcopy(args)
    retry = True
    while retry:
        args = copy.deepcopy(original_args)
        ray.init(address=args.ray_address)
        if args.cpu:
            args.distributed_world_size = int(ray.cluster_resources()["CPU"])
        else:
            n_gpus = int(ray.cluster_resources().get("GPU", 0))
            while n_gpus == 0:
                print("No GPUs available, wait 10 seconds")
                time.sleep(10)
                n_gpus = int(ray.cluster_resources().get("GPU", 0))
            args.distributed_world_size = n_gpus
        if args.fix_batch_size is not None:
            args.update_freq = math.ceil(args.fix_batch_size / (args.max_sentences * args.distributed_world_size))
            print("Training on %d GPUs, max_sentences=%d, update_freq=%d"
                  % (args.distributed_world_size, args.max_sentences, args.fix_batch_size))
        Actor = ray.remote(num_cpus=1, num_gpus=int(not args.cpu))(RayDistributedActor)
        workers = [Actor.remote() for i in range(args.distributed_world_size)]
        ip = ray.get(workers[0].get_node_ip.remote())
        port = ray.get(workers[0].find_free_port.remote())
        address = "tcp://{ip}:{port}".format(ip=ip, port=port)
        unfinished = [worker.run.remote(address, i, args)for i, worker in enumerate(workers)]
        try:
            while len(unfinished) > 0:
                finished, unfinished = ray.wait(unfinished)
                finished = ray.get(finished)
            retry = False
        except Exception as inst:
            print("Ray restart because following error occurs:")
            print(inst)
            retry = True
        ray.shutdown()


if __name__ == '__main__':
    ray_main()
