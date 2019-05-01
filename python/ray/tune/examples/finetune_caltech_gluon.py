# Run the following to prepare Caltech-256 dataset first:
# $ sh download_caltech.sh
# $ python prepare_caltech.py --data ~/data/caltech/

from __future__ import print_function

import argparse
import random
import os

import mxnet as mx
import numpy as np

from mxnet import gluon, image, init, nd
from mxnet import autograd as ag
from mxnet.gluon import nn
from mxnet.gluon.data.vision import transforms
from gluoncv.model_zoo import get_model

# Training settings
parser = argparse.ArgumentParser(description='Caltech-256 Example')
parser.add_argument(
    '--data',
    type=str,
    default='~/data/caltech/',
    help='directory for the prepared data folder')
parser.add_argument(
    '--model',
    required=True,
    type=str,
    default='resnet50_v1b',
    help='name of the pretrained model from model zoo.')
parser.add_argument(
    '--batch_size',
    type=int,
    default=64,
    metavar='N',
    help='input batch size for training (default: 64)')
parser.add_argument(
    '--epochs',
    type=int,
    default=1,
    metavar='N',
    help='number of epochs to train (default: 1)')
parser.add_argument(
    '--seed',
    type=int,
    default=1,
    metavar='S',
    help='random seed (default: 1)')
parser.add_argument(
    '--smoke_test',
    action="store_true",
    help="Finish quickly for testing")
parser.add_argument(
    '--num_gpus',
    default=0,
    type=int,
    help='number of gpus to use, 0 indicates cpu only')
parser.add_argument(
    '--num_workers',
    default=4,
    type=int,
    help='number of preprocessing workers')
parser.add_argument(
    '--classes',
    type=int,
    default=257,
    metavar='N',
    help='number of outputs')
parser.add_argument(
    '--lr',
    default=0.001,
    type=float,
    help='initial learning rate')
parser.add_argument(
    '--momentum',
    default=0.9,
    type=float,
    help='momentum')
parser.add_argument(
    '--wd',
    default=1e-4,
    type=float,
    help='weight decay (default: 1e-4)')
parser.add_argument(
    '--lr_factor',
    default=0.75,
    type=float,
    help='learning rate decay ratio')
parser.add_argument(
    '--lr_step',
    default=20,
    type=int,
    help='list of learning rate decay epochs as in str')
parser.add_argument(
    '--expname',
    type=str,
    default='caltechexp')
parser.add_argument(
    '--reuse_actors',
    action="store_true",
    help="reuse actor")
parser.add_argument(
    '--checkpoint_freq',
    default=20, type=int,
    help='checkpoint_freq')
parser.add_argument(
    '--checkpoint_at_end',
    action="store_true",
    help="checkpoint_at_end")
parser.add_argument(
    '--max_failures',
    default=20,
    type=int,
    help='max_failures')
parser.add_argument(
    '--queue_trials',
    action="store_true",
    help="queue_trials")
parser.add_argument(
    '--with_server',
    action="store_true",
    help="with_server")
parser.add_argument(
    '--num_samples',
    type=int,
    default=50,
    metavar='N',
    help='number of samples')
parser.add_argument(
    '--scheduler',
    type=str,
    default='fifo')
args = parser.parse_args()


def train_caltech(args, config, reporter):
    vars(args).update(config)
    np.random.seed(args.seed)
    random.seed(args.seed)
    mx.random.seed(args.seed)

    # Set Hyper-params
    batch_size = args.batch_size * max(args.num_gpus, 1)
    ctx = [mx.gpu(i) for i in range(args.num_gpus)] if args.num_gpus > 0 else [mx.cpu()]

    # Define DataLoader
    train_path = os.path.join(args.data, 'train')
    test_path = os.path.join(args.data, 'val')

    jitter_param = 0.4
    lighting_param = 0.1
    normalize = transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])

    transform_train = transforms.Compose([
        transforms.RandomResizedCrop(224),
        transforms.RandomFlipLeftRight(),
        transforms.RandomColorJitter(brightness=jitter_param, contrast=jitter_param,
                                     saturation=jitter_param),
        transforms.RandomLighting(lighting_param),
        transforms.ToTensor(),
        normalize
    ])

    transform_test = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        normalize
    ])
    train_data = gluon.data.DataLoader(
        gluon.data.vision.ImageFolderDataset(train_path).transform_first(transform_train),
        batch_size=batch_size, shuffle=True, num_workers=args.num_workers)

    test_data = gluon.data.DataLoader(
        gluon.data.vision.ImageFolderDataset(test_path).transform_first(transform_test),
        batch_size=batch_size, shuffle=False, num_workers=args.num_workers)

    # Load model architecture and Initialize the net with pretrained model
    finetune_net = get_model(args.model, pretrained=True)
    with finetune_net.name_scope():
        finetune_net.fc = nn.Dense(args.classes)
    finetune_net.fc.initialize(init.Xavier(), ctx=ctx)
    finetune_net.collect_params().reset_ctx(ctx)
    finetune_net.hybridize()

    # Define trainer
    trainer = gluon.Trainer(finetune_net.collect_params(), 'sgd', {
        'learning_rate': args.lr, 'momentum': args.momentum, 'wd': args.wd})
    L = gluon.loss.SoftmaxCrossEntropyLoss()
    metric = mx.metric.Accuracy()

    def train(epoch):
        if epoch == args.lr_step:
            trainer.set_learning_rate(trainer.learning_rate * args.lr_factor)

        for i, batch in enumerate(train_data):
            data = gluon.utils.split_and_load(batch[0], ctx_list=ctx, batch_axis=0,
                                              even_split=False)
            label = gluon.utils.split_and_load(batch[1], ctx_list=ctx, batch_axis=0,
                                               even_split=False)
            with ag.record():
                outputs = [finetune_net(X) for X in data]
                loss = [L(yhat, y) for yhat, y in zip(outputs, label)]
            for l in loss:
                l.backward()

            trainer.step(batch_size)
        mx.nd.waitall()

    def test():
        test_loss = 0
        for i, batch in enumerate(test_data):
            data = gluon.utils.split_and_load(batch[0], ctx_list=ctx, batch_axis=0,
                                              even_split=False)
            label = gluon.utils.split_and_load(batch[1], ctx_list=ctx, batch_axis=0,
                                               even_split=False)
            outputs = [finetune_net(X) for X in data]
            loss = [L(yhat, y) for yhat, y in zip(outputs, label)]

            test_loss += sum([l.mean().asscalar() for l in loss]) / len(loss)
            metric.update(label, outputs)

        _, test_acc = metric.get()
        test_loss /= len(test_data)
        reporter(mean_loss=test_loss, mean_accuracy=test_acc)

    for epoch in range(1, args.epochs + 1):
        train(epoch)
        test()


if __name__ == "__main__":
    args = parser.parse_args()

    import ray
    from ray import tune
    from ray.tune.schedulers import AsyncHyperBandScheduler, FIFOScheduler

    ray.init()
    if args.scheduler == 'fifo':
        sched = FIFOScheduler()
    elif args.scheduler == 'asynchyperband':
        sched = AsyncHyperBandScheduler(
            time_attr="training_iteration",
            reward_attr="neg_mean_loss",
            max_t=400,
            grace_period=60)
    else:
        raise NotImplementedError
    tune.register_trainable(
        "TRAIN_FN",
        lambda config, reporter: train_caltech(args, config, reporter))
    tune.run(
        "TRAIN_FN",
        name=args.expname,
        verbose=2,
        scheduler=sched,
        reuse_actors=args.reuse_actors,
        checkpoint_freq=args.checkpoint_freq,
        checkpoint_at_end=args.checkpoint_at_end,
        max_failures=args.max_failures,
        queue_trials=args.queue_trials,
        with_server=args.with_server,
        **{
            "stop": {
                "mean_accuracy": 0.90,
                "training_iteration": 1 if args.smoke_test else args.epochs
            },
            "resources_per_trial": {
                "cpu": int(args.num_workers),
                "gpu": int(args.num_gpus)
            },
            "num_samples": 1 if args.smoke_test else args.num_samples,
            "config": {
                "lr": tune.sample_from(
                    lambda spec: np.power(10.0, np.random.uniform(-5, -2))),
                "momentum": tune.sample_from(
                    lambda spec: np.random.uniform(0.85, 0.95))
            }
        })
