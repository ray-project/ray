from __future__ import print_function

import argparse
import random

import mxnet as mx
import numpy as np

from mxnet import gluon, init
from mxnet import autograd as ag
from mxnet.gluon import nn
from mxnet.gluon.data.vision import transforms
from gluoncv.model_zoo import get_model
from gluoncv.data import transforms as gcv_transforms

from ray.tune.schedulers import create_scheduler
from ray import tune

# Training settings
parser = argparse.ArgumentParser(description="CIFAR-10 Example")
parser.add_argument(
    "--model",
    required=True,
    type=str,
    default="resnet50_v1b",
    help="name of the pretrained model from gluoncv model zoo"
    "(default: resnet50_v1b).")
parser.add_argument(
    "--batch_size",
    type=int,
    default=64,
    metavar="N",
    help="input batch size for training (default: 64)")
parser.add_argument(
    "--epochs",
    type=int,
    default=1,
    metavar="N",
    help="number of epochs to train (default: 1)")
parser.add_argument(
    "--num_gpus",
    default=0,
    type=int,
    help="number of gpus to use, 0 indicates cpu only (default: 0)")
parser.add_argument(
    "--num_workers",
    default=4,
    type=int,
    help="number of preprocessing workers (default: 4)")
parser.add_argument(
    "--classes",
    type=int,
    default=10,
    metavar="N",
    help="number of outputs (default: 10)")
parser.add_argument(
    "--lr",
    default=0.001,
    type=float,
    help="initial learning rate (default: 0.001)")
parser.add_argument(
    "--momentum",
    default=0.9,
    type=float,
    help="initial momentum (default: 0.9)")
parser.add_argument(
    "--wd", default=1e-4, type=float, help="weight decay (default: 1e-4)")
parser.add_argument(
    "--expname", type=str, default="cifar10exp", help="experiments location")
parser.add_argument(
    "--num_samples",
    type=int,
    default=20,
    metavar="N",
    help="number of samples (default: 20)")
parser.add_argument(
    "--scheduler",
    type=str,
    default="fifo",
    help="FIFO or AsyncHyperBandScheduler.")
parser.add_argument(
    "--seed",
    type=int,
    default=1,
    metavar="S",
    help="random seed (default: 1)")
parser.add_argument(
    "--smoke_test", action="store_true", help="Finish quickly for testing")
args = parser.parse_args()


def train_cifar10(config):
    args = config.pop("args")
    vars(args).update(config)
    np.random.seed(args.seed)
    random.seed(args.seed)
    mx.random.seed(args.seed)

    # Set Hyper-params
    batch_size = args.batch_size * max(args.num_gpus, 1)
    ctx = [mx.gpu(i)
           for i in range(args.num_gpus)] if args.num_gpus > 0 else [mx.cpu()]

    # Define DataLoader
    transform_train = transforms.Compose([
        gcv_transforms.RandomCrop(32, pad=4),
        transforms.RandomFlipLeftRight(),
        transforms.ToTensor(),
        transforms.Normalize([0.4914, 0.4822, 0.4465],
                             [0.2023, 0.1994, 0.2010])
    ])

    transform_test = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize([0.4914, 0.4822, 0.4465],
                             [0.2023, 0.1994, 0.2010])
    ])

    train_data = gluon.data.DataLoader(
        gluon.data.vision.CIFAR10(train=True).transform_first(transform_train),
        batch_size=batch_size,
        shuffle=True,
        last_batch="discard",
        num_workers=args.num_workers)

    test_data = gluon.data.DataLoader(
        gluon.data.vision.CIFAR10(train=False).transform_first(transform_test),
        batch_size=batch_size,
        shuffle=False,
        num_workers=args.num_workers)

    # Load model architecture and Initialize the net with pretrained model
    finetune_net = get_model(args.model, pretrained=True)
    with finetune_net.name_scope():
        finetune_net.fc = nn.Dense(args.classes)
    finetune_net.fc.initialize(init.Xavier(), ctx=ctx)
    finetune_net.collect_params().reset_ctx(ctx)
    finetune_net.hybridize()

    # Define trainer
    trainer = gluon.Trainer(finetune_net.collect_params(), "sgd", {
        "learning_rate": args.lr,
        "momentum": args.momentum,
        "wd": args.wd
    })
    L = gluon.loss.SoftmaxCrossEntropyLoss()
    metric = mx.metric.Accuracy()

    def train(epoch):
        for i, batch in enumerate(train_data):
            data = gluon.utils.split_and_load(
                batch[0], ctx_list=ctx, batch_axis=0, even_split=False)
            label = gluon.utils.split_and_load(
                batch[1], ctx_list=ctx, batch_axis=0, even_split=False)
            with ag.record():
                outputs = [finetune_net(X) for X in data]
                loss = [L(yhat, y) for yhat, y in zip(outputs, label)]
            for ls in loss:
                ls.backward()

            trainer.step(batch_size)
        mx.nd.waitall()

    def test():
        test_loss = 0
        for i, batch in enumerate(test_data):
            data = gluon.utils.split_and_load(
                batch[0], ctx_list=ctx, batch_axis=0, even_split=False)
            label = gluon.utils.split_and_load(
                batch[1], ctx_list=ctx, batch_axis=0, even_split=False)
            outputs = [finetune_net(X) for X in data]
            loss = [L(yhat, y) for yhat, y in zip(outputs, label)]

            test_loss += sum(ls.mean().asscalar() for ls in loss) / len(loss)
            metric.update(label, outputs)

        _, test_acc = metric.get()
        test_loss /= len(test_data)
        return test_loss, test_acc

    for epoch in range(1, args.epochs + 1):
        train(epoch)
        test_loss, test_acc = test()
        tune.report(mean_loss=test_loss, mean_accuracy=test_acc)


if __name__ == "__main__":
    args = parser.parse_args()
    sched = create_scheduler(args.scheduler)

    analysis = tune.run(
        train_cifar10,
        name=args.expname,
        verbose=2,
        scheduler=sched,
        stop={
            "mean_accuracy": 0.98,
            "training_iteration": 1 if args.smoke_test else args.epochs
        },
        resources_per_trial={
            "cpu": int(args.num_workers),
            "gpu": int(args.num_gpus)
        },
        num_samples=1 if args.smoke_test else args.num_samples,
        config={
            "args": args,
            "lr": tune.loguniform(1e-4, 1e-1),
            "momentum": tune.uniform(0.85, 0.95),
        })
    print("Best hyperparameters found were: ", analysis.best_config)
