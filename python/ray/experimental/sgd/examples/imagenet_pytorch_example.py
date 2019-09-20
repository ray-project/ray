from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import logging
import math
import torch
import torch.nn as nn
import argparse
import time
from ray import tune
from ray.experimental.sgd.pytorch.pytorch_trainer import PyTorchTrainer, PyTorchTrainable
from ray.experimental.sgd.models.resnet import ResNet18

import ray
from ray.autoscaler import autoscaler
from ray.experimental.sgd.tests.pytorch_utils import optimizer_creator, cifar_creator, imagenet_creator

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def initialization_hook(runner):
    print("NCCL DEBUG SET")
    # Need this for avoiding a connection restart issue
    os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
    os.environ["NCCL_LL_THRESHOLD"] = "0"
#    os.environ["NCCL_DEBUG"] = "INFO"


def resize_cluster(target_gpus, blocking=True):
    res = ray.cluster_resources()
    logger.info("Assuming all machines are same in cluster.")
    if res['GPU'] < target_gpus:
        CPU_per_GPU = res['CPU'] / res['GPU']
        target_cpus = math.ceil(CPU_per_GPU * target_gpus)
        autoscaler.request_resources(num_cpus=target_cpus)
        logger.info(f"Target CPUs: {target_cpus}")

    total_gpus = ray.cluster_resources()["GPU"]
    logger.info(f"Total GPUs / Required GPUs: {total_gpus}/{target_gpus}")
    while total_gpus < target_gpus and blocking:
        time.sleep(1)
        logger.info(f"Total GPUs / Required GPUs: {total_gpus}/{target_gpus}")
        total_gpus = ray.cluster_resources()["GPU"]

def train(model, train_iterator, criterion, optimizer):
    model.train()
    train_loss, total_num, correct = 0, 0, 0
    for batch_idx, (data, target) in enumerate(train_iterator):
        # get small model update
        if torch.cuda.is_available():
            data, target = data.cuda(), target.cuda()
        output = model(data)
        loss = criterion(output, target)  # / float(large_ratio)
        loss.backward()
        train_loss += loss.item() * target.size(0)  # * float(large_ratio)
        total_num += target.size(0)
        _, predicted = output.max(1)
        correct += predicted.eq(target).sum().item()
        optimizer.step()
        optimizer.zero_grad()
    stats = {"train_loss": train_loss / total_num, 'train_acc': correct / total_num}
    return stats


def resizing_train_example(use_gpu=False):
    import torchvision.models as models
    def create_trainer(num_replicas):
        return PyTorchTrainer(
            model_creator=models.resnet18,
            data_creator=imagenet_creator,
            optimizer_creator=optimizer_creator,
            loss_creator=nn.CrossEntropyLoss,
            initialization_hook=initialization_hook,
            train_function=train,
            num_replicas=num_replicas,
            batch_size=256 * num_replicas,
            use_gpu=use_gpu,
            backend="nccl"
        )
    t = time.time()
    trainer = create_trainer(40)
    t = time.time() - t
    print('startup:', t)
    print('40 WORKERS')

    for target in [40, 40]:
        for i in range(20):
            t = time.time()
            stats = trainer.train()
            t = time.time() - t
            print('training:', t)

        t = time.time()
        logger.info('Checkpoint and resize')
        trainer.save('checkpoint.pth')
        logger.info("Saved.")

        resize_cluster(target)
        trainer.shutdown()
        trainer = create_trainer(target)
        trainer.restore('checkpoint.pth')
        t = time.time() - t
        print('resize:', t)
        print(target, ' WORKERS')
    logger.info('finish training')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-address",
        required=False,
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")

    args, _ = parser.parse_known_args()

    import ray

    ray.init(redis_address=args.redis_address)
    resizing_train_example(use_gpu=args.use_gpu)
