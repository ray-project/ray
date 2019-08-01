from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from ray.experimental.sgd.pytorch import PyTorchTrainer

from ray.experimental.sgd.tests.pytorch_utils import (
    model_creator, optimizer_creator, data_creator)


# def train(train_iterator, model, criterion, optimizer):
#     model.train()
#     for batch_idx, (data, target) in enumerate(train_iterator):
#         if target.size(0) < 128:
#             continue
#         # gather input and target for large batch training
#         inner_loop += 1
#         # get small model update
#         if cuda:
#             data, target = data.cuda(), target.cuda()
#         output = model(data)
#         loss = criterion(output, target) / float(large_ratio)
#         loss.backward()
#         train_loss += loss.item() * target.size(0) * float(large_ratio)
#         total_num += target.size(0)
#         _, predicted = output.max(1)
#         correct += predicted.eq(target).sum().item()
#         optimizer.step()
#         optimizer.zero_grad()
#     stats = {}
#     return stats


def train_example(num_replicas=1, use_gpu=False):
    trainer1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        # train_function=train,
        num_replicas=num_replicas,
        use_gpu=use_gpu,
        backend="nccl")
    stats = trainer1.train()
    # adjust_num_workers(stats)
    trainer1.shutdown()
    print("success!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-address",
        required=False,
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--num-replicas",
        "-n",
        type=int,
        default=1,
        help="Sets number of replicas for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    args, _ = parser.parse_known_args()

    import ray

    ray.init(redis_address=args.redis_address)
    train_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
