import argparse
import os
import json
from datetime import timedelta
import time

import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel
import torch.optim as optim
import torch.multiprocessing as mp
import torch.nn.functional as F

from torchvision import models

import ray
from ray.util.ml_utils.util import find_free_port
from ray import train
from ray.train import Trainer
from ray.train.torch import TorchConfig

from ray.util.placement_group import placement_group

def torch_setup(rank, world_size, num_workers_per_node, address, port):
    os.environ["MASTER_ADDR"] = address
    os.environ["MASTER_PORT"] = str(port)

    print(f"rank: {rank}, world size: {world_size}")

    # initialize the process group
    dist.init_process_group("nccl", rank=rank,
                                       world_size=world_size,
                            init_method="env://",
                            timeout=timedelta(seconds=30))

    print("finished process group setup")
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in range(
        num_workers_per_node)])

def torch_cleanup():
    dist.destroy_process_group()
    torch.cuda.empty_cache()
    del os.environ["CUDA_VISIBLE_DEVICES"]

def torch_prepare_model(model, rank):
    device = torch.device(f"cuda:{rank}")
    model = model.to(device)
    model = DistributedDataParallel(model, device_ids=[rank])
    return model

def train_func(config):
    print("Starting Training...")

    mode = config["mode"]

    # Call function to get rank.
    if mode == "torch":
        rank = config["rank"]
    else:
        rank = train.local_rank()

    # Create model and move to device.
    device = torch.device(f"cuda:{rank}")

    model = models.resnet50()

    if mode == "torch":
        model = torch_prepare_model(model, rank)
    else:
        model = ray.train.torch.prepare_model(model)

    # Create optimizer.
    optimizer = optim.SGD(model.parameters(), lr=0.01)
    loss_fn = F.cross_entropy

    batch_size = config["batch_size"]

    # Set up fixed fake data and move it to the appropriate device.
    data = torch.randn(batch_size, 3, 224, 224)
    target = torch.randint(high=1000, size=(batch_size,), dtype=torch.long)
    data, target = data.to(device), target.to(device)

    num_epochs = config["num_epochs"]
    num_batches_per_epoch = config["num_batches_per_epoch"]


    for i in range(num_epochs):
        print("Starting epoch ", i)
        current_time = time.time()
        for _ in range(num_batches_per_epoch):
            optimizer.zero_grad()
            output = model(data)
            loss = loss_fn(output, target)
            loss.backward()
            optimizer.step()
        epoch_time = time.time() - current_time
        img_sec = batch_size * num_batches_per_epoch / epoch_time
        print("Iter #%d: %.1f img/sec per %s" % (i, img_sec, device))


def torch_train(rank, world_size, num_workers_per_node, address, port):
    """Training function for vanilla PyTorch."""
    print("Setting up distributed torch.")
    torch_setup(rank=rank, world_size=world_size,
                num_workers_per_node=num_workers_per_node, address=address,
                port=port)
    config = CONFIG.copy()
    config["mode"] = "torch"
    config["rank"] = rank
    train_func(config)
    torch_cleanup()


def torch_run_single_node(train_func, world_size, num_workers_per_node,
                         address, port):
    current_time = time.time()
    mp.spawn(train_func,
                 args=(world_size,num_workers_per_node,address,port),
                 nprocs=num_workers_per_node,
                 join=True)
    total_time = time.time() - current_time
    print("Total time for vanilla torch.distributed: ", time.time() -
          current_time)
    return total_time

def ray_train_run(train_func, world_size):
    current_time = time.time()
    trainer = Trainer(num_workers=world_size, use_gpu=True,
                      backend=TorchConfig(backend="nccl"))
    trainer.start()

    config = CONFIG.copy()
    config["mode"] = "ray"
    trainer.run(train_func, config=config)
    total_time = time.time() - current_time
    print("Total time for Ray Train: ", total_time)
    return total_time

def single_node_benchmark():
    import ray
    ray.init("auto")

    assert NUM_NODES*NUM_GPUS_PER_NODE == NUM_GPUS_PER_NODE

    # Time torch.distributed using torch.multiprocessing.
    address = "127.0.0.1"
    port = find_free_port()
    torch_time = torch_run_single_node(train_func=torch_train,
                                       world_size=NUM_NODES*NUM_GPUS_PER_NODE,
                           num_workers_per_node=NUM_GPUS_PER_NODE,
                           address=address, port=port)

    # Time using Ray Train.
    ray_time = ray_train_run(train_func=train_func, world_size=4)

    # Make sure that the torch.distributed time and Ray Train time are
    # within 5% of each other.
    assert abs(torch_time - ray_time) <= min(0.05 * torch_time,
                                             0.05 * ray_time), \
        f"torch.distributed time: {torch_time}, Ray Train time: {ray_time}"

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(
            json.dumps({"torch_time": torch_time, "ray_train_time": ray_time}))

def torch_run_distributed(num_nodes, num_workers_per_node):
    # Use Ray tasks and placement groups to set up distributed training.
    # This allows us to run commands on all nodes in the cluster.
    per_node_bundle = {"CPU": num_workers_per_node, "GPU": num_workers_per_node}
    bundles = [per_node_bundle] * num_nodes
    pg = placement_group(bundles, strategy="STRICT_SPREAD")
    ray.get(pg.ready())

    # Use current node for the address and port.
    address = ray.util.get_node_ip_address()
    port = find_free_port()

    remote_tasks = []
    for node in range(num_nodes):
        remote_task = ray.remote(torch_run_single_node).options(
            placement_group_bundle_index=node)
        remote_tasks.append(remote_task)

    current_time = time.time()
    # Ray grpc overhead is negligible.
    ray.get([task.remote(train_func=torch_train,
                         world_size=num_nodes*num_workers_per_node,
                         num_workers_per_node=num_workers_per_node,
                         address=address, port=port) for
             task in remote_tasks])
    total_time = time.time() - current_time
    return total_time


def multi_node_benchmark():
    import ray
    ray.init("auto")

    # Time using torch.distributed.
    torch_time = torch_run_distributed(num_nodes=NUM_NODES,
                                       num_workers_per_node=NUM_GPUS_PER_NODE)

    # Time using Ray Train.
    ray_time = ray_train_run(train_func=train_func, world_size=NUM_NODES*NUM_GPUS_PER_NODE)

    # Make sure that the torch.distributed time and Ray Train time are
    # within 5% of each other.
    assert abs(torch_time - ray_time) <= min(0.05 * torch_time,
                                             0.05 * ray_time), \
        f"torch.distributed time: {torch_time}, Ray Train time: {ray_time}"

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(
            json.dumps({"torch_time": torch_time, "ray_train_time": ray_time}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-nodes",
        "-n",
        type=int,
        default=1,
        help="Sets number of nodes in cluster.")
    parser.add_argument(
        "--num-gpu-per-node",
        "-n",
        type=int,
        default=4,
        help="Sets number of GPUs per node to use for training.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Training batch size.")
    parser.add_argument(
        "--num-epochs",
        type=int,
        default=10,
        help="Number of epochs to train for.")
    parser.add_argument(
        "--batches-per-epoch",
        type=int,
        default=100,
        help="Number of batches to train for each epoch.")
    args, _ = parser.parse_known_args()

    NUM_NODES = args.num_nodes
    NUM_GPUS_PER_NODE = args.num_gpu_per_node

    CONFIG = {
        "batch_size": args.batch_size,
        "num_epochs": args.num_epochs,
        "num_batches_per_epoch": args.batches_per_epoch
    }

    if NUM_NODES == 1:
        single_node_benchmark()
    else:
        multi_node_benchmark()