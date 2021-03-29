import torch
import argparse
import torch.distributed as dist
import os

parser = argparse.ArgumentParser()
parser.add_argument("--local_rank", type=int, default=0)
parser.add_argument("--distributed", type=bool, default=False)

args=parser.parse_args()

os.environ["NCCL_SOCKET_IFNAME"]='eno1'

num_gpus = int(os.environ["WORLD_SIZE"]) if "WORLD_SIZE" in os.environ else 1

args.distributed = num_gpus > 1

print("enter init")
# dist.init_process_group(backend="nccl", init_method="env://")
dist.init_process_group(backend="nccl", init_method="tcp://172.18.167.21:23456",
                        rank=1, world_size=2)


device = torch.device("cuda", args.local_rank)

data = torch.ones(3,3).to(device)

print("calling reduce")
dist.all_reduce(data)

print(data)
