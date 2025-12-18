import argparse
import json
import os
import socket
import time

import torch

import ray
from ray.experimental.collective import (
    create_collective_group,
    destroy_all_collective_groups,
)

ray.init(
    runtime_env={
        "env_vars": {
            # Needed for torch distributed.
            "MASTER_ADDR": socket.gethostbyname(socket.gethostname()),
            "MASTER_PORT": "8888",
        }
    }
)


@ray.remote(num_gpus=1, enable_tensor_transport=True)
class GPUActor:
    def send(self, size_in_bytes, device):
        return torch.ones(size_in_bytes, dtype=torch.int8, device=device)

    def recv(self, rdt_tensor: torch.Tensor):
        return rdt_tensor[0].item()

    def init_torch(self, rank):
        self.rank = rank
        torch.distributed.init_process_group(
            backend="nccl",
            world_size=2,
            rank=rank,
        )

    def send_with_torch(self, size_in_bytes, device, other_rank):
        buf = torch.ones(size_in_bytes, dtype=torch.int8, device=device)
        torch.distributed.send(buf, other_rank)

    def recv_with_torch(self, size_in_bytes, device, other_rank):
        buf = torch.empty(size_in_bytes, dtype=torch.int8, device=device)
        torch.distributed.recv(buf, other_rank)
        return buf[0].item()

    def send_many_with_torch(self, size_in_bytes, device, other_rank, num_transfers):
        for _ in range(num_transfers):
            buf = torch.ones(size_in_bytes, dtype=torch.int8, device=device)
            torch.distributed.send(buf, other_rank)

    def recv_many_with_torch(self, size_in_bytes, device, other_rank, num_transfers):
        results = []
        for _ in range(num_transfers):
            buf = torch.empty(size_in_bytes, dtype=torch.int8, device=device)
            torch.distributed.recv(buf, other_rank)
            results.append(buf[0].item())
        return results


"""
THROUGHPUT
- NEW SEND OBJECT PER RECV
- SAME SEND OBJECT PER RECV
LATENCY
- JUST 1 TRANSFER
TORCH_LATENCY
- JUST 1 TRANSFER
TORCH THROUGHPUT
- NEW SEND PER RECV (all transfers done inside just 2 ray tasks)
"""


def throughput_new_send_per_recv(
    num_transfers, transport, size, device, sender, receiver
):
    refs = []
    ########### optional warmup
    send_ref = sender.send.options(tensor_transport=transport).remote(size, device)
    ray.get(receiver.recv.remote(send_ref))
    ############
    start = time.perf_counter()
    for _ in range(num_transfers):
        send_ref = sender.send.options(tensor_transport=transport).remote(size, device)
        refs.append(receiver.recv.remote(send_ref))
    ray.get(refs)
    return time.perf_counter() - start


def throughput_same_send_per_recv(
    num_transfers, transport, size, device, sender, receiver
):
    refs = []
    ########### optional warmup
    send_ref = sender.send.options(tensor_transport=transport).remote(size, device)
    ray.get(receiver.recv.remote(send_ref))
    ############
    start = time.perf_counter()
    send_ref = sender.send.options(tensor_transport=transport).remote(size, device)
    for _ in range(num_transfers):
        refs.append(receiver.recv.remote(send_ref))
    ray.get(refs)
    return time.perf_counter() - start


def latency_test(_num_transfers, transport, size, device, sender, receiver):
    times = []
    for _ in range(10):
        start = time.perf_counter()
        ray.get(
            receiver.recv.remote(
                sender.send.options(tensor_transport=transport).remote(size, device)
            )
        )
        times.append(time.perf_counter() - start)
    return sum(times) / len(times)


def torch_latency(_num_transfers, _transport, size, device, sender, receiver):
    times = []
    for _ in range(10):
        start = time.perf_counter()
        send_ref = sender.send_with_torch.remote(size, device, 1)
        recv_ref = receiver.recv_with_torch.remote(size, device, 0)
        ray.get([send_ref, recv_ref])
        times.append(time.perf_counter() - start)
    return sum(times) / len(times)


def torch_throughput(num_transfers, _transport, size, device, sender, receiver):
    start_time = time.perf_counter()
    send_ref = sender.send_many_with_torch.remote(size, device, 1, num_transfers)
    recv_ref = receiver.recv_many_with_torch.remote(size, device, 0, num_transfers)
    ray.get([send_ref, recv_ref])
    return time.perf_counter() - start_time


# torch funcs only for when directly testing torch distributed
TEST_FUNCS = [
    throughput_new_send_per_recv,
    throughput_same_send_per_recv,
    latency_test,
    # torch_latency, added based on cli arg
    # torch_throughput, added based on cli arg
]

# (transport, device)
TRANSPORTS_AND_DEVICE = [
    ("nccl", "cuda"),
    # ("nixl", "cuda"), # nixl enabled based on cli arg
    # ("nixl", "cpu"),
    ("gloo", "cpu"),
    # ("object_store", "cpu"),
    # ("object_store", "cuda"),
    # ("torch", "cuda") # only works with torch TEST_FUNCS, added based on cli arg
]

# (size_str, size, num_transfers)
SIZES_AND_NUM_TRANSFERS = [
    ("4B", 4, 50),
    # ("1KB",   (1024),               50),
    # ("50KB",  (50 * 1024),          50),
    ("150KB", (150 * 1024), 50),
    # ("500KB", (500 * 1024),         50),
    ("1MB", (1024 * 1024), 50),
    # ("10MB",  (10 * 1024 * 1024),   50),
    # ("50MB",  (50 * 1024 * 1024),   50),
    ("100MB", (100 * 1024 * 1024), 50),
    # ("512MB", (512 * 1024 * 1024),  20),
    ("1GB", (1024 * 1024 * 1024), 10),
    # ("10GB", (10 * 1024 * 1024 * 1024), 1) - added based on cli arg
]


def do_benchmark(transport, device, test_func):
    # Create actors + collective group
    sender = GPUActor.remote()
    receiver = GPUActor.remote()
    if transport == "nccl" or transport == "gloo":
        create_collective_group([sender, receiver], transport)

    # Initialize
    if transport == "torch":
        ray.get([sender.init_torch.remote(0), receiver.init_torch.remote(1)])
    else:
        ray.get(
            receiver.recv.remote(
                sender.send.options(tensor_transport=transport).remote(4, device)
            )
        )

    # Bench per size
    bench_times = []
    print(f"Benchmark times for transport {transport}, test_func {test_func.__name__}")
    for size_str, size, num_transfers in SIZES_AND_NUM_TRANSFERS:
        bench_time = test_func(num_transfers, transport, size, device, sender, receiver)
        bench_times.append(
            {
                "transport": transport,
                "test_func": test_func.__name__,
                "num_transfers": num_transfers,
                "size_str": size_str,
                "bench_time": bench_time,
            }
        )

        extra_pad = (10 - len(size_str)) * " "
        if test_func == latency_test or test_func == torch_latency:
            print(f"Size {size_str}{extra_pad}: {bench_time}")
        else:
            print(
                f"{num_transfers} Transfers, Size {size_str}{extra_pad}: {bench_time}"
            )

        # Cool off, GC time
        time.sleep(2)

    destroy_all_collective_groups()
    print()
    return bench_times


parser = argparse.ArgumentParser()
parser.add_argument(
    "--enable_10gb",
    action="store_true",
)
parser.add_argument(
    "--enable_nixl",
    action="store_true",
)
parser.add_argument(
    "--enable_torch_bench",
    action="store_true",
)
args = parser.parse_args()
if args.enable_10gb:
    SIZES_AND_NUM_TRANSFERS.append(("10GB", (10 * 1024 * 1024 * 1024), 1))

if args.enable_nixl:
    TRANSPORTS_AND_DEVICE.append(("nixl", "cuda"))

if args.enable_torch_bench:
    TEST_FUNCS.append(torch_latency)
    TEST_FUNCS.append(torch_throughput)
    TRANSPORTS_AND_DEVICE.append(("torch", "cuda"))


bench_results = []
for test_func in TEST_FUNCS:
    for transport, device in TRANSPORTS_AND_DEVICE:
        if (
            test_func == torch_latency or test_func == torch_throughput
        ) and transport != "torch":
            continue
        if transport == "torch" and (
            test_func != torch_latency and test_func != torch_throughput
        ):
            continue
        bench_results.extend(do_benchmark(transport, device, test_func))


if "TEST_OUTPUT_JSON" in os.environ:
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
        # NOTE that throughput results are also returned as a time because we have to fix the amount of memory
        # being moved to avoid GPU memory OOM's.
        results = {}
        results["perf_metrics"] = [
            {
                "perf_metric_name": f"{res['transport']}-{res['size_str']}-{res['test_func']}",
                "perf_metric_value": res["bench_time"],
                "perf_metric_type": "LATENCY",
            }
            for res in bench_results
        ]

        json.dump(results, out_file)
