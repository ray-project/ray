#!/usr/bin/env python3
"""
Rust CoreWorker-based GPU actor worker.

Each instance pins to one GPU, joins an NCCL process group, and accepts
actor method calls for GPU tensor operations and direct GPU-to-GPU transfer.

Usage:
    CUDA_VISIBLE_DEVICES=0 python3 gpu_worker.py \
        --gcs-address <gcs_ip>:<gcs_port> \
        --gpu-id 0 --nccl-rank 0 --nccl-world-size 4 \
        --nccl-init-method tcp://<gpu_node_ip>:29500
"""
import argparse
import os
import pickle
import json
import socket
import time
import sys


def get_private_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser(description="GPU actor worker")
    parser.add_argument("--gcs-address", required=True)
    parser.add_argument("--gpu-id", type=int, required=True)
    parser.add_argument("--nccl-rank", type=int, required=True)
    parser.add_argument("--nccl-world-size", type=int, required=True)
    parser.add_argument("--nccl-init-method", required=True)
    parser.add_argument("--bind-ip", default="0.0.0.0")
    parser.add_argument("--bind-port", type=int, default=0)
    args = parser.parse_args()

    # Pin to specific GPU BEFORE importing torch
    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu_id)

    import torch
    import torch.distributed as dist

    from _raylet import PyCoreWorker, PyWorkerType

    node_ip = get_private_ip()
    print(f"[gpu_worker rank={args.nccl_rank}] Node IP: {node_ip}")
    print(f"[gpu_worker rank={args.nccl_rank}] GPU ID: {args.gpu_id}")
    print(f"[gpu_worker rank={args.nccl_rank}] CUDA available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"[gpu_worker rank={args.nccl_rank}] GPU: {torch.cuda.get_device_name(0)}")

    worker = PyCoreWorker(
        int(PyWorkerType.Worker),
        node_ip,
        args.gcs_address,
        1,
    )
    worker_id_hex = worker.worker_id().hex()
    print(f"[gpu_worker rank={args.nccl_rank}] Worker ID: {worker_id_hex}")

    port = worker.start_grpc_server(args.bind_ip, args.bind_port)
    print(f"[gpu_worker rank={args.nccl_rank}] gRPC server on {args.bind_ip}:{port}")

    # Actor state
    state = {
        "tensor": None,
        "rank": args.nccl_rank,
        "world_size": args.nccl_world_size,
        "init_method": args.nccl_init_method,
        "gpu_id": args.gpu_id,
        "nccl_initialized": False,
    }

    TASK_FUNCTIONS = {}

    def register_task(name):
        def decorator(fn):
            TASK_FUNCTIONS[name] = fn
            return fn
        return decorator

    @register_task("gpu_info")
    def gpu_info():
        if not torch.cuda.is_available():
            return {"error": "CUDA not available"}
        props = torch.cuda.get_device_properties(0)
        mem_mb = int(getattr(props, 'total_memory', getattr(props, 'total_mem', 0))) // (1024 * 1024)
        return {
            "gpu_name": str(torch.cuda.get_device_name(0)),
            "gpu_memory_mb": mem_mb,
            "cuda_version": str(torch.version.cuda),
            "torch_version": str(torch.__version__),
            "rank": int(state["rank"]),
            "physical_gpu_id": int(state["gpu_id"]),
            "node_ip": str(get_private_ip()),
        }

    @register_task("init_nccl")
    def init_nccl():
        try:
            dist.init_process_group(
                backend="nccl",
                init_method=state["init_method"],
                world_size=state["world_size"],
                rank=state["rank"],
            )
            state["nccl_initialized"] = True
            return {
                "status": "ok",
                "rank": int(state["rank"]),
                "world_size": int(dist.get_world_size()),
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    @register_task("create_tensor")
    def create_tensor(shape, fill_value):
        shape = list(shape)
        state["tensor"] = torch.full(shape, float(fill_value), dtype=torch.float32, device="cuda:0")
        return {
            "shape": shape,
            "fill_value": float(fill_value),
            "nbytes": int(state["tensor"].nelement() * state["tensor"].element_size()),
            "device": str(state["tensor"].device),
            "rank": int(state["rank"]),
        }

    @register_task("nccl_send")
    def nccl_send(dst_rank):
        t0 = time.time()
        dist.send(state["tensor"], dst=int(dst_rank))
        elapsed = time.time() - t0
        nbytes = int(state["tensor"].nelement() * state["tensor"].element_size())
        return {
            "status": "ok",
            "dst_rank": int(dst_rank),
            "nbytes": nbytes,
            "elapsed_s": float(elapsed),
            "bandwidth_gbps": float(nbytes / elapsed / 1e9) if elapsed > 0 else float("inf"),
            "rank": int(state["rank"]),
        }

    @register_task("nccl_recv")
    def nccl_recv(src_rank, shape):
        shape = list(shape)
        buf = torch.empty(shape, dtype=torch.float32, device="cuda:0")
        t0 = time.time()
        dist.recv(buf, src=int(src_rank))
        elapsed = time.time() - t0
        state["tensor"] = buf
        nbytes = int(buf.nelement() * buf.element_size())
        return {
            "status": "ok",
            "src_rank": int(src_rank),
            "nbytes": nbytes,
            "elapsed_s": float(elapsed),
            "bandwidth_gbps": float(nbytes / elapsed / 1e9) if elapsed > 0 else float("inf"),
            "rank": int(state["rank"]),
        }

    @register_task("all_reduce_sum")
    def all_reduce_sum():
        t0 = time.time()
        dist.all_reduce(state["tensor"], op=dist.ReduceOp.SUM)
        torch.cuda.synchronize()
        elapsed = time.time() - t0
        return {
            "status": "ok",
            "elapsed_s": float(elapsed),
            "rank": int(state["rank"]),
        }

    @register_task("get_tensor_info")
    def get_tensor_info():
        t = state["tensor"]
        cpu_t = t.cpu()
        return {
            "shape": [int(s) for s in t.shape],
            "dtype": str(t.dtype),
            "device": str(t.device),
            "mean": float(cpu_t.mean()),
            "min": float(cpu_t.min()),
            "max": float(cpu_t.max()),
            "first_10": [float(x) for x in cpu_t.flatten()[:10].tolist()],
            "rank": int(state["rank"]),
        }

    @register_task("verify_tensor")
    def verify_tensor(expected_value):
        t = state["tensor"]
        expected = torch.full_like(t, float(expected_value))
        match = bool(torch.allclose(t, expected, atol=1e-6))
        return {
            "pass": match,
            "expected": float(expected_value),
            "actual_mean": float(t.mean()),
            "actual_min": float(t.min()),
            "actual_max": float(t.max()),
            "rank": int(state["rank"]),
        }

    @register_task("cleanup_nccl")
    def cleanup_nccl():
        if state["nccl_initialized"]:
            dist.destroy_process_group()
            state["nccl_initialized"] = False
        return {"status": "ok", "rank": int(state["rank"])}

    def task_callback(task_name, args_list, num_returns):
        fn = TASK_FUNCTIONS.get(task_name)
        if fn is None:
            raise ValueError(f"Unknown task: {task_name}")
        deserialized = []
        for arg_bytes in args_list:
            if arg_bytes:
                deserialized.append(pickle.loads(bytes(arg_bytes)))
        result = fn(*deserialized)
        return pickle.dumps(result)

    worker.set_task_callback(task_callback)
    print(f"[gpu_worker rank={args.nccl_rank}] Ready. Functions: {list(TASK_FUNCTIONS.keys())}")
    print(f"[gpu_worker rank={args.nccl_rank}] CONNECT_STRING={node_ip}:{port}:{worker_id_hex}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[gpu_worker rank={args.nccl_rank}] Shutting down")


if __name__ == "__main__":
    main()
