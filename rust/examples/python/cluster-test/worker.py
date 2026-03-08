#!/usr/bin/env python3
"""
Rust CoreWorker-based task worker.

Starts a Rust CoreWorker in worker mode, registers a Python task callback,
and listens for PushTask RPCs from the driver. Returns task results including
the node's private IP address to demonstrate multi-node execution.

Usage:
    python3 worker.py --gcs-address <gcs_ip>:<gcs_port>
"""
import argparse
import os
import pickle
import socket
import time
import sys


def get_private_ip():
    """Get the node's private IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser(description="Rust CoreWorker task worker")
    parser.add_argument("--gcs-address", required=True, help="GCS server address (ip:port)")
    parser.add_argument("--bind-ip", default="0.0.0.0", help="IP to bind gRPC server")
    parser.add_argument("--bind-port", type=int, default=0, help="Port to bind gRPC server (0=random)")
    args = parser.parse_args()

    from _raylet import PyCoreWorker, PyWorkerType

    node_ip = get_private_ip()
    print(f"[worker] Node IP: {node_ip}")
    print(f"[worker] GCS address: {args.gcs_address}")

    # Create CoreWorker in worker mode
    # Constructor: (worker_type: int, node_ip_address: str, gcs_address: str, job_id_int: int)
    worker = PyCoreWorker(
        int(PyWorkerType.Worker),
        node_ip,
        args.gcs_address,
        1,  # job_id
    )
    worker_id_obj = worker.worker_id()
    worker_id_hex = worker_id_obj.hex()
    print(f"[worker] Worker ID: {worker_id_hex}")

    # Start gRPC server to accept PushTask RPCs
    port = worker.start_grpc_server(args.bind_ip, args.bind_port)
    print(f"[worker] gRPC server listening on {args.bind_ip}:{port}")

    # Define task functions
    TASK_FUNCTIONS = {}

    def register_task(name):
        def decorator(fn):
            TASK_FUNCTIONS[name] = fn
            return fn
        return decorator

    @register_task("get_node_ip")
    def get_node_ip_task():
        return get_private_ip()

    @register_task("compute_square")
    def compute_square(x):
        return {"result": x * x, "node_ip": get_private_ip()}

    @register_task("sum_range")
    def sum_range(n):
        total = sum(range(n))
        return {"sum": total, "node_ip": get_private_ip()}

    @register_task("process_text")
    def process_text(text):
        return {
            "original": text,
            "upper": text.upper(),
            "length": len(text),
            "node_ip": get_private_ip(),
        }

    @register_task("coordinator")
    def coordinator(data_list):
        return {
            "count": len(data_list),
            "items": data_list,
            "node_ip": get_private_ip(),
        }

    # Set task callback
    def task_callback(task_name, args_list, num_returns):
        """Execute a task by name with the given arguments.

        Args:
            task_name: function name string
            args_list: list of bytes objects (serialized arguments)
            num_returns: number of return values expected
        Returns:
            bytes (serialized result)
        """
        fn = TASK_FUNCTIONS.get(task_name)
        if fn is None:
            raise ValueError(f"Unknown task function: {task_name}")
        # Deserialize arguments
        deserialized_args = []
        for arg_bytes in args_list:
            if arg_bytes:
                deserialized_args.append(pickle.loads(bytes(arg_bytes)))
        result = fn(*deserialized_args)
        # Serialize result
        return pickle.dumps(result)

    worker.set_task_callback(task_callback)
    print(f"[worker] Ready to accept tasks. Registered functions: {list(TASK_FUNCTIONS.keys())}")
    print(f"[worker] Worker info: ip={node_ip} port={port} id={worker_id_hex}")
    # Print the connection string for the driver
    print(f"[worker] CONNECT_STRING={node_ip}:{port}:{worker_id_hex}")

    # Block waiting for tasks
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[worker] Shutting down")


if __name__ == "__main__":
    main()
