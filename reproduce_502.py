"""
Ray Client / Anyscale Connect 502 Reproducer

Usage:
    See test-repro.sh for usage.

Expected behavior with conda-forge ray:
    Repeated "Received http2 header with status: 502" errors
    Eventually: "ConnectionError: ray client connection timeout"

Expected behavior with pip-installed ray:
    Connects successfully and runs tasks.
"""
import ray
import os
import time
import sys
import logging

@ray.remote
def foo():
    time.sleep(5)

print(f"Ray version: {ray.__version__}")
print(f"Python version: {sys.version}")

try:
    import grpc
    print(f"grpcio version: {grpc.__version__}")
except ImportError:
    print("grpcio not found")

try:
    from google.protobuf import __version__ as pb_version
    print(f"protobuf version: {pb_version}")
except ImportError:
    print("protobuf not found")

print("\nAttempting ray.init()...")
init_kwargs = {"logging_level": logging.DEBUG}
cloud = os.environ.get("ANYSCALE_CLOUD")
if cloud:
    init_kwargs["cloud"] = cloud
ray.init(**init_kwargs)  # Uses RAY_ADDRESS env var

print("Connected! Submitting tasks...")
tasks = []
for _ in range(100):
    tasks.append(foo.remote())

waits = 1
while tasks:
    finished_tasks, tasks = ray.wait(tasks, num_returns=1)
    print(f"{len(tasks)} remaining tasks after wait {waits}")
    waits += 1

print("All tasks completed.")
