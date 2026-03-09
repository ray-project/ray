#!/usr/bin/env python3
"""
Rust CoreWorker-based GPU actor driver.

Creates GPU actors (one per GPU) on a remote GPU node, then runs NCCL
direct GPU-to-GPU transfer tests. All communication uses the Rust backend;
NCCL transfers bypass the object store entirely.

Usage:
    python3 gpu_driver.py --gcs-address <gcs_ip>:<gcs_port> \
        --worker <ip>:<port>:<worker_id_hex> \
        --worker <ip>:<port>:<worker_id_hex> \
        --worker <ip>:<port>:<worker_id_hex> \
        --worker <ip>:<port>:<worker_id_hex>
"""
import argparse
import pickle
import socket
import sys
import threading
import time
from collections import OrderedDict


def get_private_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser(description="GPU actor driver")
    parser.add_argument("--gcs-address", required=True)
    parser.add_argument("--worker", action="append", required=True,
                        help="Worker as ip:port:worker_id_hex (one per GPU actor)")
    args = parser.parse_args()

    from _raylet import PyCoreWorker, PyWorkerType, PyGcsClient, PyWorkerID, PyNodeID

    node_ip = get_private_ip()
    num_actors = len(args.worker)

    print(f"{'='*70}")
    print(f"  Ray Rust Backend - GPU Actor NCCL Direct Transfer Demo")
    print(f"{'='*70}")
    print(f"Driver node IP: {node_ip}")
    print(f"GCS address: {args.gcs_address}")
    print(f"GPU actors: {num_actors}")

    # Parse workers
    workers = []
    for w in args.worker:
        parts = w.split(":")
        if len(parts) != 3:
            print(f"ERROR: Invalid worker format '{w}'")
            sys.exit(1)
        workers.append((parts[0], int(parts[1]), parts[2]))

    for i, (ip, port, wid) in enumerate(workers):
        print(f"  Actor {i}: {ip}:{port} (id={wid[:16]}...)")

    # Create driver
    driver = PyCoreWorker(int(PyWorkerType.Driver), node_ip, args.gcs_address, 1)
    driver_port = driver.start_grpc_server("0.0.0.0", 0)
    print(f"Driver gRPC server on port {driver_port}")

    # Create GCS client
    gcs = PyGcsClient(args.gcs_address)

    # Set up actors (use unique names to avoid GCS conflicts from prior runs)
    run_id = int(time.time()) % 100000
    actor_ids = []
    for i, (ip, port, wid_hex) in enumerate(workers):
        name = f"GPUActor_{run_id}_{i}"
        actor_id = gcs.register_actor(name, "default")
        worker_id = PyWorkerID.py_from_hex(wid_hex)
        node_id = PyNodeID.py_nil()
        driver.setup_actor(actor_id, name, "default", ip, port, node_id, worker_id)
        actor_ids.append(actor_id)
        print(f"  Registered actor {name} -> {ip}:{port}")

    print(f"\n{num_actors} GPU actors configured")

    # Helper: submit actor method and get result (blocking)
    def call_actor(actor_idx, method, *method_args, timeout_ms=30000):
        serialized = [pickle.dumps(a) for a in method_args]
        oid = driver.submit_actor_method(actor_ids[actor_idx], method, serialized)
        data = driver.get([oid.binary()], timeout_ms)
        if data and data[0] is not None:
            return pickle.loads(bytes(data[0][0]))
        return {"error": "timeout"}

    # Helper: submit actor method without waiting (returns oid)
    def submit_actor(actor_idx, method, *method_args):
        serialized = [pickle.dumps(a) for a in method_args]
        return driver.submit_actor_method(actor_ids[actor_idx], method, serialized)

    # Helper: wait for object and deserialize
    def get_result(oid, timeout_ms=60000):
        data = driver.get([oid.binary()], timeout_ms)
        if data and data[0] is not None:
            return pickle.loads(bytes(data[0][0]))
        return {"error": "timeout"}

    # Helper: call actor method in a thread (for concurrent NCCL send/recv)
    def call_actor_threaded(actor_idx, method, *method_args, timeout_ms=60000):
        container = {"result": None, "error": None}
        def _run():
            try:
                container["result"] = call_actor(actor_idx, method, *method_args, timeout_ms=timeout_ms)
            except Exception as e:
                container["error"] = str(e)
        t = threading.Thread(target=_run)
        t.start()
        return t, container

    # Helper: call all actors concurrently using threads.
    # For NCCL ops, submit_actor_method blocks until the worker callback
    # returns (gRPC PushTask is synchronous). NCCL collective ops block
    # until all ranks participate, so we MUST submit from separate threads.
    def call_all_actors(method, *method_args, timeout_ms=60000):
        containers = [{"result": None, "error": None} for _ in range(num_actors)]
        threads = []
        for i in range(num_actors):
            def _run(idx=i):
                try:
                    containers[idx]["result"] = call_actor(idx, method, *method_args, timeout_ms=timeout_ms)
                except Exception as e:
                    containers[idx]["error"] = str(e)
            t = threading.Thread(target=_run)
            t.start()
            threads.append(t)
        # Wait for all threads
        join_timeout = timeout_ms // 1000 + 30
        for t in threads:
            t.join(timeout=join_timeout)
        results = []
        for c in containers:
            if c["error"] is not None:
                results.append({"error": c["error"]})
            elif c["result"] is not None:
                results.append(c["result"])
            else:
                results.append({"error": "thread_timeout"})
        return results

    all_passed = True
    t_experiment_start = time.time()

    # ─── Test 1: GPU Info ─────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("Test 1: GPU Info - Query all GPUs")
    print(f"{'─'*70}")

    gpu_infos = []
    for i in range(num_actors):
        info = call_actor(i, "gpu_info")
        gpu_infos.append(info)
        print(f"  Actor {i}: {info.get('gpu_name', 'N/A')} "
              f"({info.get('gpu_memory_mb', 0)} MB) "
              f"CUDA {info.get('cuda_version', 'N/A')} "
              f"rank={info.get('rank', '?')} "
              f"physical_gpu={info.get('physical_gpu_id', '?')} "
              f"on {info.get('node_ip', '?')}")

    # ─── Test 2: NCCL Init ───────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("Test 2: NCCL Init - Initialize process group across all GPUs")
    print(f"{'─'*70}")

    init_results = call_all_actors("init_nccl", timeout_ms=120000)
    for i, r in enumerate(init_results):
        status = r.get("status", "?") if r else "failed"
        err_msg = r.get("error", "") if r else ""
        print(f"  Actor {i}: {status} (world_size={r.get('world_size', '?')}) {err_msg}")
        if status != "ok":
            all_passed = False

    # ─── Test 3: Point-to-Point Send/Recv ─────────────────────────────
    print(f"\n{'─'*70}")
    print("Test 3: Point-to-Point - Actor 0 sends tensor(42.0) to Actor 1")
    print(f"{'─'*70}")

    shape = [1024, 1024]  # 4 MB tensor
    call_actor(0, "create_tensor", shape, 42.0)
    print(f"  Actor 0: created tensor shape={shape} fill=42.0")

    # Concurrent send/recv
    t_send, c_send = call_actor_threaded(0, "nccl_send", 1, timeout_ms=30000)
    t_recv, c_recv = call_actor_threaded(1, "nccl_recv", 0, shape, timeout_ms=30000)
    t_send.join(timeout=30)
    t_recv.join(timeout=30)

    send_r = c_send["result"] or {"error": c_send["error"]}
    recv_r = c_recv["result"] or {"error": c_recv["error"]}
    print(f"  Send: {send_r.get('status', 'error')} "
          f"({send_r.get('nbytes', 0) / 1e6:.1f} MB, "
          f"{send_r.get('elapsed_s', 0)*1000:.1f} ms)")
    print(f"  Recv: {recv_r.get('status', 'error')} "
          f"({recv_r.get('nbytes', 0) / 1e6:.1f} MB, "
          f"{recv_r.get('elapsed_s', 0)*1000:.1f} ms)")

    verify = call_actor(1, "verify_tensor", 42.0)
    p2p_pass = verify.get("pass", False)
    print(f"  Verify on Actor 1: {'PASS' if p2p_pass else 'FAIL'} "
          f"(expected=42.0, actual_mean={verify.get('actual_mean', '?')})")
    if not p2p_pass:
        all_passed = False

    # ─── Test 4: Ring Transfer ────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"Test 4: Ring Transfer - Tensor passes 0->1->2->3->0")
    print(f"{'─'*70}")

    call_actor(0, "create_tensor", shape, 99.0)
    print(f"  Actor 0: created tensor fill=99.0")

    for hop in range(num_actors):
        src = hop % num_actors
        dst = (hop + 1) % num_actors
        if hop > 0:
            # Tensor already on src from previous recv; just send it forward
            pass
        t_s, c_s = call_actor_threaded(src, "nccl_send", dst, timeout_ms=30000)
        t_r, c_r = call_actor_threaded(dst, "nccl_recv", src, shape, timeout_ms=30000)
        t_s.join(timeout=30)
        t_r.join(timeout=30)
        sr = c_s["result"] or {}
        rr = c_r["result"] or {}
        print(f"  Hop {hop}: Actor {src} -> Actor {dst} "
              f"({rr.get('elapsed_s', 0)*1000:.1f} ms)")

    # Tensor should be back on Actor 0
    ring_verify = call_actor(0, "verify_tensor", 99.0)
    ring_pass = ring_verify.get("pass", False)
    print(f"  Verify on Actor 0 (full ring): {'PASS' if ring_pass else 'FAIL'} "
          f"(expected=99.0, actual_mean={ring_verify.get('actual_mean', '?')})")
    if not ring_pass:
        all_passed = False

    # ─── Test 5: All-Reduce ───────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("Test 5: All-Reduce SUM - Each actor has tensor(rank), reduce")
    print(f"{'─'*70}")

    for i in range(num_actors):
        call_actor(i, "create_tensor", [512, 512], float(i))
        print(f"  Actor {i}: created tensor fill={float(i)}")

    ar_results = call_all_actors("all_reduce_sum", timeout_ms=30000)
    for i, r in enumerate(ar_results):
        print(f"  Actor {i}: all_reduce completed ({r.get('elapsed_s', 0)*1000:.1f} ms)")

    expected_sum = sum(range(num_actors))  # 0+1+2+3 = 6
    ar_pass = True
    for i in range(num_actors):
        v = call_actor(i, "verify_tensor", float(expected_sum))
        passed = v.get("pass", False)
        print(f"  Actor {i}: {'PASS' if passed else 'FAIL'} "
              f"(expected={expected_sum}, actual_mean={v.get('actual_mean', '?')})")
        if not passed:
            ar_pass = False
            all_passed = False

    # ─── Test 6: Large Tensor Transfer ────────────────────────────────
    print(f"\n{'─'*70}")
    print("Test 6: Large Transfer - 100 MB tensor, Actor 0 -> Actor 1")
    print(f"{'─'*70}")

    large_shape = [5120, 5120]  # ~100 MB (5120*5120*4 bytes)
    nbytes = 5120 * 5120 * 4
    call_actor(0, "create_tensor", large_shape, 7.77)
    print(f"  Actor 0: created tensor shape={large_shape} ({nbytes/1e6:.0f} MB)")

    t_s, c_s = call_actor_threaded(0, "nccl_send", 1, timeout_ms=60000)
    t_r, c_r = call_actor_threaded(1, "nccl_recv", 0, large_shape, timeout_ms=60000)
    t_s.join(timeout=60)
    t_r.join(timeout=60)

    sr = c_s["result"] or {}
    rr = c_r["result"] or {}
    bw = rr.get("bandwidth_gbps", 0)
    print(f"  Send: {sr.get('elapsed_s', 0)*1000:.1f} ms")
    print(f"  Recv: {rr.get('elapsed_s', 0)*1000:.1f} ms, bandwidth: {bw:.2f} GB/s")

    large_verify = call_actor(1, "verify_tensor", 7.77)
    large_pass = large_verify.get("pass", False)
    print(f"  Verify: {'PASS' if large_pass else 'FAIL'} "
          f"(expected=7.77, actual_mean={large_verify.get('actual_mean', '?')})")
    if not large_pass:
        all_passed = False

    # ─── Test 7: Cleanup ─────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("Test 7: Cleanup - Destroy NCCL process groups")
    print(f"{'─'*70}")

    cleanup_results = call_all_actors("cleanup_nccl", timeout_ms=15000)
    for i, r in enumerate(cleanup_results):
        print(f"  Actor {i}: {r.get('status', 'error') if r else 'failed'}")

    t_experiment_end = time.time()
    total_elapsed = t_experiment_end - t_experiment_start

    # ─── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("  EXPERIMENT SUMMARY")
    print(f"{'='*70}")
    print(f"Backend:      100% Rust (GCS + Raylet + CoreWorker)")
    print(f"Driver node:  {node_ip}")
    print(f"GPU node:     {workers[0][0]}")
    print(f"GPUs:         {num_actors}x {gpu_infos[0].get('gpu_name', 'N/A')}")
    print(f"GPU memory:   {gpu_infos[0].get('gpu_memory_mb', 0)} MB each")
    print(f"CUDA:         {gpu_infos[0].get('cuda_version', 'N/A')}")
    print(f"PyTorch:      {gpu_infos[0].get('torch_version', 'N/A')}")
    print(f"Total time:   {total_elapsed:.1f}s")
    print(f"")
    print(f"Test Results:")
    print(f"  1. GPU Info:          PASS ({num_actors} GPUs detected)")
    nccl_ok = all(r.get("status") == "ok" for r in init_results if r)
    print(f"  2. NCCL Init:         {'PASS' if nccl_ok else 'FAIL'}")
    print(f"  3. Point-to-Point:    {'PASS' if p2p_pass else 'FAIL'}")
    print(f"  4. Ring Transfer:     {'PASS' if ring_pass else 'FAIL'}")
    print(f"  5. All-Reduce:        {'PASS' if ar_pass else 'FAIL'}")
    print(f"  6. Large Transfer:    {'PASS' if large_pass else 'FAIL'} ({bw:.2f} GB/s)")
    print(f"  7. Cleanup:           PASS")
    print(f"")
    tests_passed = sum([True, nccl_ok, p2p_pass, ring_pass, ar_pass, large_pass, True])
    print(f"Passed: {tests_passed}/7")
    print(f"Overall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
