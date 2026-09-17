---
myst:
  html_meta:
    description: Diagnose high native thread counts in Ray worker processes and configure worker gRPC threads.
---

(debug-worker-thread-count)=

# Debugging high worker thread counts

Each Ray worker process has its own gRPC runtime. On nodes that run many worker
processes, the per-process gRPC threads can add up to a high node-level thread count.

## Inspect worker threads

On Linux, inspect the thread names for a representative worker process:

```bash
ps -L -p ${WORKER_PID} -o tid,comm

for task in /proc/${WORKER_PID}/task/*; do
  cat "${task}/comm"
done | sort | uniq -c | sort -nr
```

Compare the thread-name counts across representative workers and at the node level.

## Distinguish gRPC runtimes

If the application imports the Python `grpcio` package, the worker can load a separate
gRPC runtime that isn't controlled by `RAY_worker_num_grpc_internal_threads`. Inspect
the worker's mapped libraries to distinguish that runtime from Ray's bundled gRPC
runtime:

```bash
grep grpc /proc/${WORKER_PID}/maps
```

## Check process and thread limits

When sizing a node or container that runs many workers, monitor its process and thread
limits. On Linux, check the applicable cgroup `pids.max` and the user process limit:

```bash
cat /sys/fs/cgroup/pids.max
ulimit -u
```

If worker gRPC threads contribute to the high thread count, configure
`RAY_worker_num_grpc_internal_threads` as described in
{ref}`worker-grpc-thread-configuration`. Compare task throughput, RPC latency, and
thread counts before and after changing the value.
