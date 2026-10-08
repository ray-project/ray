---
myst:
  html_meta:
    description: "Tune operating system settings such as open file limits and the ARP cache to run Ray on more than 1,000 nodes, with benchmark results."
---

(core-large-clusters)=

# Run large Ray clusters

The following tips help you run Ray on more than 1,000 nodes. At that scale, you might need to tune several system settings so that the machines can communicate with each other.

For networking, head node, and autoscaler practices for large clusters, see {ref}`vms-large-cluster`.

## Tune operating system settings

All nodes and workers connect to the GCS, so the operating system (OS) has to support a large number of network connections.

### Maximum open files

Every worker and raylet connects to the GCS, so configure the OS to support opening many TCP connections. On POSIX systems, check the current limit with `ulimit -n`. If the limit is small, increase it as your OS manual describes.

### ARP cache

You also need to configure the Address Resolution Protocol (ARP) cache. In a large cluster, all the worker nodes connect to the head node, which adds many entries to the ARP table. Make sure the ARP cache is large enough to handle that many nodes. Otherwise, the head node hangs, and `dmesg` shows errors such as `neighbor table overflow message`.

On Ubuntu, tune the ARP cache size in `/etc/sysctl.conf` by increasing the values of `net.ipv4.neigh.default.gc_thresh1` through `net.ipv4.neigh.default.gc_thresh3`. For more details, see your OS manual.

## Benchmark

The benchmark uses the following machines:

- One head node: m5.4xlarge, with 16 vCPUs and 64 GB of memory
- 2,000 worker nodes: m5.large, with 2 vCPUs and 8 GB of memory

The benchmark uses the following OS settings:

- Set the maximum number of open files to 1048576.
- Increase the ARP cache size:
    - `net.ipv4.neigh.default.gc_thresh1=2048`
    - `net.ipv4.neigh.default.gc_thresh2=4096`
    - `net.ipv4.neigh.default.gc_thresh3=8192`


The benchmark uses the following Ray setting:

- `RAY_event_stats=false`

The test workload runs the following script:

- [`actor_test.py`](https://github.com/ray-project/ray/blob/master/release/benchmarks/distributed/many_nodes_tests/actor_test.py)



```{list-table} Benchmark result
:header-rows: 1

* - Number of actors
  - Actor launch time
  - Actor ready time
  - Total time
* - 20k, at 10 actors per node
  - 14.5s
  - 136.1s
  - 150.7s
```
