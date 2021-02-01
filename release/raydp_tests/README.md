# RayDP Shuffle Benchmarks

## Run locally

The following will run a 1 MB, 100 repartitioning shuffle locally.

```
python raydp_benchmark.py --repartition
```

## Run on a single-node cluster

### Configure the cluster

Set `min_workers = max_workers = 0` in the `raydp_cluster.yaml` manifest so only the Ray head node will be started.

### Deploy the cluster

Run

```
ray up raydp_cluster.yaml
```

Run `ray exec raydp_cluster.yaml 'tail -n 100 -f /tmp/ray/session_latest/logs/monitor*'` and wait until the head node is healthy and ready.

### Run the benchmark

The following will run a 10 GB, 200 repartitioning shuffle on your one-node Ray cluster, using the 4 NVMe instance volumes as Spark's scratch space.

```
ray submit raydp_cluster.yaml raydp_benchmark.py -- --cluster --repartition --num-nodes=1 --spark-local-dir=/mnt/disk0,/mnt/disk1,/mnt/disk2,/mnt/disk3 --nbytes=$(( 10 ** 10 )) --npartitions=200
```

## Run on a multi-node cluster

### Configure the cluster

Set `min_workers` and `max_workers` to the number of worker nodes `n` that you want in the `raydp_cluster.yaml` manifest. The total number of Ray nodes will be `n + 1`.

### Deploy the cluster

Run

```
ray up raydp_cluster.yaml
```

Run `ray exec raydp_cluster.yaml 'tail -n 100 -f /tmp/ray/session_latest/logs/monitor*'` and wait until the head node and the `n` worker nodes are all healthy and ready.

### Run the benchmark

The following will run a 100 GB, 200 repartitioning shuffle on your `n`-node Ray cluster, using the 4 NVMe instance volumes on each node as Spark's scratch space. `--s3` will cause the random source data to be written to and read from S3, as well as the output Parquet files.

```
ray submit raydp_cluster.yaml raydp_benchmark.py -- --cluster --repartition --s3 --num-nodes=1 --spark-local-dir=/mnt/disk0,/mnt/disk1,/mnt/disk2,/mnt/disk3 --nbytes=$(( 10 ** 11 )) --npartitions=200
```
