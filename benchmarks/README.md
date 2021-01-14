# Ray Scalability Envelope

### Note: This document is a WIP. This is not a scalability guarantee (yet).

## Distributed Benchmarks

All distributed tests are run with the maximum number of nodes.

| Dimension | Quantity |
| --------- | -------- |
| # nodes in cluster (with trivial task workload) | N/A |


## Single Node Benchmarks.

All single node benchmarks are run on a single m4.16xlarge.

| Dimension | Quantity |
| --------- | -------- |
| # of objects returned from a single task | 10000 |
| # of plasma objects in a single `ray.get` call | 10000 |
| # of tasks queued on a single node | 1,000,000 |
| Maximum `ray.get` numpy object size | 100GiB |



    
