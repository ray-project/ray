# Ray Scalability Envelope

## Distributed Benchmarks

All distributed tests are run on 64 nodes with 64 cores/node. Maximum number of nodes is achieved by adding 4 core nodes.

| Dimension                                       | Quantity |
| ---------                                       | -------- |
| # nodes in cluster (with trivial task workload) | 250+     |
| # actors in cluster (with trivial workload)     | 10k+     |
| # simultaneously running tasks                  | 10k+     |
| # simultaneously running placement groups       | 1k+      |

## Object Store Benchmarks

| Dimension                           | Quantity |
| ---------                           | -------- |
| 1 GiB object broadcast (# of nodes) | 50+      |


## Single Node Benchmarks.

All single node benchmarks are run on a single m4.16xlarge.

| Dimension                                      | Quantity   |
| ---------                                      | --------   |
| # of object arguments to a single task         | 10000+     |
| # of objects returned from a single task       | 3000+     |
| # of plasma objects in a single `ray.get` call | 10000+     |
| # of tasks queued on a single node             | 1,000,000+ |
| Maximum `ray.get` numpy object size            | 100GiB+    |

    
