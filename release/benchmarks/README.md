# Ray Scalability Envelope

## Distributed Benchmarks

| Dimension                                                    | Quantity |
| ---------                                                    | -------- |
| # nodes in cluster (with trivial task workload)<sup>+</sup>  | 3k+      |
| # actors in cluster (with trivial workload)<sup>+</sup>      | 20k+     |
| # simultaneously running tasks<sup>++</sup>                  | 15k+     |
| # simultaneously running placement groups<sup>++</sup>       | 1k+      |

<sup>+</sup> Tests run on m5.16xlarge for head nodes and m5.large for worker nodes

<sup>++</sup> Tests run on 64 nodes with 64 cores/node. Maximum number of nodes is achieved by adding 4 core nodes.


## Object Store Benchmarks

| Dimension                           | Quantity |
| ---------                           | -------- |
| 1 GiB object broadcast (# of nodes) | 50+      |


## Single Node Benchmarks.

All single node benchmarks are run on a single m4.16xlarge.

| Dimension                                      | Quantity   |
| ---------                                      | --------   |
| # of object arguments to a single task         | 10000+     |
| # of objects returned from a single task       | 3000+      |
| # of plasma objects in a single `ray.get` call | 10000+     |
| # of tasks queued on a single node             | 1,000,000+ |
| Maximum `ray.get` numpy object size            | 100GiB+    |
