---
myst:
  html_meta:
    description: "Pattern: pipeline task submission and result processing so compute overlaps with data transfer and throughput rises."
---

# Pattern: Using pipelining to increase throughput

If you have multiple work items that each take several steps to complete, use the [pipelining](https://en.wikipedia.org/wiki/Pipeline_(computing)) technique to improve cluster utilization and increase your system's throughput.

:::{note}
Pipelining is an important technique for improving performance, and Ray libraries use it heavily. For an example, see {ref}`Ray Data <data>`.
:::

```{figure} ../images/pipelining.svg
```

## Example use case

A component of your application needs to both do compute-intensive work and communicate with other processes. Ideally, you overlap computation and communication to saturate the CPU and increase overall throughput.

## Code example

```{literalinclude} ../doc_code/pattern_pipelining.py
```

In the preceding example, a worker actor pulls work off a queue and then does some computation on it. Without pipelining, you call {func}`ray.get() <ray.get>` immediately after requesting a work item, so the actor blocks while that RPC is in flight and the CPU sits idle. With pipelining, you request the next work item before processing the current one. The actor can then use the CPU while the RPC is in flight, which increases CPU utilization.
