Ray Compiled Graph User Guide
=============================

Ray 2.32 ships the Alpha (Developer Preview) for a new Ray Core feature called Ray accelerated DAGs (aDAGs). aDAGs give you a classic Ray Core-like API but with:
Much lower system overhead for workloads that repeatedly execute the same task DAG
Native support for GPU-GPU communication, via NCCL

The performance benefit comes from allocating resources for a task once and then reusing them for each future execution. Native support for GPU-GPU communication is made possible by declaring the DAG ahead of execution. Finally, the aDAG API is “Ray Core-like” but with some restrictions, which we’ll discuss below.

Ray Core already provides a DAG API for lazy execution, but normally the way that it executes is still with the classic Ray Core backend. In contrast, aDAGs provide an entirely different backend for the same DAG API. It is targeted towards developers who:
Are looking for optimization opportunities across multiple GPUs, or GPUs+CPUs.
Have stringent performance requirements, e.g., millisecond/microsecond-level response time.
Can express their application with static control flow.

For example, we are currently integrating Ray aDAGs as a backend for the LLM inference engine, vLLM. LLM inference is a good fit because:
For large models that can’t fit in one GPU, vLLM coordinates across multiple GPUs, each holding one model shard.
Inter-token latency can be as low as 10ms, meaning that even the 1ms of overhead imposed by vanilla Ray Core is significant.
Within a request batch, the control flow is static. That is, each execution request executes across GPUs in exactly the same way.

aDAGs are probably not a good fit if:
Your application can be decomposed to tasks that are >=100ms. In this case, classic Ray Core’s execution overheads are negligible, and the additional limitations imposed by the aDAG API may be burdensome.
You want to make lots of dynamic control flow decisions, e.g., load-balancing, autoscaling, etc.

If your application meets either of these properties, you should probably just use the classic Ray Core API.