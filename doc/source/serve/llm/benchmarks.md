# Benchmarks

Performance in LLM serving depends heavily on your specific workload characteristics and hardware stack. From a Ray Serve perspective, the focus is on orchestration overhead and the effectiveness of serving pattern implementations. The Ray team maintains the [ray-serve-llm-perf-examples](https://github.com/anyscale/ray-serve-llm-perf-examples) repository with benchmarking snapshots, tooling, and lessons learned. These benchmarks validate the correctness and effectiveness of different serving patterns. You can use these benchmarks to validate your production stack more systematically. 

## Replica Startup Latency

Replica startup times involving large models can be slow, leading to slow autoscaling and poor response to changing workloads. Experiments on replica startup can be found [here](https://github.com/anyscale/ray-serve-llm-perf-examples/tree/master/replica_initialization). The experiments illustrate the effects of the various techniques mentioned in [this guide](./user-guides/deployment-initialization.md), primarily targeting the latency cost of model loading and Torch Compile. As models grow larger, the effects of these optimizations become increasingly pronounced. As an example, we get nearly 3.88x reduction in latency on `Qwen/Qwen3-235B-A22B`.
