# Benchmarks

Performance in LLM serving depends heavily on your specific workload characteristics and hardware stack. From a Ray Serve perspective, the focus is on orchestration overhead and the effectiveness of serving pattern implementations. The Ray team maintains the [ray-serve-llm-perf-examples](https://github.com/anyscale/ray-serve-llm-perf-examples) repository with benchmarking snapshots, tooling, and lessons learned. These benchmarks validate the correctness and effectiveness of different serving patterns. You can use them to validate your production stack more systematically.

## What to measure

When you benchmark a deployment, track the metrics that map to your service objectives:

- **Time to first token (TTFT)**: latency from request arrival to the first streamed token. Dominated by queueing and the prefill phase.
- **Time per output token (TPOT)**: average latency per generated token during decode. Determines perceived streaming speed.
- **Throughput**: tokens per second and requests per second the deployment sustains. Driven by batching, parallelism, and replica count.
- **Replica startup latency**: time for a new replica to become ready. Determines how quickly autoscaling responds to load. See below.

Ray Serve LLM exposes TTFT, TPOT, and throughput as built-in metrics. See {doc}`Observability and monitoring <user-guides/observability>` to collect them, and the serving-pattern guides ({doc}`prefill/decode <user-guides/prefill-decode>`, {doc}`data parallel attention <user-guides/data-parallel-attention>`) for the levers that move them.

## Replica startup latency

Replica startup times involving large models can be slow, leading to slow autoscaling and poor response to changing workloads. Experiments on replica startup can be found [here](https://github.com/anyscale/ray-serve-llm-perf-examples/tree/master/replica_initialization). The experiments illustrate the effects of the various techniques described in {doc}`Deployment initialization <user-guides/deployment-initialization>`, primarily targeting the latency cost of model loading and Torch Compile. As models grow larger, the effects of these optimizations become increasingly pronounced. As an example, we get nearly 3.88x reduction in latency on `Qwen/Qwen3-235B-A22B`.
