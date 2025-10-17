(serve-llm-architecture-deepseek-v3)=
# DeepSeek-V3 pattern

:::{note}
This document is under development. The DeepSeek-V3 serving pattern combines data parallelism, expert parallelism, and prefill-decode disaggregation for efficient serving of large Mixture-of-Experts (MoE) models.
:::

The DeepSeek-V3 pattern demonstrates how to compose Ray Serve LLM's serving patterns for complex deployment scenarios.

## Pattern overview

DeepSeek-V3 style deployments require:
- **Pipeline parallelism**: Distribute model layers across multiple nodes.
- **Expert parallelism**: Distribute MoE experts across replicas.
- **Data parallelism**: Scale throughput with multiple inference instances.
- **Prefill-decode disaggregation**: Optimize resource utilization for each phase.

## Architecture

(Architecture diagram and details to be added)

## See also

- {doc}`../overview` - High-level architecture overview
- {doc}`data-parallel` - Data parallelism architecture
- {doc}`prefill-decode` - Prefill-decode disaggregation architecture

