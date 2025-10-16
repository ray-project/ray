# Serving patterns

Architecture documentation for distributed LLM serving patterns.

```{toctree}
:maxdepth: 1

Data parallel <data-parallel>
Prefill-decode disaggregation <prefill-decode>
DeepSeek-V3 pattern <deepseek-v3>
```

## Overview

Ray Serve LLM supports several serving patterns that can be combined for complex deployment scenarios:

- **Data parallel**: Scale throughput by running multiple independent engine instances.
- **Prefill-decode disaggregation**: Optimize resource utilization by separating prompt processing from token generation.
- **DeepSeek-V3 pattern**: Combine data parallelism, expert parallelism, and prefill-decode for maximum efficiency.

These patterns are composable and can be mixed to meet specific requirements for throughput, latency, and cost optimization.

