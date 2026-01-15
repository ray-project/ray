# Serving patterns

Architecture documentation for distributed LLM serving patterns.

```{toctree}
:maxdepth: 1

Data parallel attention <data-parallel>
Prefill-decode disaggregation <prefill-decode>
```

## Overview

Ray Serve LLM supports several serving patterns that can be combined for complex deployment scenarios:

- **Data parallel attention**: Scale throughput by running multiple coordinated engine instances that shard requests across attention layers.
- **Prefill-decode disaggregation**: Optimize resource utilization by separating prompt processing from token generation.

These patterns are composable and can be mixed to meet specific requirements for throughput, latency, and cost optimization.

