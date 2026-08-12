# Serving patterns

Architecture documentation for distributed LLM serving patterns.

```{toctree}
:hidden:
:maxdepth: 1

Data parallel attention <data-parallel>
Prefill-decode disaggregation <prefill-decode>
```

## Overview

Ray Serve LLM supports several serving patterns that can be combined for complex deployment scenarios:

- {doc}`Data parallel attention <data-parallel>`: scale throughput by running multiple coordinated engine replicas that process requests in parallel, replicating attention while sharding requests across the replicas.
- {doc}`Prefill-decode disaggregation <prefill-decode>`: optimize resource utilization by separating prompt processing from token generation.

These patterns are composable and can be mixed to meet specific requirements for throughput, latency, and cost optimization.

These pages describe how each pattern works. For step-by-step configuration, see the matching how-to guides: {doc}`Data parallel attention <../../user-guides/data-parallel-attention>` and {doc}`Prefill/decode disaggregation <../../user-guides/prefill-decode>`.

