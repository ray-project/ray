---
myst:
  html_meta:
    description: "Reference for ray.autoscaler.sdk.request_resources, which programmatically requests cluster capacity."
---

(ref-autoscaler-sdk)=

# Programmatic Cluster Scaling

(ref-autoscaler-sdk-request-resources)=

## ray.autoscaler.sdk.request_resources

Within a Ray program, you can command the autoscaler to scale the cluster up to a desired size with `request_resources()` call. The cluster will immediately attempt to scale to accommodate the requested resources, bypassing normal upscaling speed constraints.

```{eval-rst}
.. autofunction:: ray.autoscaler.sdk.request_resources
```
