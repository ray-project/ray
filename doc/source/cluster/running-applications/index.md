---
myst:
  html_meta:
    description: "Run Ray applications on a cluster using the Ray Jobs API and CLI, and reference autoscaling configuration for production workloads."
---

# Application guide

This section introduces the main differences in running a Ray application on your laptop vs on a Ray Cluster. To get started, check out the [job submissions](jobs-quickstart) page.

To develop and deploy a Ray Serve application, see the Ray Serve guides. {ref}`serve-dev-workflow` covers the local development loop. {ref}`serve-in-production-deploying` covers VM clusters that the cluster launcher started, and {ref}`serve-in-production-kubernetes` covers Kubernetes with KubeRay.

```{toctree}
:maxdepth: '2'

job-submission/index
../../core/api/autoscaler
```
