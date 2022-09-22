(serve-e2e-ft)=
# Adding End-to-End Fault Tolerance

Serve handles component failures _within_ a Ray cluster by default. You can configure your Ray cluster and Serve application to guard them against cluster-level failures as well.

This section helps you:

* provide additional fault tolerance for your Serve application
* understand Serve's failure conditions and how it recovers from them
* inspect your Serve application to detect system errors

:::{admonition} Relevant Guides
:class: seealso
This section discusses concepts from:
* the Serve [architecture guide](serve-architecture)
* Serve's [Kubernetes production guide](serve-in-production-kubernetes)
:::

(serve-e2e-ft-guide)=
## Guide: providing end-to-end fault tolerance for your Serve app

### Replica health-checking

By default, the Serve controller periodically health-checks each Serve deployment replica and restarts it on failure.

You can define custom application-level health-checks and adjust the check frequency and timeout.
To define a custom health-check, define a `check_health` method on your deployment class.
This method should take no arguments and return no result, and it should raise an exception if the replica should be considered unhealthy.
The Serve controller logs the raised exception if the health-check fails.

You can also use the deployment options to customize how frequently the health-check is run and the timeout after which a replica is marked unhealthy in the deployment options.

```{literalinclude} doc_code/fault_tolerance/replica_health_check.py
:start-after: __health_check_start__
:end-before: __health_check_end__
:language: python
```

### Ray cluster recovery

:::{admonition} KubeRay Required
:class: caution, dropdown
You **must** deploy your Serve application with [KubeRay] to leverage this feature.

See Serve's [Kubernetes production guide](serve-in-production-kubernetes) to learn how you can deploy your app with KubeRay.
:::

Serve can recover from certain failures _within_ a Ray cluster (e.g. unhealthy replicas, failed HTTPProxies, failed controller, etc.). However, Serve on its own cannot recover from _cluster-level_ failures (e.g. lost worker nodes, dead head node, etc.). 

### Ray Global Control Store (GCS) fault tolerance

:::{admonition} KubeRay Required
:class: caution, dropdown
You **must** deploy your Serve application with [KubeRay] to leverage this feature.

See Serve's [Kubernetes production guide](serve-in-production-kubernetes) to learn how you can deploy your app with KubeRay.
:::

(serve-e2e-ft-behavior)=
## Serve's fault tolerance behavior

[KubeRay]: https://ray-project.github.io/kuberay/