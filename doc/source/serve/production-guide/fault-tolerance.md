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

Serve provides some [fault tolerance](serve-ft-detail) features out of the box. You can provide end-to-end fault tolerance by tuning these features and running Serve on top of [KubeRay].

### Replica health-checking

By default, the Serve controller periodically health-checks each Serve deployment replica and restarts it on failure.

You can define custom application-level health-checks and adjust their frequency and timeout.
To define a custom health-check, add a `check_health` method to your deployment class.
This method should take no arguments and return no result, and it should raise an exception if the replica should be considered unhealthy.
The Serve controller logs the raised exception if the health-check fails.
You can also use the deployment options to customize how frequently the health-check is run and the timeout after which a replica is marked unhealthy.

```{literalinclude} doc_code/fault_tolerance/replica_health_check.py
:start-after: __health_check_start__
:end-before: __health_check_end__
:language: python
```

### Worker node recovery

:::{admonition} KubeRay Required
:class: caution, dropdown
You **must** deploy your Serve application with [KubeRay] to leverage this feature.

See Serve's [Kubernetes production guide](serve-in-production-kubernetes) to learn how you can deploy your app with KubeRay.
:::

By default, Serve can recover from certain failures _within_ a Ray cluster, such as unhealthy actors. When [Serve runs on Kubernetes](serve-in-production-kubernetes) with [KubeRay], it can also recover from some _cluster-level_ failures, such as dead worker nodes.

When a worker node fails, the actors running on it also fail. Serve detects that the actors have failed, and it attempts to respawn the actors on the remaining, healthy nodes. Meanwhile, KubeRay detects that the node itself has failed, so it brings up a new healthy node to replace it. Serve can then respawn any pending actors on that node as well. The deployment replicas that were running on healthy nodes are unaffected and can continue serving traffic throughout the recovery period.

(serve-e2e-ft-guide-gcs)=
### Head node recovery: Ray Global Control Store (GCS) fault tolerance

:::{admonition} KubeRay Required
:class: caution, dropdown
You **must** deploy your Serve application with [KubeRay] to leverage this feature.

See Serve's [Kubernetes production guide](serve-in-production-kubernetes) to learn how you can deploy your app with KubeRay.
:::

By default the Ray head node is a single point of failure: if it crashes, the entire cluster crashes and must be restarted. When running on Kubernetes, the `RayService` controller health-checks the cluster and restarts it if this occurs, but this introduces some downtime.

In Ray 2.0, KubeRay has added experimental support for [GCS fault tolerance](https://ray-project.github.io/kuberay/guidance/gcs-ft/#ray-gcs-fault-tolerancegcs-ft-experimental), preventing the Ray cluster from crashing if the head node goes down.
While the head node is recovering, Serve applications can still handle traffic but cannot be updated or recover from other failures (e.g. actors or worker nodes crashing).
Once the GCS is recovered, the cluster will return to normal behavior.

(serve-e2e-ft-behavior)=
## Serve's fault tolerance behavior

[KubeRay]: https://ray-project.github.io/kuberay/