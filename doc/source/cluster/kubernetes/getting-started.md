(kuberay-quickstart)=

# Getting Started with KubeRay

```{toctree}
:hidden:

getting-started/raycluster-quick-start
getting-started/rayjob-quick-start
getting-started/rayservice-quick-start
```


## Custom Resource Definitions (CRDs)

[KubeRay](https://github.com/ray-project/kuberay) is a powerful, open-source Kubernetes operator that simplifies the deployment and management of Ray applications on Kubernetes.
It offers 3 custom resource definitions (CRDs):

* **RayCluster**: KubeRay fully manages the lifecycle of RayCluster, including cluster creation/deletion, autoscaling, and ensuring fault tolerance.

* **RayJob**: With RayJob, KubeRay automatically creates a RayCluster and submits a job when the cluster is ready. You can also configure RayJob to automatically delete the RayCluster once the job finishes.

* **RayService**: RayService is made up of two parts: a RayCluster and Ray Serve deployment graphs. RayService offers zero-downtime upgrades for RayCluster and high availability.

## Which CRD should you choose?

Using [RayService](kuberay-rayservice-quickstart) to serve models and using [RayCluster](kuberay-raycluster-quickstart) to develop Ray applications are no-brainer recommendations from us.
However, if the use case is not model serving or prototyping, how do you choose between [RayCluster](kuberay-raycluster-quickstart) and [RayJob](kuberay-rayjob-quickstart)?

### Q: Is downtime acceptable during a cluster upgrade (e.g. Upgrade Ray version)?

If not, use RayJob. RayJob can be configured to automatically delete the RayCluster once the job is completed. You can switch between Ray versions and configurations for each job submission using RayJob.

If yes, use RayCluster. Ray doesn't natively support rolling upgrades; thus, you'll need to manually shut down and create a new RayCluster.

### Q: Are you deploying on public cloud providers (e.g. AWS, GCP, Azure)?

If yes, use RayJob. It allows automatic deletion of the RayCluster upon job completion, helping you reduce costs.

### Q: Do you care about the latency introduced by spinning up a RayCluster?

If yes, use RayCluster.
Unlike RayJob, which creates a new RayCluster every time a job is submitted, RayCluster creates the cluster just once and can be used multiple times.

## Run your first Ray application on Kubernetes!

* [RayCluster Quick Start](kuberay-raycluster-quickstart)
* [RayJob Quick Start](kuberay-rayjob-quickstart)
* [RayService Quick Start](kuberay-rayservice-quickstart)
