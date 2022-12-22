(kuberay-experimental)=

# Experimental Features

We provide an overview of new and experimental features available
for deployments of Ray on Kubernetes.

## RayServices

The `RayService` controller enables fault-tolerant deployments of
{ref}`Ray Serve <rayserve>` applications on Kubernetes.

If your Ray Serve application enters an unhealthy state, the RayService controller will create a new Ray Cluster.
Once the new cluster is ready, Ray Serve traffic will be re-routed to the new Ray cluster.

For details, see the guide on {ref}`Kubernetes-based RayServe deployments <serve-in-production-kubernetes>`.

## GCS Fault Tolerance

In addition to the application-level fault-tolerance provided by the RayService controller,
Ray now supports infrastructure-level fault tolerance for the Ray head pod.

You can set up an external Redis instance as a data store for the Ray head. If the Ray head crashes,
a new head will be created without restarting the Ray cluster.
The Ray head's GCS will recover its state from the external Redis instance.

See the {ref}`Ray Serve documentation <serve-e2e-ft-guide-gcs>` for more information and
the [KubeRay docs on GCS Fault Tolerance][KubeFT] for a detailed guide.

## RayJobs

The `RayJob` custom resource consists of two elements:
1. Configuration for a Ray cluster.
2. A job, i.e. a Ray program to be executed on the Ray cluster.

To run a Ray job, you create a RayJob CR:
```shell
kubectl apply -f rayjob.yaml
```
The RayJob controller then creates the Ray cluster and runs the job.
If you wish, you may configure the Ray cluster to be deleted when the job finishes.

See the [KubeRay docs on RayJobs][KubeJob] for details.

[KubeServe]: https://ray-project.github.io/kuberay/guidance/rayservice/
[KubeFT]: https://ray-project.github.io/kuberay/guidance/gcs-ft/
[KubeJob]: https://ray-project.github.io/kuberay/guidance/rayjob/
