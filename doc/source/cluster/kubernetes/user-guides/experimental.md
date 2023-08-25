(kuberay-experimental)=

# Experimental Features

We provide an overview of new and experimental features available
for deployments of Ray on Kubernetes.

## GCS Fault Tolerance

In addition to the application-level fault-tolerance provided by the RayService controller,
Ray now supports infrastructure-level fault tolerance for the Ray head pod.

You can set up an external Redis instance as a data store for the Ray head. If the Ray head crashes,
a new head will be created without restarting the Ray cluster.
The Ray head's GCS will recover its state from the external Redis instance.

See the {ref}`Ray Serve documentation <serve-e2e-ft-guide-gcs>` for more information and
the [KubeRay docs on GCS Fault Tolerance][KubeFT] for a detailed guide.

[KubeServe]: https://ray-project.github.io/kuberay/guidance/rayservice/
[KubeFT]: https://ray-project.github.io/kuberay/guidance/gcs-ft/
[KubeJob]: https://ray-project.github.io/kuberay/guidance/rayjob/
