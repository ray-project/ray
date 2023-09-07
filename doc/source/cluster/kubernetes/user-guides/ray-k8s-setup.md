(ray-kubernetes-setup)=

# Set up a Ray + Kubernetes cluster

This document contains recommendations for setting up a Ray + Kubernetes cluster for your organization.

When you set up Ray on Kubernetes, the KubeRay documentation provides an overview of how to configure the operator to execute and manage the Ray cluster lifecycle. This guide complements the KubeRay documentation by providing best practices for effectively using Ray deployments in your organization.

This guide covers best practices for these deployment considerations:

* Where to ship or run your code on the Ray cluster
* Choosing a storage system for artifacts
* Package dependencies for your application

Deployment considerations are different for development and production. This table summarizes the recommended setup for both interactive development and production:

|   | Interactive Development  | Production  |
|---|---|---|
| Cluster Configuration  | KubeRay YAML  | KubeRay YAML  |
| Code | Run driver or Jupyter notebook on head node | S3 + runtime envs <br /> OR <br /> Bake code into Docker image (link)  |
| Artifact Storage | Set up an EFS  | Cloud storage (S3, GS)  |
| Package Dependencies | Install onto NFS <br /> or <br /> Use runtime environments | Bake into docker image  |

Table 1: Table comparing recommended setup for development and production.

## Interactive development

To provide an interactive development environment for data scientists, you should set up the code, storage, and dependencies in a way that reduces context switches for developers and shortens iteration times.

```{eval-rst}
.. image:: ../images/interact-dev.png
    :align: center
..
    Find the source document here (https://whimsical.com/clusters-P5Y6R23riCuNb6xwXVXN72)
```

### Storage

Use one of these two standard solutions for artifact and log storage during the development process:

* POSIX-compliant network file storage (like AWS and EFS): This approach is useful when you have artifacts or dependencies accessible in an interactive fashion.
* Cloud storage (like AWS S3 or GCP GS): This approach is useful for large artifacts or datasets that you need to access with high throughput.

### Driver script

Run the main (driver) script on the head node of the cluster. Ray Core and library programs often assume that the driver is located on the head node, and take advantage of the local storage. For example:

* Start a Jupyter server on the head node
* SSH onto the head node and run the driver script or application there
* Use the Ray Job Submission client to submit code from a local machine onto a cluster

### Dependencies

For local dependencies (for example, if you’re working in a mono-repo), or external dependencies (like a pip package), use one of the following options:

* Put the code and install the packages onto your NFS. The benefit is that you can quickly interact with the rest of the codebase and dependencies without shipping it across a cluster every time.
* Bake remote and local dependencies into a published Docker image for the workers. This is the most common way to deploy applications onto [Kubernetes](https://kube.academy/courses/building-applications-for-kubernetes). 
* Use the `runtime env` with the [Ray Job Submission Client](ray.job_submission.JobSubmissionClient), which can pull down code from S3 or ship code from your local working directory onto the remote cluster.

## Production

For production, we suggest the following configuration.


```{eval-rst}
.. image:: ../images/prod.png
    :align: center
..
    Find the source document here (https://whimsical.com/clusters-P5Y6R23riCuNb6xwXVXN72)
```


### Storage

Reading and writing data and artifacts to cloud storage is the most reliable and observable option for production Ray deployments. 

### Code and Dependencies

Bake your code, remote, and local dependencies into a published Docker image for the workers. This is the most common way to deploy applications onto [Kubernetes](https://kube.academy/courses/building-applications-for-kubernetes).

Using Cloud storage and the `runtime_env` is a less preferred method. In this case, use the runtime environment option to download zip files containing code and other private modules from cloud storage, in addition to specifying the pip packages needed to run your application.
