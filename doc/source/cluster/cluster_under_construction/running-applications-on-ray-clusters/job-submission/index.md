(ray-jobs)=

# Ray Jobs
:::{warning}
This page is under construction!
:::

Once you have deployed a Ray cluster (on [VMs](ref-cluster-quick-start-vms-under-construction) or [Kubernetes](kuberay-quickstart)), you are ready to run a Ray application!

The recommended way to run a Ray application on a Ray Cluster is to use *Ray Jobs*.
This mechanism allows you to submit locally developed applications to a remote Ray Cluster for execution.
It simplifies the experience of packaging, deploying, and managing a Ray application.

:::{note}
This component is in **beta**. APIs may change before becoming stable.
:::

A Ray Job consists of:
1. An entrypoint command, like `python my_script.py`.
2. A [runtime environment](runtime-environments), which specifies the application's file and package dependencies.

A Ray Job can be submitted by a remote client that lives outside of the Ray Cluster, which we will show in the following user guides.
After a Ray Job is submitted, it runs once to completion or failure, regardless of the original submitter's connectivity.
Retries or different runs with different parameters should be handled by the submitter.
Jobs are bound to the lifetime of a Ray cluster, so if the cluster goes down, all running jobs on that cluster will be terminated.

To get started with Ray Jobs, check out the [quickstart](jobs-quickstart-under-construction) guide, which walks you through the CLI tools for submitting and interacting with a Ray Job.
This is suitable for any client that can communicate over HTTP to the Ray Cluster.
If needed, Ray Jobs also provides APIs for [programmatic job submission](ray-job-sdk-under-construction) and [job submission using REST](ray-job-rest-api-under-construction).

Finally, if you would like to run an application *interactively* and see the output in real time in a client of your choice (e.g. your local machine, SageMaker Studio, or Google Colab), you can use [Ray Client](ray-client-under-construction). This tool can be useful during development.

```{toctree}
:maxdepth: '1'

quickstart
sdk
rest
jobs-package-ref
ray-client
```
