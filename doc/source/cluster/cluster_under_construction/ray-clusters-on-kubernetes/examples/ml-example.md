(kuberay-ml-example)=

# XGBoost-Ray on Kubernetes

:::{note}
To learn the basics of Ray on Kubernetes, we recommend taking a look
at the {ref}`introductory guide<kuberay-quickstart>` first.
:::

In this guide, we show you how to run a sample Ray machine learning
workload on Kubernetes infrastructure.

We will run Ray's {ref}`XGBoost training benchmark<xgboost-benchmark>` with a 100 gigabyte training set.
* Learn more about {ref}`XGBoost-Ray<xgboost-ray>`.

## A note on autoscaling
This guide will show you how to run an XGBoost workload with and without optional Ray Autoscaler support.
When choosing whether or not to use autoscaling functionality, keep the following considerations in mind.

### Autoscaling: Pros

#### Cope with unknown resource requirements.
If you don't know how much compute your Ray workload will require,
autoscaling will adjust your Ray cluster to the right size.

#### Save on costs.
Idle compute is automatically scaled down, potentially leading to cost savings.

### Autoscaling: Cons

#### Less predictable when resource requirements are known.
If you already know exactly how much compute your workload requires, it makes
sense to provision a statically-sized Ray cluster.
In this guide's example, we know that we need 1 Ray head and 9 Ray workers,
so autoscaling is not strictly required.

#### Longer end-to-end runtime.
Autoscaling entails provisioning compute for Ray workers while the Ray application
is running. On the other hand, if you pre-provision a fixed number of Ray nodes,
all of the Ray nodes can be started in parallel, potentially reducing your application's
runtime.

## Kubernetes infrastructure setup

For the workload in this guide, it is recommended to use a pool (group) of Kubernetes nodes
with the following properties:
- 10 nodes total
- A capacity of 16 CPU and 64 Gi memory per node. For the major cloud providers, suitable instance types include
    * m5.4xlarge (Amazon Web Services)
    * Standard_D5_v2 (Azure)
    * e2-standard-16 (Google Cloud)
- Each node should be configured with 1000 gigabytes of disk space (to store the training set).

### Kubernetes infrastructure and autoscaling

**If you would like to try running the workload with autoscaling enabled**, use an autoscaling
node group or pool with
- 1 node minimum
- 10 nodes maximum

The 1 static node will be used to run the Ray head pod. This node may also host the KubeRay
operator and Kubernetes system components. After the workload is submitted, 9 additional nodes will
scale up to accommodate Ray worker pods. These nodes will scale back down after the workload is complete.

### Learn more about node pools

To learn about node group or node pool setup with managed Kubernetes services,
refer to you cloud provider's documentation.
- [AKS (Azure)](https://docs.microsoft.com/en-us/azure/aks/use-multiple-node-pools)
- [EKS (Amazon Web Services)](https://docs.aws.amazon.com/eks/latest/userguide/managed-node-groups.html)
- [GKE (Google Cloud)](https://cloud.google.com/kubernetes-engine/docs/concepts/node-pools)

## Deploying the KubeRay operator

Once you have set up your Kubernetes cluster, deploy the KubeRay operator:
```shell
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v0.3.0-rc.0"
```

## Deploying a Ray cluster

Now we're ready to deploy the Ray cluster that will execute our workload.

:::{tip}
The Ray cluster we'll deploy is configured such that one Ray pod will be scheduled
per Kubernetes node. The pattern of one Ray pod per Kubernetes node is encouraged, but not required.
Broadly speaking, it is more efficient to use a few large Ray pods than many small ones.
:::

You may choose to deploy either a statically-sized Ray cluster or an autoscaling
Ray cluster. Run one of the two commands below to deploy your Ray cluster.

### Deploying a fixed-sized Ray cluster

This option is most appropriate if you have set up a statically sized Kubernetes
node pool or group.

We recommend taking a look at the config file applied in the following command.
```shell
# Starting from the parent directory of cloned Ray master,
pushd ray/doc/source/cluster/cluster_under_construction/ray-clusters-on-kubernetes/configs/
kubectl apply -f xgboost-benchmark.yaml
popd
```

A Ray head pod and 9 Ray worker pods will be created.

### Deploying an autoscaling Ray cluster

This option is most appropriate if you have set up an autoscaling Kubernetes
node pool or group.

We recommend taking a look at the config file applied in the following command.
```shell
# Starting from the parent directory of cloned Ray master,
pushd ray/doc/source/cluster/cluster_under_construction/ray-clusters-on-kubernetes/configs/
kubectl apply -f xgboost-benchmark-autoscaler.yaml
popd
```

One Ray head pod will be created. Once the workload is run, the Ray autoscaler will trigger
creation of Ray worker pods. Kubernetes autoscaling will then create nodes to place the Ray pods.


## Running the workload

To observe the startup progress of the Ray head pod, run the following command.
```shell
# If you're on MacOS, first `brew install watch`.
watch -n 1 kubectl get pod
```

Once the Ray head pod enters `Running` state, we are ready to execute the XGBoost workload.
We will use {ref}`Ray Job Submission<jobs-overview>` to kick off the workload.

### Connect to the cluster.

First, we connect to the Job server. Run the following blocking command
in a separate shell.
```shell
kubectl port-forward service/raycluster-xgboost-benchmark-head-svc 8265:8265
```

### Submit the workload.

We'll use the Ray Job {ref}`Python SDK<ray-job-sdk>` to submit the xgboost workload:

```{literalinclude} ../doc_code/xgboost_submit.py
:language: python
```

To submit the workload, run the above Python script.
This script is available as a file in the Ray repository.

```shell
# From the parent directory of cloned Ray master.
pushd ray/doc/source/cluster/cluster_under_construction/ray-clusters-on-kubernetes/doc_code/
python xgboost_submit.py
popd
```

### Observe progress.

The benchmark may take up to 30 minutes to run.
Use the following tools to observe its progress.

#### Job logs

To follow the job's logs, use the command printed by the above submission script.
```shell
# Subsitute the Ray Job's submission id.
ray job logs 'raysubmit_xxxxxxxxxxxxxxxx' --follow
```

#### Kubectl

Observe the pods in your cluster with
```shell
# If you're on MacOS, first `brew install watch`.
watch -n 1 kubectl get pod
```

#### Ray Dashboard

View `localhost:8265` in your browser to access the Ray Dashboard.

#### Ray Status

Observe autoscaling status and Ray resource usage with
```shell
# Substitute the name of your Ray cluster's head pod.
watch -n 1 kubectl exec -it raycluster-xgboost-benchmark-head-xxxxx -- ray status
```

:::{note}
Under some circumstances and for certain cloud providers,
the K8s API server may become briefly unavailable during Kuberentes
cluster resizing events.

Don't worry if that happens -- the Ray workload should be uninterrupted.
For the example in this guide, wait until the API server is back up, restart the port-forwarding process,
and re-run the job log command.
:::

### Job completion

#### Benchmark results

Once the benchmark is complete, the job log will display the results:

```
Results: {'training_time': 1338.488839321999, 'prediction_time': 403.36653568099973}
```

The performance of the benchmark is sensitive to the underlying cloud infrastructure --
you might not match {ref}`the numbers quoted in the benchmark docs<xgboost-benchmark>`.

#### Model parameters
The file `model.json` in the head pod contains the parameters for the trained model.
Other result data will be available in the directory `ray_results` in the head pod.
Refer to the `XGBoost-Ray documentation<xgboost-ray>` for details.

#### Scale-down
If autoscaling is enabled, Ray worker pods will scale down after 60 seconds.
After the Ray worker pods are gone, your Kubernetes infrastructure should scale down the nodes
that hosted these pods.

#### Clean-up
Delete your Ray cluster with the following command:
```shell
kubectl delete raycluster raycluster-xgboost-benchmark
```
If you're on a public cloud, don't forget to clean up the underlying
node group and/or Kubernetes cluster.
