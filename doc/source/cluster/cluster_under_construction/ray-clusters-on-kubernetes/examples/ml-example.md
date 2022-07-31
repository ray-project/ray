(kuberay-ml-example)=

# XGBoost on Ray, on Kubernetes

In this guide, we show you how to run a sample Ray machine learning
workload on Kubernetes infrastructure.

We will run Ray's XGBoost benchmark with a 100 gigabyte training set.

## Autoscaling: Pros and Cons

This guide will show how to run XGBoost workload with and without optional Ray Autoscaler support.
Here are some considerations when choosing whether or not to use autoscaling functionality.

### Pros of autoscaling

#### Deal with unknown resource requirements.
If you don't know how much compute your Ray workload will require,
autoscaling will adjust your Ray cluster to the right size.

#### Cost-savings.
Idle compute is automatically scaled down, potentially leading to cost savings.

### Cons of autoscaling:

#### Less predictable when resource requirements are known.
If you already know exactly how much compute your workload requires, it may make
sense to provision a statically-sized Ray cluster.
In this guide's example, we know that we need 1 Ray head and 9 Ray workers,
so autoscaling is not strictly required.

#### Longer end-to-end runtime.
Autoscaling entails provisioning compute for Ray workers while the Ray application
is running. Pre-provisioning static compute resources allows all required compute
to be provisioned in parallel before the application executes, potentially reducing the application's runtime.

## Kubernetes infrastructure setup.

For the workload in this guide, it is recommended to use a pool (group) of Kubernetes nodes
with the following properties:
- 10 nodes total
- A capacity of 16 CPU and 64 Gi memory per node.
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
refer to the cloud provider documentation.
- [EKS (Amazon Web Services)](https://docs.aws.amazon.com/eks/latest/userguide/managed-node-groups.html)
- [AKS (Azure)](https://docs.microsoft.com/en-us/azure/aks/use-multiple-node-pools)
- [GKE (Google Cloud)](https://cloud.google.com/kubernetes-engine/docs/concepts/node-pools)

## Deploying the KubeRay operator

Once you have set up your Kubernetes cluster, deploy the KubeRay operator:
```shell
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v0.3.0-rc.0"
```

## Deploying a Ray cluster

Now we're ready to deploy the Ray cluster that will execute our workload.

You may choose to deploy either a statically-sized Ray cluster or an autoscaling
Ray cluster. Run one of the two commands below to deploy your Ray cluster.

### Deploying a fixed-sized Ray cluster

This option is most appropriate if you have set up a statically sized Kubernetes
node pool or group.

```shell
# We recommend taking a look at the config file applied in this command.
kubectl apply -f ...
```

A Ray head pod and 9 Ray worker pods will be created.

### Deploying an autoscaling Ray cluster

This option is most appropriate if you have set up an autoscaling Kubernetes
node pool or group.

```shell
# We recommend taking a look at the config file applied in this command.
kubectl apply -f ...
```

One Ray head pod will be created. Once the workload is run, the Ray autoscaler will trigger
creation of Ray worker pods. Kubernetes autoscaling will then create nodes to place the Ray pods.


## Running the workload

Once the Ray head pod enters Running state, we are ready to execute the XGBoost workload.
We will use Ray Job Submission to kick off the workload.

### Connect to the cluster.

First, we connect to the job server. Run the following blocking command
in a separate shell.
```shell
kubectl port-forward service/raycluster-xgboost-benchmark-head-svc 8265:8265
```

### Submit the workload.

We'll use the Python Job client to submit the xgboost workload.

```{literalinclude} ../serve/doc_code/sklearn_quickstart.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

```shell
# From the parent directory of cloned Ray master.
python ray/doc/source/submit_xgboost.py
```

### Observe progress.

The benchmark may take up to 30 minutes to run.
Use the following tools to observe its progress.

#### Job logs

Use the command displayed by the submission script executed above to follow the job logs.
```shell
ray job logs 'raysubmit_ebfPPZv1kByG9t8V' --follow
```

#### Ray cluster state

Observe the pods in your cluster with
```shell
# If you're on MacOS, first `brew install watch`.
watch -n 1 kubectl get pod
```

#### Ray dashboard

View `localhost:8265` in your browser.

#### Ray Status

Observe autoscaling status and Ray resource usage with
```shell
# Substitute the name of your Ray cluster's head pod.
watch -n 1 kubectl exec -it -- ray status
```

:::{note}
Under some circumstances and for certain cloud providers,
the K8s API server may become briefly unavailable during Kuberentes
cluster resizing events.

Don't worry if that happens -- the workload should be uninterrupted.
For the example in this guide, simply restart the port-forwarding process and
re-run the job log command.
:::

### Job complete.

#### Benchmarks results

```
Results: {'training_time': 1338.488839321999, 'prediction_time': 403.36653568099973}
```

The performance of the benchmark is sensitive to the underlying cloud infrastructure --
you might not match [the numbers quoted in the benchmark docs]().

#### Model parameters

#### Scale-down
If autoscaling is enabled, Ray worker pods will scale down after 60 seconds.
The Kubernetes nodes in your autoscaling pool will be scaled down after a configurable timeout.

#### Clean-up
Delete your Ray cluster with the following command:
```shell
kubectl delete raycluster raycluster-xgboost-benchmark
```

If you're on a public cloud, don't forget to clean up the underlying
node group and/or Kubernetes cluster.
