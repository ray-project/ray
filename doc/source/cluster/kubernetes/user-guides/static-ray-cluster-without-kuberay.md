(deploy-a-static-ray-cluster-without-kuberay)=

# (Advanced) Deploying a static Ray cluster without KubeRay

This deployment method for Ray no longer requires the use of CustomResourceDefinitions (CRDs).
In contrast, the CRDs is a prerequisite to use KubeRay. One of its key components, the KubeRay operator,
manages the Ray cluster resources by watching for Kubernetes events (create/delete/update).
Although the KubeRay operator can function within a single namespace, the use of CRDs has a cluster-wide scope.
If the necessary Kubernetes admin permissions are not available for deploying KubeRay, this doc introduces a way to deploy a static Ray cluster to Kubernetes without using KubeRay. However, it should be noted that this deployment method lacks the built-in
autoscaling feature that KubeRay provides.

## Preparation

### Install the latest Ray release

This step is necessary for interacting with remote clusters using {ref}`Ray Job Submission <jobs-overview>`.

```
! pip install -U "ray[default]"
```

See {ref}`installation` for more details.

### Install kubectl

To interact with Kubernetes, we will use kubectl. Installation instructions can be found in the [Kubernetes documentation](https://kubernetes.io/docs/tasks/tools/#kubectl).

### Access a Kubernetes cluster

We will need access to a Kubernetes cluster. There are two options:

1. Configure access to a remote Kubernetes cluster
**OR**

2. Run the examples locally by [installing kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation). Start your [kind](https://kind.sigs.k8s.io/) cluster by running the following command:

```
! kind create cluster
```

To execute the example in this guide, ensure that your Kubernetes cluster (or local Kind cluster) can handle additional resource requests of 3 CPU and 3Gi memory.
Also, ensure that both your Kubernetes cluster and Kubectl are at least version 1.19.

### Deploying a Redis for fault tolerance

Note that [the Kubernetes deployment config file](https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/kubernetes/configs/static-ray-cluster.with-fault-tolerance.yaml) has a section to deploy a Redis to Kubernetes so that the Ray head can write through the GCS metadata.
If a Redis has already been deployed on Kubernetes, this section can be omitted.

## Deploying a static Ray cluster

In this section, we will deploy a static Ray cluster into the `default` namespace without using KubeRay. To use another
namespace, specify the namespace in your kubectl commands:

`kubectl -n <your-namespace> ...`

```
# Deploy a sample Ray Cluster from the Ray repo:

! kubectl apply -f https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/kubernetes/configs/static-ray-cluster.with-fault-tolerance.yaml

# Note that the Ray cluster has fault tolerance enabled by default using the external Redis. 
# Please set the Redis IP address in the config.

# The password is currently set as '' for the external Redis. 
# Please download the config file and substitute the real password for the empty string if the external Redis has a password.
```

Once the Ray cluster has been deployed, you can view the pods for the head node and worker nodes by running

```
! kubectl get pods

# NAME                                             READY   STATUS    RESTARTS   AGE
# deployment-ray-head-xxxxx                        1/1     Running   0          XXs
# deployment-ray-worker-xxxxx                      1/1     Running   0          XXs
# deployment-ray-worker-xxxxx                      1/1     Running   0          XXs
```

Wait for the pods to reach the `Running` state. This may take a few minutes -- most of this time is spent downloading the Ray images.
In a separate shell, you may wish to observe the pods' status in real-time with the following command:

```
# If you're on MacOS, first `brew install watch`.
# Run in a separate shell:

! watch -n 1 kubectl get pod
```

If your pods are stuck in the `Pending` state, you can check for errors via `kubectl describe pod deployment-ray-head-xxxx-xxxxx`
and ensure that your Docker resource limits are set high enough.

Note that in production scenarios, you will want to use larger Ray pods. In fact, it is advantageous to size each Ray pod to take up an entire Kubernetes node. See the [configuration guide](kuberay-config) for more details.

## Deploying a network policy for the static Ray cluster

If your Kubernetes has a default deny network policy for pods, you need to manually create a network policy to allow bidirectional
communication among the head and worker nodes in the Ray cluster as mentioned in [the ports configurations doc](https://docs.ray.io/en/latest/ray-core/configure.html#ports-configurations).

```
# Create a sample network policy for the static Ray cluster from the Ray repo:
! kubectl apply -f https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/kubernetes/configs/static-ray-cluster-networkpolicy.yaml
```

Once the network policy has been deployed, you can view the network policy for the static Ray cluster by running

```
! kubectl get networkpolicies

# NAME                               POD-SELECTOR                           AGE
# ray-head-egress                    app=ray-cluster-head                   XXs
# ray-head-ingress                   app=ray-cluster-head                   XXs
# ray-worker-egress                  app=ray-cluster-worker                 XXs
# ray-worker-ingress                 app=ray-cluster-worker                 XXs
```

### External Redis Integration for fault tolerance

Ray by default uses an internal key-value store, called the Global Control Store (GCS). The GCS runs on the head node and stores cluster
metadata. One drawback of this approach is that the head node loses the metadata if it crashes.
Ray can also write this metadata to an external Redis for reliability and high availability.
With this setup, the static Ray cluster can recover from head node crashes and tolerate GCS failures without losing connections to worker nodes.

To use this feature, we need to pass in the `RAY_REDIS_ADDRESS` env var and `--redis-password` in the Ray head node section of [the Kubernetes deployment config file](https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/kubernetes/configs/static-ray-cluster.with-fault-tolerance.yaml).

## Running Applications on the static Ray Cluster

In this section, we will interact with the static Ray cluster that just got deployed.

### Accessing the cluster with kubectl exec

Same as the Ray cluster that deployed using KubeRay, we can exec directly into the head pod and run a Ray program.

Firstly, run the command below to get the head pod:

```
! kubectl get pods --selector=app=ray-cluster-head

# NAME                                             READY   STATUS    RESTARTS   AGE
# deployment-ray-head-xxxxx                        1/1     Running   0          XXs
```

We can now execute a Ray program on the previously identified head pod. The following command connects to the Ray Cluster and then terminates the Ray program.

```
# Substitute your output from the last cell in place of "deployment-ray-head-xxxxx"

! kubectl exec deployment-ray-head-xxxxx -it -c ray-head -- python -c "import ray; ray.init('auto')"
# 2022-08-10 11:23:17,093 INFO worker.py:1312 -- Connecting to existing Ray cluster at address: <IP address>:6380...
# 2022-08-10 11:23:17,097 INFO worker.py:1490 -- Connected to Ray cluster. View the dashboard at ...
```

Although the above cell can be useful for occasional execution on the Ray Cluster, the recommended approach for running an application on a Ray Cluster is to use [Ray Jobs](jobs-quickstart).

### Ray Job submission

To set up your Ray Cluster for Ray Jobs submission, it is necessary to ensure that the Ray Jobs port is accessible to the client.
Ray receives job requests through the Dashboard server on the head node.

First, we need to identify the Ray head node. The static Ray cluster configuration file sets up a
[Kubernetes service](https://kubernetes.io/docs/concepts/services-networking/service/) that targets the Ray head pod.
This service lets us interact with Ray clusters without directly executing commands in the Ray container.
To identify the Ray head service for our example cluster, run:

```
! kubectl get service service-ray-cluster

# NAME                             TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)                            AGE
# service-ray-cluster              ClusterIP   10.92.118.20   <none>        6380/TCP,8265/TCP,10001/TCP...     XXs
```

Now that we have the name of the service, we can use port-forwarding to access the Ray Dashboard port (8265 by default).

```
# Execute this in a separate shell.
# Substitute the service name in place of service-ray-cluster

! kubectl port-forward service/service-ray-cluster 8265:8265
```

Now that we have access to the Dashboard port, we can submit jobs to the Ray Cluster for execution:

```
! ray job submit --address http://localhost:8265 -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

## Cleanup

### Deleting a Ray Cluster

Delete the static Ray cluster service and deployments

```
! kubectl delete -f https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/kubernetes/configs/static-ray-cluster.with-fault-tolerance.yaml
```

Delete the static Ray cluster network policy

```
! kubectl delete -f https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/kubernetes/configs/static-ray-cluster-networkpolicy.yaml
```
