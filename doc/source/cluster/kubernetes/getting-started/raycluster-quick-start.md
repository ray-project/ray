(kuberay-raycluster-quickstart)=

# RayCluster

In this guide, we show you how to manage and interact with Ray clusters on Kubernetes.

## Preparation

* Install [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) (>= 1.19), [Helm](https://helm.sh/docs/intro/install/) (>= v3.4), and [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation).
* Make sure your Kubernetes cluster has at least 4 CPU and 4 GB RAM.

## Step 1: Create a Kubernetes cluster

This step creates a local Kubernetes cluster using [kind](https://kind.sigs.k8s.io/). If you already have a Kubernetes cluster, you can skip this step.

```sh
kind create cluster --image=kindest/node:v1.23.0
```

(kuberay-operator-deploy)=
## Step 2: Deploy a KubeRay operator

Deploy the KubeRay operator with the [Helm chart repository](https://github.com/ray-project/kuberay-helm).

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/

# Install both CRDs and KubeRay operator v0.6.0.
helm install kuberay-operator kuberay/kuberay-operator --version 0.6.0

# Confirm that the operator is running in the namespace `default`.
kubectl get pods
# NAME                                READY   STATUS    RESTARTS   AGE
# kuberay-operator-7fbdbf8c89-pt8bk   1/1     Running   0          27s
```

KubeRay offers multiple options for operator installations, such as Helm, Kustomize, and a single-namespaced operator. For further information, please refer to [the installation instructions in the KubeRay documentation](https://ray-project.github.io/kuberay/deploy/installation/).

## Step 3: Deploy a RayCluster custom resource

Once the KubeRay operator is running, we are ready to deploy a RayCluster. To do so, we create a RayCluster Custom Resource (CR) in the `default` namespace.

```sh
# Deploy a sample RayCluster CR from the KubeRay Helm chart repo:
helm install raycluster kuberay/ray-cluster --version 0.6.0

# Once the RayCluster CR has been created, you can view it by running:
kubectl get rayclusters

# NAME                 DESIRED WORKERS   AVAILABLE WORKERS   STATUS   AGE
# raycluster-kuberay   1                 1                   ready    72s
```

The KubeRay operator will detect the RayCluster object. The operator will then start your Ray cluster by creating head and worker pods. To view Ray cluster's pods, run the following command:

```sh
# View the pods in the RayCluster named "raycluster-kuberay"
kubectl get pods --selector=ray.io/cluster=raycluster-kuberay

# NAME                                          READY   STATUS    RESTARTS   AGE
# raycluster-kuberay-head-vkj4n                 1/1     Running   0          XXs
# raycluster-kuberay-worker-workergroup-xvfkr   1/1     Running   0          XXs
```

Wait for the pods to reach Running state. This may take a few minutes -- most of this time is spent downloading the Ray images.
If your pods are stuck in the Pending state, you can check for errors via `kubectl describe pod raycluster-kuberay-xxxx-xxxxx` and ensure that your Docker resource limits are set high enough.
Note that in production scenarios, you will want to use larger Ray pods. In fact, it is advantageous to size each Ray pod to take up an entire Kubernetes node. See the [configuration guide](kuberay-config) for more details.

## Step 4: Run an application on a RayCluster

Now, let's interact with the RayCluster we've deployed. 

### Method 1: Execute a Ray job in the head Pod

The most straightforward way to experiment with your RayCluster is to exec directly into the head pod.
First, identify your RayCluster's head pod:

```sh
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
echo $HEAD_POD
# raycluster-kuberay-head-vkj4n

# Print the cluster resources.
kubectl exec -it $HEAD_POD -- python -c "import ray; ray.init(); print(ray.cluster_resources())"

# 2023-04-07 10:57:46,472 INFO worker.py:1243 -- Using address 127.0.0.1:6379 set in the environment variable RAY_ADDRESS
# 2023-04-07 10:57:46,472 INFO worker.py:1364 -- Connecting to existing Ray cluster at address: 10.244.0.6:6379...
# 2023-04-07 10:57:46,482 INFO worker.py:1550 -- Connected to Ray cluster. View the dashboard at http://10.244.0.6:8265 
# {'object_store_memory': 802572287.0, 'memory': 3000000000.0, 'node:10.244.0.6': 1.0, 'CPU': 2.0, 'node:10.244.0.7': 1.0}
```

### Method 2: Submit a Ray job to the RayCluster via [ray job submission SDK](jobs-quickstart)

Unlike Method 1, this method does not require you to execute commands in the Ray head pod.
Instead, you can use the [Ray job submission SDK](jobs-quickstart) to submit Ray jobs to the RayCluster via the Ray Dashboard port (8265 by default) where Ray listens for Job requests.
The KubeRay operator configures a [Kubernetes service](https://kubernetes.io/docs/concepts/services-networking/service/) targeting the Ray head Pod.

```sh
kubectl get service raycluster-kuberay-head-svc

# NAME                          TYPE        CLUSTER-IP    EXTERNAL-IP   PORT(S)                                         AGE
# raycluster-kuberay-head-svc   ClusterIP   10.96.93.74   <none>        8265/TCP,8080/TCP,8000/TCP,10001/TCP,6379/TCP   15m
```

Now that we have the name of the service, we can use port-forwarding to access the Ray Dashboard port (8265 by default).

```sh
# Execute this in a separate shell.
kubectl port-forward --address 0.0.0.0 service/raycluster-kuberay-head-svc 8265:8265

# Visit ${YOUR_IP}:8265 in your browser for the Dashboard (e.g. 127.0.0.1:8265)
```

Note: We use port-forwarding in this guide as a simple way to experiment with a RayCluster's services. For production use-cases, you would typically either 
- Access the service from within the Kubernetes cluster or
- Use an ingress controller to expose the service outside the cluster.

See the {ref}`networking notes <kuberay-networking>` for details.

Now that we have access to the Dashboard port, we can submit jobs to the RayCluster:

```sh
# The following job's logs will show the Ray cluster's total resource capacity, including 2 CPUs.
ray job submit --address http://localhost:8265 -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

## Step 5: Cleanup

```sh
# [Step 5.1]: Delete the RayCluster CR
# Uninstall the RayCluster Helm chart
helm uninstall raycluster
# release "raycluster" uninstalled

# Note that it may take several seconds for the Ray pods to be fully terminated.
# Confirm that the RayCluster's pods are gone by running
kubectl get pods

# NAME                                READY   STATUS    RESTARTS   AGE
# kuberay-operator-7fbdbf8c89-pt8bk   1/1     Running   0          XXm

# [Step 5.2]: Delete the KubeRay operator
# Uninstall the KubeRay operator Helm chart
helm uninstall kuberay-operator
# release "kuberay-operator" uninstalled

# Confirm that the KubeRay operator pod is gone by running
kubectl get pods
# No resources found in default namespace.

# [Step 5.3]: Delete the Kubernetes cluster
kind delete cluster
```
