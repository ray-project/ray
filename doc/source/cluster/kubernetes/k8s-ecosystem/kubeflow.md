(kuberay-kubeflow-integration)=

# Kubeflow: an interactive development solution

<!-- TODO(kevin85421): Update Ray versions and replace Ray client with the Ray Job Submission -->

> Credit: This manifest refers a lot to the engineering blog ["Building a Machine Learning Platform with Kubeflow and Ray on Google Kubernetes Engine"](https://cloud.google.com/blog/products/ai-machine-learning/build-a-ml-platform-with-kubeflow-and-ray-on-gke) from Google Cloud.

The [Kubeflow](https://www.kubeflow.org/) project is dedicated to making deployments of machine learning (ML) workflows on Kubernetes simple, portable and scalable.

# Requirements
* Dependencies
    * `kustomize`: v3.2.0 (Kubeflow manifest is sensitive to `kustomize` version.)
    * `Kubernetes`: v1.23

* Computing resources:
    * 16GB RAM
    * 8 CPUs

# Example: Use Kubeflow to provide an interactive development environment
![image](../images/kubeflow-architecture.svg)

## Step 1: Create a Kubernetes cluster with Kind.
```sh
# Kubeflow is sensitive to Kubernetes version and Kustomize version.
kind create cluster --image=kindest/node:v1.26.0
kustomize version --short
# 3.2.0
```

## Step 2: Install Kubeflow v1.6-branch
* This example installs Kubeflow with the [v1.6-branch](https://github.com/kubeflow/manifests/tree/v1.6-branch).

* Install all Kubeflow official components and all common services using [one command](https://github.com/kubeflow/manifests/tree/v1.6-branch#install-with-a-single-command).
    * If you do not want to install all components, you can comment out **KNative**, **Katib**, **Tensorboards Controller**, **Tensorboard Web App**, **Training Operator**, and **KServe** from [example/kustomization.yaml](https://github.com/kubeflow/manifests/blob/v1.6-branch/example/kustomization.yaml).

## Step 3: Install KubeRay operator

* Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator via Helm repository.

## Step 4: Install RayCluster
```sh
# Create a RayCluster CR, and the KubeRay operator will reconcile a Ray cluster
# with 1 head Pod and 1 worker Pod.
helm install raycluster kuberay/ray-cluster --version 1.0.0 --set image.tag=2.2.0-py38-cpu

# Check RayCluster
kubectl get pod -l ray.io/cluster=raycluster-kuberay
# NAME                                          READY   STATUS    RESTARTS   AGE
# raycluster-kuberay-head-bz77b                 1/1     Running   0          64s
# raycluster-kuberay-worker-workergroup-8gr5q   1/1     Running   0          63s
```

* This step uses `rayproject/ray:2.2.0-py38-cpu` as its image. Ray is very sensitive to the Python versions and Ray versions between the server (RayCluster) and client (JupyterLab) sides. This image uses:
    * Python 3.8.13
    * Ray 2.2.0

## Step 5: Forward the port of Istio's Ingress-Gateway
* Follow the [instructions](https://github.com/kubeflow/manifests/tree/v1.6-branch#port-forward) to forward the port of Istio's Ingress-Gateway and log in to Kubeflow Central Dashboard.

## Step 6: Create a JupyterLab via Kubeflow Central Dashboard
* Click "Notebooks" icon in the left panel.
* Click "New Notebook"
* Select `kubeflownotebookswg/jupyter-scipy:v1.6.1` as OCI image.
* Click "Launch"
* Click "CONNECT" to connect into the JupyterLab instance.

## Step 7: Use Ray client in the JupyterLab to connect to the RayCluster
> Warning: Ray client has some known [limitations](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html#things-to-know) and is not actively maintained. We recommend using the [Ray Job Submission](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/) instead.

* As mentioned in Step 4, Ray is very sensitive to the Python versions and Ray versions between the server (RayCluster) and client (JupyterLab) sides. Open a terminal in the JupyterLab:
    ```sh
    # Check Python version. The version's MAJOR and MINOR should match with RayCluster (i.e. Python 3.8)
    python --version 
    # Python 3.8.10
    
    # Install Ray 2.2.0
    pip install -U ray[default]==2.2.0
    ```
* Connect to RayCluster via Ray client.
    ```python
    # Open a new .ipynb page.

    import ray
    # ray://${RAYCLUSTER_HEAD_SVC}.${NAMESPACE}.svc.cluster.local:${RAY_CLIENT_PORT}
    ray.init(address="ray://raycluster-kuberay-head-svc.default.svc.cluster.local:10001")
    print(ray.cluster_resources())
    # {'node:10.244.0.41': 1.0, 'memory': 3000000000.0, 'node:10.244.0.40': 1.0, 'object_store_memory': 805386239.0, 'CPU': 2.0}

    # Try Ray task
    @ray.remote
    def f(x):
        return x * x

    futures = [f.remote(i) for i in range(4)]
    print(ray.get(futures)) # [0, 1, 4, 9]

    # Try Ray actor
    @ray.remote
    class Counter(object):
        def __init__(self):
            self.n = 0

        def increment(self):
            self.n += 1

        def read(self):
            return self.n

    counters = [Counter.remote() for i in range(4)]
    [c.increment.remote() for c in counters]
    futures = [c.read.remote() for c in counters]
    print(ray.get(futures)) # [1, 1, 1, 1]
    ```
