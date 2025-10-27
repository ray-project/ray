(kuberay-text-summarizer-rayservice-example)=

# Serve a text summarizer on Kubernetes

> **Note:** The Python files for the Ray Serve application and its client are in the [ray-project/serve_config_examples](https://github.com/ray-project/serve_config_examples) repository.

## Step 1: Create a Kubernetes cluster with GPUs

See [aws-eks-gpu-cluster.md](kuberay-eks-gpu-cluster-setup) or [gcp-gke-gpu-cluster.md](kuberay-gke-gpu-cluster-setup) or [ack-gpu-cluster.md](kuberay-ack-gpu-cluster-setup) to create a Kubernetes cluster with 1 CPU node and 1 GPU node.

## Step 2: Install KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator using the Helm repository.

## Step 3: Install a RayService

```sh
# Create a RayService
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.text-summarizer.yaml
```

* In the RayService, the head Pod doesn't have any `tolerations`. Meanwhile, the worker Pods use the following `tolerations` so the scheduler won't assign the head Pod to the GPU node.
    ```yaml
    # Please add the following taints to the GPU node.
    tolerations:
        - key: "ray.io/node-type"
        operator: "Equal"
        value: "worker"
        effect: "NoSchedule"
    ```

## Step 4: Forward the port of Serve

```sh
# Step 4.1: Wait until the RayService is ready to serve requests.
kubectl describe rayservices text-summarizer

# Step 4.2: Get the service name.
kubectl get services

# [Example output]
# text-summarizer-head-svc                    ClusterIP   None             <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   31s
# text-summarizer-raycluster-tb9zf-head-svc   ClusterIP   None             <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   108s
# text-summarizer-serve-svc                   ClusterIP   34.118.226.139   <none>        8000/TCP                                        31s

# Step 4.3: Forward the port of Serve.
kubectl port-forward svc/text-summarizer-serve-svc 8000
```

## Step 5: Send a request to the text summarizer model

```sh
# Step 5.1: Download `text_summarizer_req.py`
curl -LO https://raw.githubusercontent.com/ray-project/serve_config_examples/master/text_summarizer/text_summarizer_req.py

# Step 5.2: Send a request to the Summarizer model.
python text_summarizer_req.py
# Check printed to console
```

## Step 6: Delete your service

```sh
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.text-summarizer.yaml
```

## Step 7: Uninstall your KubeRay operator

Follow [this document](https://github.com/ray-project/kuberay/tree/master/helm-chart/kuberay-operator) to uninstall the latest stable KubeRay operator using the Helm repository.
