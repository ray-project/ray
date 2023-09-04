(kuberay-stable-diffusion-rayservice-example)=

# Serve a StableDiffusion text-to-image model on Kubernetes

> **Note:** The Python files for the Ray Serve application and its client are in the [ray-project/serve_config_examples](https://github.com/ray-project/serve_config_examples) repo 
and [the Ray documentation](https://docs.ray.io/en/latest/serve/tutorials/stable-diffusion.html).

## Step 1: Create a Kubernetes cluster with GPUs

Follow [aws-eks-gpu-cluster.md](kuberay-eks-gpu-cluster-setup) or [gcp-gke-gpu-cluster.md](kuberay-gke-gpu-cluster-setup) to create a Kubernetes cluster with 1 CPU node and 1 GPU node.

## Step 2: Install KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator via Helm repository.
Please note that the YAML file in this example uses `serveConfigV2`, which is supported starting from KubeRay v0.6.0.

## Step 3: Install a RayService

```sh
# Step 3.1: Download `ray-service.stable-diffusion.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.stable-diffusion.yaml

# Step 3.2: Create a RayService
kubectl apply -f ray-service.stable-diffusion.yaml
```

This RayService configuration contains some important settings:

* The `tolerations` for workers allow them to be scheduled on nodes without any taints or on nodes with specific taints. However, workers will only be scheduled on GPU nodes because we set `nvidia.com/gpu: 1` in the Pod's resource configurations.
    ```yaml
    # Please add the following taints to the GPU node.
    tolerations:
        - key: "ray.io/node-type"
        operator: "Equal"
        value: "worker"
        effect: "NoSchedule"
    ```
* It includes `diffusers` in `runtime_env` since this package is not included by default in the `ray-ml` image.

## Step 4: Forward the port of Serve

First get the service name from this command.

```sh
kubectl get services
```

Then, port forward to the serve.

```sh
kubectl port-forward svc/stable-diffusion-serve-svc 8000
```

Note that the RayService's Kubernetes service will be created after the Serve applications are ready and running. This process may take approximately 1 minute after all Pods in the RayCluster are running.

## Step 5: Send a request to the text-to-image model

```sh
# Step 5.1: Download `stable_diffusion_req.py` 
curl -LO https://raw.githubusercontent.com/ray-project/serve_config_examples/master/stable_diffusion/stable_diffusion_req.py

# Step 5.2: Set your `prompt` in `stable_diffusion_req.py`.

# Step 5.3: Send a request to the Stable Diffusion model.
python stable_diffusion_req.py
# Check output.png
```

* You can refer to the document ["Serving a Stable Diffusion Model"](https://docs.ray.io/en/latest/serve/tutorials/stable-diffusion.html) for an example output image.