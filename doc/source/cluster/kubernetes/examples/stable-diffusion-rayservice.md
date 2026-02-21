(kuberay-stable-diffusion-rayservice-example)=

# Serve a StableDiffusion text-to-image model on Kubernetes

> **Note:** The Python files for the Ray Serve application and its client are in the [ray-project/serve_config_examples](https://github.com/ray-project/serve_config_examples) repository
and [the Ray documentation](https://docs.ray.io/en/latest/serve/tutorials/stable-diffusion.html).

## Step 1: Create a Kubernetes cluster with GPUs

See [aws-eks-gpu-cluster.md](kuberay-eks-gpu-cluster-setup) or [gcp-gke-gpu-cluster.md](kuberay-gke-gpu-cluster-setup) or [ack-gpu-cluster.md](kuberay-ack-gpu-cluster-setup) to create a Kubernetes cluster with 1 CPU node and 1 GPU node.

## Step 2: Install KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator using the Helm repository.
Note that the YAML file in this example uses `serveConfigV2`. This feature requires KubeRay v0.6.0 or later.

## Step 3: Install a RayService

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.stable-diffusion.yaml
```

This RayService configuration contains some important settings:

* In the RayService, the head Pod doesn't have any `tolerations`. Meanwhile, the worker Pods use the following `tolerations` so the scheduler won't assign the head Pod to the GPU node.
    ```yaml
    # Please add the following taints to the GPU node.
    tolerations:
        - key: "ray.io/node-type"
        operator: "Equal"
        value: "worker"
        effect: "NoSchedule"
    ```
* It includes `diffusers` in `runtime_env` since this package isn't included by default in the `ray-ml` image.

## Step 4: Forward the port of Serve

First get the service name from this command.

```sh
kubectl get services
```

Then, port forward to the serve.

```sh
# Wait until the RayService `Ready` condition is `True`. This means the RayService is ready to serve.
kubectl describe rayservices.ray.io stable-diffusion

# [Example output]
#   Conditions:
#     Last Transition Time:  2025-02-13T07:10:34Z
#     Message:               Number of serve endpoints is greater than 0
#     Observed Generation:   1
#     Reason:                NonZeroServeEndpoints
#     Status:                True
#     Type:                  Ready

# Forward the port of Serve
kubectl port-forward svc/stable-diffusion-serve-svc 8000
```

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
