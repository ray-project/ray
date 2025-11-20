(kuberay-mobilenet-rayservice-example)=

# Serve a MobileNet image classifier on Kubernetes

> **Note:** The Python files for the Ray Serve application and its client are in the repository [ray-project/serve_config_examples](https://github.com/ray-project/serve_config_examples).

## Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: Install KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.
Note that the YAML file in this example uses `serveConfigV2`. You need KubeRay version v0.6.0 or later to use this feature.

## Step 3: Install a RayService

```sh
# Create a RayService
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-service.mobilenet.yaml
```

* The [mobilenet.py](https://github.com/ray-project/serve_config_examples/blob/master/mobilenet/mobilenet.py) file needs `tensorflow` as a dependency. Hence, the YAML file uses `rayproject/ray-ml` image instead of `rayproject/ray` image.
* The request parsing function `starlette.requests.form()` needs `python-multipart`, so the YAML file includes `python-multipart` in the runtime environment.

## Step 4: Forward the port for Ray Serve

```sh
# Wait for the RayService to be ready to serve requests
kubectl describe rayservice/rayservice-mobilenet
#  Conditions:
#   Last Transition Time:  2025-02-13T02:29:26Z
#   Message:               Number of serve endpoints is greater than 0
#   Observed Generation:   1
#   Reason:                NonZeroServeEndpoints
#   Status:                True
#   Type:                  Ready

# Forward the port for Ray Serve service
kubectl port-forward svc/rayservice-mobilenet-serve-svc 8000
```

## Step 5: Send a request to the ImageClassifier

* Step 5.1: Prepare an image file.
* Step 5.2: Update `image_path` in [mobilenet_req.py](https://github.com/ray-project/serve_config_examples/blob/master/mobilenet/mobilenet_req.py)
* Step 5.3: Send a request to the `ImageClassifier`.
  ```sh
  python mobilenet_req.py
  # sample output: {"prediction":["n02099601","golden_retriever",0.17944198846817017]}
  ```
