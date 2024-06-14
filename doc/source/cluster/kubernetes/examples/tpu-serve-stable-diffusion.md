(kuberay-tpu-stable-diffusion-example)=

# Serve a Stable Diffusion model on GKE with TPUs

> **Note:** The Python files for the Ray Serve application and its client are in the [ray-project/serve_config_examples](https://github.com/ray-project/serve_config_examples) repo under `/stable_diffusion/tpu`. This example is adapted from the [tensorflow/tpu](https://github.com/tensorflow/tpu/tree/master/tools/ray_tpu/src/serve) Cloud TPU example.

## Step 1: Create a Kubernetes cluster with TPUs

Follow [Creating a GKE Cluster with TPUs for KubeRay](kuberay-gke-tpu-cluster-setup) to create a GKE Autopilot or Standard cluster with 1 CPU node and 2 TPU nodes.

## Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator via Helm repository. Multi-host TPU support is provided in Kuberay v1.1.0+. Please note that the YAML file in this example uses `serveConfigV2`, which is supported starting from KubeRay v0.6.0.

## Step 3: Create a RayCluster with a TPU worker group

```sh
# Creates a RayCluster with a single-host v4 TPU worker group of 2x2x1 topology
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.1.1/ray-operator/config/samples/ray-cluster.tpu-v4-singlehost.yaml

# Creates a RayCluster with a multi-host v4 TPU worker group of 2x2x2 topology
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.1.1/ray-operator/config/samples/ray-cluster.tpu-v4-multihost.yaml
```

Kuberay operator v1.1.0 adds a new `NumOfHosts` field to the RayCluster CR, supporting multi-host worker groups. This field specifies the number of workers to create per replica, with each replica representing a multi-host PodSlice. The value for `NumOfHosts` should match the number of TPU VM hosts expected by the given `cloud.google.com/gke-tpu-topology` node selector.

## Step 4: Run the Serve deployment with ray job submit

Download the Ray Serve and API client Python files:

```sh
git clone https://github.com/ray-project/serve_config_examples.git

cd stable_diffusion/tpu
```

Retrieve the name of the RayCluster head svc.

```sh
kubectl get pods
```

Then, port-forward the Ray dashboard. To view the dashboard, open http://localhost:8265/ on your local machine.

```sh
kubectl port-forward svc/example-cluster-kuberay-head-svc 8265:8265 &
```

In a separate terminal, submit the serve deployment using ray job.

```sh
ray job submit --runtime-env runtime_env.yaml -- python3 ray_serve_diffusion_flax.py
```

You may now monitor the status of the Ray Job in the Ray dashboard, and view the running serve deployment from
the 'Serve' tab.


## Step 5: Send text-to-image prompts to the model server

Port forward the Ray Serve endpoint:
```sh
kubectl port-forward svc/example-cluster-kuberay-head-svc 8000:8000 &
```

In a separate terminal, submit the prompt script using ray job submit:

```sh
ray job submit --runtime-env runtime_env.yaml -- python3 fake_load_test.py
```

* The output of the Ray job can be viewed from http://localhost:8000/, or saved by passing the flag --save_pictures.
