(kuberay-uv)=
# Run RayCluster with custom image using uv.

This guide is the basic setting about how to build a custom image with uv as a package manager. For best practices, please reference https://github.com/astral-sh/uv-docker-example.

The following example uses `kind` and `helm` to demonstrate deploying to kubernetes.

## Prepare custom image
Create `pyproject.toml`:
```
[project]
name = "hello_ray"
version = "0.1"
requires-python = ">=3.12"
dependencies = [
  "ray[default]",
  "emoji"
]
```

Prepare python script `hello_ray.py`:
```python
import emoji
import ray

@ray.remote
def f():
    return emoji.emojize('Python is :thumbs_up:')
# Execute 10 copies of f across a cluster.

print(ray.get([f.remote() for _ in range(10)]))
```

Prepare `Dockerfile`:
```dockerfile
FROM python:3.12-slim AS builder

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.7.3 /uv /uvx /bin/

# Install the project's dependencies.
COPY pyproject.toml .
RUN uv sync

FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.7.3 /uv /uvx /bin/

# Copy the dependencies from builder
COPY --from=builder /app/.venv /app/.venv

# install wget for liveness and readiness probe.
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/*

# Add ray binary into PATH for starting command
ENV PATH="/app/.venv/bin:$PATH"

# Add the project source code.
WORKDIR /app
COPY hello_ray.py /app

ENTRYPOINT ["bash", "-c", "--"]
```

Build docker image:
```bash
docker build -t ray-uv:demo .
```

Create kind cluster and follow [KubeRay Operator Installation](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/kuberay-operator-installation.html) to install operator:
```bash
kind create cluster --image=kindest/node:v1.26.0

helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
# Install both CRDs and KubeRay operator v1.4.0.
helm install kuberay-operator kuberay/kuberay-operator --version 1.4.0
```

Load custom image into node:
```bash
kind load docker-image ray-uv:demo
```

Prepare RayCluster yaml file `ray-cluster.uv.yaml` with the following steps:
- replace the image under `headGroupSpec` and `workerGroupSpecs` with `image: ray-uv:demo` which is the image previously built.
- change `rayVersion` to align with the version in the custom image.
- (optional) add env `RAY_RUNTIME_ENV_HOOK=ray._private.runtime_env.uv_runtime_env_hook.hook` if you'd like to use `uv run --with` without dependencies pre-built on the image. reference [uv + Ray: Pain-Free Python Dependencies in Clusters](https://www.anyscale.com/blog/uv-ray-pain-free-python-dependencies-in-clusters) for the more detail.

<details>
  <summary>example: `ray-cluster.uv.yaml`</summary>

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-uv
spec:
  rayVersion: '2.46.0' # should match the Ray version in the image of the containers
  # Ray head Pod template
  headGroupSpec:
    rayStartParams: {}
    # Pod template
    template:
      spec:
        containers:
        - name: ray-head
          image: ray-uv:demo
#          env:
#            - name: RAY_RUNTIME_ENV_HOOK
#              value: ray._private.runtime_env.uv_runtime_env_hook.hook
          resources:
            limits:
              cpu: 1
              memory: 2Gi
            requests:
              cpu: 500m
              memory: 2Gi
          ports:
          - containerPort: 6379
            name: gcs-server
          - containerPort: 8265 # Ray dashboard
            name: dashboard
          - containerPort: 10001
            name: client
  workerGroupSpecs:
  - replicas: 1
    minReplicas: 1
    maxReplicas: 5
    groupName: small-group
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-worker
          image: ray-uv:demo
#          env:
#            - name: RAY_RUNTIME_ENV_HOOK
#              value: ray._private.runtime_env.uv_runtime_env_hook.hook
          resources:
            limits:
              cpu: "1"
              memory: "1G"
            requests:
              cpu: "500m"
              memory: "1G"
```
</details>

Apply RayCluster yaml modified from the previous step:
```bash
kubectl apply -f ray-cluster.uv.yaml
```

Wait for the deployed RayCluster become ready:
```bash
kubectl get rayclusters
```

Execute python script on the custom image or follow [Run an application on a RayCluster](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html#step-4-run-an-application-on-a-raycluster) to interact with the deployed RayCluster.
```bash
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -it $HEAD_POD -- uv run /app/hello_ray.py
```
