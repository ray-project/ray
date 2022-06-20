# Deploying with KubeRay (experimental)

```{admonition} What is Kuberay?
[KubeRay](https://github.com/ray-project/kuberay) is a set of tools for running Ray on Kubernetes.
It has been used by some larger corporations to deploy Ray on their infrastructure.
Going forward, we would like to make this way of deployment accessible and seamless for
all Ray users and standardize Ray deployment on Kubernetes around KubeRay's operator.
Presently you should consider this integration a minimal viable product that is not polished
enough for general use and prefer the [Kubernetes integration](kubernetes.rst) for running
Ray on Kubernetes. If you are brave enough to try the KubeRay integration out, this documentation
is for you! We would love your feedback as a [Github issue](https://github.com/ray-project/ray/issues)
including `[KubeRay]` in the title.
```

Here we describe how you can deploy a Ray cluster on KubeRay. The following instructions are for
Minikube but the deployment works the same way on a real Kubernetes cluster. You need to have at
least 4 CPUs to run this example. First we make sure Minikube is initialized with

```shell
minikube start
```

Now you can deploy the KubeRay operator using

```shell
./ray/python/ray/autoscaler/kuberay/init-config.sh
kubectl create -k "ray/python/ray/autoscaler/kuberay/config/default"
```

You can verify that the operator has been deployed using

```shell
kubectl -n ray-system get pods
```

Now let's deploy a new Ray cluster:

```shell
kubectl create -f ray/python/ray/autoscaler/kuberay/ray-cluster.complete.yaml
```

## Using the autoscaler

Let's now try out the autoscaler. We can run the following command to get a
Python interpreter in the head pod:

```shell
kubectl exec `kubectl get pods -o custom-columns=POD:metadata.name | grep raycluster-complete-head` -it -c ray-head -- python
```

In the Python interpreter, run the following snippet to scale up the cluster:

```python
import ray.autoscaler.sdk
ray.init("auto")
ray.autoscaler.sdk.request_resources(num_cpus=4)
```

> **_NOTE:_**  The example config ray-cluster.complete.yaml specifies rayproject/ray:8c5fe4
> as the Ray autoscaler image. This image carries the latest improvements to KubeRay autoscaling
> support. This autoscaler image is confirmed to be compatible with Ray versions >= 1.11.0.
> Once Ray autoscaler support is stable, the recommended pattern will be to use the same
> Ray version in the autoscaler and Ray containers.

## Uninstalling the KubeRay operator

You can uninstall the KubeRay operator using
```shell
kubectl delete -f "ray/python/ray/autoscaler/kuberay/kuberay-autoscaler-rbac.yaml"
kubectl delete -k "ray/python/ray/autoscaler/kuberay/config/default"
```

Note that all running Ray clusters will automatically be terminated.

## Developing the KubeRay integration (advanced)

### Developing the KubeRay operator
If you also want to change the underlying KubeRay operator, please refer to the instructions
in [the KubeRay development documentation](https://github.com/ray-project/kuberay/blob/master/ray-operator/DEVELOPMENT.md). In that case you should push the modified operator to your docker account or registry and
follow the instructions in `ray/python/ray/autoscaler/kuberay/init-config.sh`.

### Developing the Ray autoscaler code
Code for the Ray autoscaler's KubeRay integration is located in `ray/python/ray/autoscaler/_private/kuberay`.

Here is one procedure to test development autoscaler code.
1. Push autoscaler code changes to your fork of Ray.
2. Use the following Dockerfile to build an image with your changes.
```dockerfile
# Use the latest Ray master as base.
FROM rayproject/ray:nightly
# Invalidate the cache so that fresh code is pulled in the next step.
ARG BUILD_DATE
# Retrieve your development code.
RUN git clone -b <my-dev-branch> https://github.com/<my-git-handle>/ray
# Install symlinks to your modified Python code.
RUN python ray/python/ray/setup-dev.py -y
```
3. Push the image to your docker account or registry. Assuming your Dockerfile is named "Dockerfile":
```shell
docker build --build-arg BUILD_DATE=$(date +%Y-%m-%d:%H:%M:%S) -t <registry>/<repo>:<tag> - < Dockerfile
docker push <registry>/<repo>:<tag>
```
4. Update the autoscaler image in `ray-cluster.complete.yaml`

Refer to the [Ray development documentation](https://docs.ray.io/en/latest/development.html#building-ray-python-only) for
further details.
