# Deploying with Kuberay (experimental)

```{admonition} What is Kuberay?
[Kuberay](https://github.com/ray-project/kuberay) is a set of tools for running Ray on Kubernetes.
It has been used by some larger corporations to deploy Ray on their infrastructure.
Going forward, we would like to make this way of deployment accessible and seamless for
all Ray users and standardize Ray deployment on Kubernetes around Kuberay's operator.
Presently you should consider this integration a minimal viable product that is not polished
enough for general use and prefer the [Kubernetes integration](kubernetes.rst) for running
Ray on Kubernetes. If you are brave enough to try the Kuberay integration out, this documentation
is for you! We would love your feedback as a [Github issue](https://github.com/ray-project/ray/issues)
including `[Kuberay]` in the title.
```

Here we describe how you can deploy a Ray cluster on Kuberay. The following instructions are for
Minikube but the deployment works the same way on a real Kubernetes cluster. You need to have at
least 4 CPUs to run this example. First we make sure Minikube is initialized with

```shell
minikube start
```

Now you can deploy the Kuberay operator using

```shell
./ray/python/ray/autoscaler/kuberay/init-config.sh
kubectl apply -k "ray/python/ray/autoscaler/kuberay/config/default"
kubectl apply -f "ray/python/ray/autoscaler/kuberay/kuberay-autoscaler.yaml"
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

## Uninstalling the Kuberay operator

You can uninstall the Kuberay operator using
```shell
kubectl delete -f "ray/python/ray/autoscaler/kuberay/kuberay-autoscaler.yaml"
kubectl delete -k "ray/python/ray/autoscaler/kuberay/config/default"
```

Note that all running Ray clusters will automatically be terminated.

## Developing the Kuberay integration (advanced)

If you also want to change the underlying Kuberay operator, please refer to the instructions
in [the Kuberay development documentation](https://github.com/ray-project/kuberay/blob/master/ray-operator/DEVELOPMENT.md). In that case you should push the modified operator to your docker account or registry and
follow the instructions in `ray/python/ray/autoscaler/kuberay/init-config.sh`.

The remainder of the instructions will cover how to change the autoscaler code.

In order to maximize development iteration speed, we recommend using a Linux machine with Python 3.7 for
development, since that will simplify building wheels incrementally.
Make the desired modification to Ray and/or the autoscaler and build the Ray wheels by running
the following command in the `ray/python` directory:

```shell
python setup.py bdist_wheel
```

Then in the `ray/docker/kuberay-autoscaler` directory run:

```shell
cp ../../python/dist/ray-2.0.0.dev0-cp37-cp37m-linux_x86_64.whl ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl
docker build --build-arg WHEEL_PATH="ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl" -t rayproject/kuberay-autoscaler --no-cache .
docker push rayproject/kuberay-autoscaler
```

where you replace `rayproject/kuberay-autoscaler` with the desired image path in your own docker account (normally
`<username>/kuberay-autoscaler`). Please also make sure to update the image in `ray-cluster.complete.yaml`.
