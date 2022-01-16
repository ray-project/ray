# Using Kuberay

Here we describe how you can deploy a Ray cluster on Kuberay. The following instructions are for
Minikube but the deployment works the same way on a real Kubernetes cluster. First we make sure
Minikube is initialized with

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
