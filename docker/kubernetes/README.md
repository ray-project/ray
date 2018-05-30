# Deploy ray with Helm

Create a `ray` namespace:

```console
$ kubectl create ns ray-system
```

Note that you could install ray in the default namespace or any other namespace.

Install ray with [helm](https://github.com/kubernetes/helm)

```console
helm init
helm install --name ray --namespace ray-system ./ray
```