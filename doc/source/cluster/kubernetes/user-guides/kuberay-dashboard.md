# Use kubectl dashboard (experimental)

Starting from KubeRay v1.4.0, you can use the open source dashboard UI for KubeRay. This component is still experimental and not considered ready for production, but feedbacks are welcome.

KubeRay dashboard is a web-based UI that allows you to view and manage KubeRay resources running on your Kubernetes cluster. It is different from the Ray dashboard, which is a part of the Ray cluster itself. The KubeRay dashboard provides a centralized view of all KubeRay resources.

## Installation

KubeRay dashboard depends on the optional component `kuberay-apiserver`, so you need to install it first. For simplicity, let's disable the security proxy and allow all origins for CORS.

```bash
helm install kuberay-apiserver kuberay/kuberay-apiserver --version v1.4.0 --set security= --set cors.allowOrigin='*'
```

And you need to port-forward the `kuberay-apiserver` service:

```bash
kubectl port-forward svc/kuberay-apiserver-service 31888:8888
```

Install the KubeRay dashboard:

```bash
kubectl run kuberay-dashboard --image=quay.io/kuberay/dashboard:v1.4.0
```

Port-forward the KubeRay dashboard:

```bash
kubectl port-forward kuberay-dashboard 3000:3000
```

Go to `http://localhost:3000/ray/jobs` to see the list of Ray jobs.

![KubeRay Dashboard List of Rayjobs](./images/kuberay-dashboard-rayjobs.png)
