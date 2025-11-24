(kuberay-scheduler-plugins)=
# KubeRay integration with scheduler plugins

The [kubernetes-sigs/scheduler-plugins](https://github.com/kubernetes-sigs/scheduler-plugins) repository provides out-of-tree scheduler plugins based on the scheduler framework.

Starting with KubeRay v1.4.0, KubeRay integrates with the [PodGroup API](https://github.com/kubernetes-sigs/scheduler-plugins/blob/93126eabdf526010bf697d5963d849eab7e8e898/site/content/en/docs/plugins/coscheduling.md) provided by scheduler plugins to support gang scheduling for RayCluster custom resources.

## Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: Install scheduler plugins

Follow the [installation guide](https://scheduler-plugins.sigs.k8s.io/docs/user-guide/installation/) in the scheduler-plugins repository to install the scheduler plugins.

:::{note}

There are two modes for installing the scheduler plugins: *single scheduler mode* and *second scheduler mode*.

KubeRay v1.4.0 only supports the *single scheduler mode*.
You need to have the access to configure Kubernetes control plane to replace the default scheduler with the scheduler plugins.

:::

## Step 3: Install KubeRay operator with scheduler plugins enabled

KubeRay v1.4.0 and later versions support scheduler plugins.

```sh
helm install kuberay-operator kuberay/kuberay-operator --version 1.5.0 --set batchScheduler.name=scheduler-plugins
```

## Step 4: Deploy a RayCluster with gang scheduling

```sh
# Configure the RayCluster with label `ray.io/gang-scheduling-enabled: "true"`
# to enable gang scheduling.
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/release-1.4/ray-operator/config/samples/ray-cluster.scheduler-plugins.yaml
```

## Step 5: Verify Ray Pods and PodGroup

Note that if you use "second scheduler mode," which KubeRay currently doesn't support, the following commands still show similar results.
However, the Ray Pods don't get scheduled in a gang scheduling manner.
Make sure to use "single scheduler mode" to enable gang scheduling.

```sh
kubectl get podgroups.scheduling.x-k8s.io
# NAME              PHASE     MINMEMBER   RUNNING   SUCCEEDED   FAILED   AGE
# test-podgroup-0   Running   3           3                              2m25s

# All Ray Pods (1 head and 2 workers) belong to the same PodGroup.
kubectl get pods -L scheduling.x-k8s.io/pod-group
# NAME                                  READY   STATUS    RESTARTS   AGE     POD-GROUP
# test-podgroup-0-head                  1/1     Running   0          3m30s   test-podgroup-0
# test-podgroup-0-worker-worker-4vc6j   1/1     Running   0          3m30s   test-podgroup-0
# test-podgroup-0-worker-worker-ntm9f   1/1     Running   0          3m30s   test-podgroup-0
```