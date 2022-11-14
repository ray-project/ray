(serve-in-production-kubernetes)=

# Deploying on Kubernetes

This section should help you:

- understand how to install and use the [KubeRay] operator.
- understand how to deploy a Ray Serve application using a [RayService].
- understand how to monitor and update your application.

The recommended way to deploy Ray Serve is on Kubernetes, providing the best of both worlds: the user experience and scalable compute of Ray Serve and operational benefits of Kubernetes.
This also allows you to integrate with existing applications that may be running on Kubernetes.
The recommended practice when running on Kubernetes is to use the [RayService] controller that's provided as part of [KubeRay]. The RayService controller automatically handles important production requirements such as health checking, status reporting, failure recovery, and upgrades.

A [RayService] CR encapsulates a multi-node Ray Cluster and a Serve application that runs on top of it into a single Kubernetes manifest.
Deploying, upgrading, and getting the status of the application can be done using standard `kubectl` commands.
This section walks through how to deploy, monitor, and upgrade the [`FruitStand` example](serve-in-production-example) on Kubernetes.

:::{warning}
Although it's actively developed and maintained, [KubeRay] is still considered alpha, or experimental, so some APIs may be subject to change.
:::

(serve-installing-kuberay-operator)=
## Installing the KubeRay operator

This guide assumes that you have a running Kubernetes cluster and have `kubectl` configured to run commands on it.
See the [Kubernetes documentation](https://kubernetes.io/docs/setup/) or the [KubeRay quickstart guide](kuberay-quickstart) if you need help getting started. Make sure your Kubernetes cluster and Kubectl are both at version at least 1.19.

The first step is to install the `KubeRay` operator into your Kubernetes cluster.
This creates a pod that runs the `KubeRay` controller. The `KubeRay` controller manages resources based on the `RayService` CRs you create.

Install the operator using `kubectl apply` and check that the controller pod is running:
```console
$ kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v0.3.0&timeout=90s"
$ kubectl get deployments -n ray-system
NAME                READY   UP-TO-DATE   AVAILABLE   AGE
kuberay-operator    1/1     1            1           13s

$ kubectl get pods -n ray-system
NAME                                 READY   STATUS    RESTARTS   AGE
kuberay-operator-68c75b5d5f-m8xd7    1/1     Running   0          42s
```

For more details, see the [KubeRay quickstart guide](kuberay-quickstart).

(serve-deploy-app-on-kuberay)=
## Deploying a Serve application

Once the KubeRay controller is running, you can manage your Ray Serve application by creating and updating a `RayService` custom resource (CR).
`RayService` custom resources consist of the following:
- a `KubeRay` `RayCluster` config defining the cluster that the Serve application runs on.
- a Ray Serve [config](serve-in-production-config-file) defining the Serve application to run on the cluster.

:::{tip}
You can use the `--kubernetes-format`/`-k` flag with `serve build` to print the Serve config in a format that can be copy-pasted directly into your [Kubernetes config](serve-in-production-kubernetes). You can paste this config into the `RayService` CR.
:::

When the `RayService` is created, the `KubeRay` controller first creates a Ray cluster using the provided configuration.
Then, once the cluster is running, it deploys the Serve application to the cluster using the [REST API](serve-in-production-deploying).
The controller also creates a Kubernetes Service that can be used to route traffic to the Serve application.

Let's see this in action by deploying the [`FruitStand` example](serve-in-production-example).
The Serve config for the example is embedded into [this example `RayService` CR](https://github.com/ray-project/kuberay/blob/release-0.3/ray-operator/config/samples/ray_v1alpha1_rayservice.yaml).
To follow along, save this CR locally in a file named `ray_v1alpha1_rayservice.yaml`:

:::{note}
The example `RayService` uses very small resource requests because it's only for demonstration.
In production, you'll want to provide more resources to the cluster.
Learn more about how to configure KubeRay clusters [here](kuberay-config).
:::

```console
$ curl -o ray_v1alpha1_rayservice.yaml https://raw.githubusercontent.com/ray-project/kuberay/release-0.3/ray-operator/config/samples/ray_v1alpha1_rayservice.yaml
```

To deploy the example, we simply `kubectl apply` the CR.
This creates the underlying Ray cluster, consisting of a head and worker node pod (see [Ray Clusters Key Concepts](../../cluster/key-concepts.rst) for more details on Ray clusters), as well as the service that can be used to query our application:

```console
$ kubectl apply -f ray_v1alpha1_rayservice.yaml

$ kubectl get rayservices
NAME                AGE
rayservice-sample   7s

$ kubectl get pods
NAME                                                      READY   STATUS    RESTARTS   AGE
rayservice-sample-raycluster-qd2vl-worker-small-group-bxpp6   1/1     Running   0          24m
rayservice-sample-raycluster-qd2vl-head-45hj4             1/1     Running   0          24m

$ kubectl get services
NAME                                               TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                          AGE
kubernetes                                         ClusterIP   10.100.0.1       <none>        443/TCP                                          62d
# Services used internally by the KubeRay controller.
rayservice-sample-head-svc                         ClusterIP   10.100.34.24     <none>        6379/TCP,8265/TCP,10001/TCP,8000/TCP,52365/TCP   24m
rayservice-sample-raycluster-qd2vl-dashboard-svc   ClusterIP   10.100.109.177   <none>        52365/TCP                                        24m
rayservice-sample-raycluster-qd2vl-head-svc        ClusterIP   10.100.180.221   <none>        6379/TCP,8265/TCP,10001/TCP,8000/TCP,52365/TCP   24m
# The Serve service that we will use to send queries to the application.
rayservice-sample-serve-svc                        ClusterIP   10.100.39.92     <none>        8000/TCP                                         24m
```

Note that the `rayservice-sample-serve-svc` above is the one that can be used to send queries to the Serve application -- this will be used in the next section.

## Querying the application

Once the `RayService` is running, we can query it over HTTP using the service created by the KubeRay controller.
This service can be queried directly from inside the cluster, but to access it from your laptop you'll need to configure a [Kubernetes ingress](kuberay-networking) or use port forwarding as below:

```console
$ kubectl port-forward service/rayservice-sample-serve-svc 8000
$ curl -X POST -H 'Content-Type: application/json' localhost:8000 -d '["MANGO", 2]'
6
```

## Getting the status of the application

As the `RayService` is running, the `KubeRay` controller continually monitors it and writes relevant status updates to the CR.
You can view the status of the application using `kubectl describe`.
This includes the status of the cluster, events such as health check failures or restarts, and the application-level statuses reported by [`serve status`](serve-in-production-inspecting).

```console
$ kubectl get rayservices
NAME                AGE
rayservice-sample   7s

$ kubectl describe rayservice rayservice-sample
...
Status:
  Active Service Status:
    App Status:
      Last Update Time:  2022-08-16T20:52:41Z
      Status:            RUNNING
    Dashboard Status:
      Health Last Update Time:  2022-08-16T20:52:41Z
      Is Healthy:               true
      Last Update Time:         2022-08-16T20:52:41Z
    Ray Cluster Name:           rayservice-sample-raycluster-9ghjw
    Ray Cluster Status:
      Available Worker Replicas:  2
      Desired Worker Replicas:    1
      Endpoints:
        Client:             10001
        Dashboard:          8265
        Dashboard - Agent:  52365
        Gcs - Server:       6379
        Serve:              8000
      Last Update Time:     2022-08-16T20:51:14Z
      Max Worker Replicas:  5
      Min Worker Replicas:  1
      State:                ready
    Serve Deployment Statuses:
      Health Last Update Time:  2022-08-16T20:52:41Z
      Last Update Time:         2022-08-16T20:52:41Z
      Name:                     MangoStand
      Status:                   HEALTHY
      Health Last Update Time:  2022-08-16T20:52:41Z
      Last Update Time:         2022-08-16T20:52:41Z
      Name:                     OrangeStand
      Status:                   HEALTHY
      Health Last Update Time:  2022-08-16T20:52:41Z
      Last Update Time:         2022-08-16T20:52:41Z
      Name:                     PearStand
      Status:                   HEALTHY
      Health Last Update Time:  2022-08-16T20:52:41Z
      Last Update Time:         2022-08-16T20:52:41Z
      Name:                     FruitMarket
      Status:                   HEALTHY
      Health Last Update Time:  2022-08-16T20:52:41Z
      Last Update Time:         2022-08-16T20:52:41Z
      Name:                     DAGDriver
      Status:                   HEALTHY
  Pending Service Status:
    App Status:
    Dashboard Status:
    Ray Cluster Status:
  Service Status:  Running
Events:
  Type    Reason                       Age                     From                   Message
  ----    ------                       ----                    ----                   -------
  Normal  WaitForDashboard             5m44s (x2 over 5m44s)   rayservice-controller  Service "rayservice-sample-raycluster-9ghjw-dashboard-svc" not found
  Normal  WaitForServeDeploymentReady  4m37s (x17 over 5m42s)  rayservice-controller  Put "http://rayservice-sample-raycluster-9ghjw-dashboard-svc.default.svc.cluster.local:52365/api/serve/deployments/": context deadline exceeded (Client.Timeout exceeded while awaiting headers)
  Normal  WaitForServeDeploymentReady  4m35s (x6 over 5m38s)   rayservice-controller  Put "http://rayservice-sample-raycluster-9ghjw-dashboard-svc.default.svc.cluster.local:52365/api/serve/deployments/": dial tcp 10.121.3.243:52365: i/o timeout (Client.Timeout exceeded while awaiting headers)
  Normal  Running                      44s (x129 over 94s)     rayservice-controller  The Serve applicaton is now running and healthy.
```

## Updating the application

To update the `RayService`, modify the manifest and apply it use `kubectl apply`.
There are two types of updates that can occur:
- *Application-level updates*: when only the Serve config options are changed, the update is applied _in-place_ on the same Ray cluster. This enables [lightweight updates](serve-in-production-lightweight-update) such as scaling a deployment up or down or modifying autoscaling parameters.
- *Cluster-level updates*: when the `RayCluster` config options are changed, such as updating the container image for the cluster, it may result in a cluster-level update. In this case, a new cluster is started, and the application is deployed to it. Once the new cluster is ready, the Kubernetes service is updated to point to the new cluster and the previous cluster is terminated. There should not be any downtime for the application, but note that this requires the Kubernetes cluster to be large enough to schedule both Ray clusters.

### Example: Serve config update

In the `FruitStand` example above, let's change the price of a mango in the Serve config to 4:

```console
  - name: MangoStand
    numReplicas: 1
    userConfig: |
      price: 4
```

Now to update the application we apply the modified manifest:

```console
$ kubectl apply -f ray_v1alpha1_rayservice.yaml

$ kubectl describe rayservice rayservice-sample
...
  serveDeploymentStatuses:
  - healthLastUpdateTime: "2022-07-18T21:51:37Z"
    lastUpdateTime: "2022-07-18T21:51:41Z"
    name: MangoStand
    status: UPDATING
...
```

If we query the application, we can see that we now get a different result reflecting the updated price:

```console
$ curl -X POST -H 'Content-Type: application/json' localhost:8000 -d '["MANGO", 2]'
8
```

### Updating the RayCluster config

The process of updating the RayCluster config is the same as updating the Serve config.
For example, we can update the number of worker nodes to 2 in the manifest:

```console
workerGroupSpecs:
  # the number of pods in the worker group.
  - replicas: 2
```

```console
$ kubectl apply -f ray_v1alpha1_rayservice.yaml

$ kubectl describe rayservice rayservice-sample
...
  pendingServiceStatus:
    appStatus: {}
    dashboardStatus:
      healthLastUpdateTime: "2022-07-18T21:54:53Z"
      lastUpdateTime: "2022-07-18T21:54:54Z"
    rayClusterName: rayservice-sample-raycluster-bshfr
    rayClusterStatus: {}
...
```

In the status, you can see that the `RayService` is preparing a pending cluster.
After the pending cluster is healthy, it becomes the active cluster and the previous cluster is terminated.

## Next Steps

Check out [the end-to-end fault tolerance guide](serve-e2e-ft) to learn more about Serve's failure conditions and how to guard against them.

[KubeRay]: https://ray-project.github.io/kuberay/
[RayService]: https://ray-project.github.io/kuberay/guidance/rayservice/
