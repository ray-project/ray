(serve-in-production-kubernetes)=

# Deploy on Kubernetes

This section should help you:

- understand how to install and use the [KubeRay] operator.
- understand how to deploy a Ray Serve application using a [RayService].
- understand how to monitor and update your application.

Deploying Ray Serve on Kubernetes provides the scalable compute of Ray Serve and operational benefits of Kubernetes.
This combination also allows you to integrate with existing applications that may be running on Kubernetes. When running on Kubernetes, use the [RayService] controller from [KubeRay].

> NOTE: [Anyscale](https://www.anyscale.com/get-started) is a managed Ray solution that provides high-availability, high-performance autoscaling, multi-cloud clusters, spot instance support, and more out of the box.

A [RayService] CR encapsulates a multi-node Ray Cluster and a Serve application that runs on top of it into a single Kubernetes manifest.
Deploying, upgrading, and getting the status of the application can be done using standard `kubectl` commands.
This section walks through how to deploy, monitor, and upgrade the [Text ML example](serve-in-production-example) on Kubernetes.

(serve-installing-kuberay-operator)=

## Installing the KubeRay operator

Follow the [KubeRay quickstart guide](kuberay-quickstart) to:
* Install `kubectl` and `Helm`
* Prepare a Kubernetes cluster
* Deploy a KubeRay operator

## Setting up a RayService custom resource (CR)
Once the KubeRay controller is running, manage your Ray Serve application by creating and updating a `RayService` CR ([example](https://github.com/ray-project/kuberay/blob/5b1a5a11f5df76db2d66ed332ff0802dc3bbff76/ray-operator/config/samples/ray-service.text-ml.yaml)).

Under the `spec` section in the `RayService` CR, set the following fields:

**`serveConfigV2`**: Represents the configuration that Ray Serve uses to deploy the application. Using `serve build` to print the Serve configuration and copy-paste it directly into your [Kubernetes config](serve-in-production-kubernetes) and `RayService` CR.

**`rayClusterConfig`**: Populate this field with the contents of the `spec` field from the `RayCluster` CR YAML file. Refer to [KubeRay configuration](kuberay-config) for more details.

:::{tip}
To enhance the reliability of your application, particularly when dealing with large dependencies that may require a significant amount of time to download, consider including the dependencies in your image's Dockerfile, so the dependencies are available as soon as the pods start.
:::

(serve-deploy-app-on-kuberay)=
## Deploying a Serve application

When the `RayService` is created, the `KubeRay` controller first creates a Ray cluster using the provided configuration.
Then, once the cluster is running, it deploys the Serve application to the cluster using the [REST API](serve-in-production-deploying).
The controller also creates a Kubernetes Service that can be used to route traffic to the Serve application.

To see an example, deploy the [Text ML example](serve-in-production-example).
The Serve config for the example is embedded into [this sample `RayService` CR](https://github.com/ray-project/kuberay/blob/5b1a5a11f5df76db2d66ed332ff0802dc3bbff76/ray-operator/config/samples/ray-service.text-ml.yaml).
Save this CR locally to a file named `ray-service.text-ml.yaml`:

:::{note}
- The example `RayService` uses very low `numCpus` values for demonstration purposes. In production, provide more resources to the Serve application.
Learn more about how to configure KubeRay clusters [here](kuberay-config).
- If you have dependencies that must be installed during deployment, you can add them to the `runtime_env` in the Deployment code. Learn more [here](serve-handling-dependencies)
:::

```console
$ curl -o ray-service.text-ml.yaml https://raw.githubusercontent.com/ray-project/kuberay/5b1a5a11f5df76db2d66ed332ff0802dc3bbff76/ray-operator/config/samples/ray-service.text-ml.yaml
```

To deploy the example, we simply `kubectl apply` the CR.
This creates the underlying Ray cluster, consisting of a head and worker node pod (see [Ray Clusters Key Concepts](../../cluster/key-concepts.rst) for more details on Ray clusters), as well as the service that can be used to query our application:

```console
$ kubectl apply -f ray-service.text-ml.yaml

$ kubectl get rayservices
NAME                AGE
rayservice-sample   7s

$ kubectl get pods
NAME                                                      READY   STATUS    RESTARTS   AGE
ervice-sample-raycluster-454c4-worker-small-group-b6mmg   1/1     Running   0          XXs
kuberay-operator-7fbdbf8c89-4lrnr                         1/1     Running   0          XXs
rayservice-sample-raycluster-454c4-head-krk9d             1/1     Running   0          XXs

$ kubectl get services

rayservice-sample-head-svc                         ClusterIP   ...        8080/TCP,6379/TCP,8265/TCP,10001/TCP,8000/TCP,52365/TCP   XXs
rayservice-sample-raycluster-454c4-dashboard-svc   ClusterIP   ...        52365/TCP                                                 XXs
rayservice-sample-raycluster-454c4-head-svc        ClusterIP   ...        8000/TCP,52365/TCP,8080/TCP,6379/TCP,8265/TCP,10001/TCP   XXs
rayservice-sample-serve-svc                        ClusterIP   ...        8000/TCP                                                  XXs
```

Note that the `rayservice-sample-serve-svc` above is the one that can be used to send queries to the Serve application -- this will be used in the next section.

## Querying the application

Once the `RayService` is running, we can query it over HTTP using the service created by the KubeRay controller.
This service can be queried directly from inside the cluster, but to access it from your laptop you'll need to configure a [Kubernetes ingress](kuberay-networking) or use port forwarding as below:

```console
$ kubectl port-forward service/rayservice-sample-serve-svc 8000
$ curl -X POST -H "Content-Type: application/json" localhost:8000/summarize_translate -d '"It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness, it was the epoch of belief"'
c'était le meilleur des temps, c'était le pire des temps .
```

(serve-getting-status-kubernetes)=
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
    Application Statuses:
      text_ml_app:
        Health Last Update Time:  2023-09-07T01:21:30Z
        Last Update Time:         2023-09-07T01:21:30Z
        Serve Deployment Statuses:
          text_ml_app_Summarizer:
            Health Last Update Time:  2023-09-07T01:21:30Z
            Last Update Time:         2023-09-07T01:21:30Z
            Status:                   HEALTHY
          text_ml_app_Translator:
            Health Last Update Time:  2023-09-07T01:21:30Z
            Last Update Time:         2023-09-07T01:21:30Z
            Status:                   HEALTHY
        Status:                       RUNNING
    Dashboard Status:
      Health Last Update Time:  2023-09-07T01:21:30Z
      Is Healthy:               true
      Last Update Time:         2023-09-07T01:21:30Z
    Ray Cluster Name:           rayservice-sample-raycluster-kkd2p
    Ray Cluster Status:
      Head:
  Observed Generation:  1
  Pending Service Status:
    Dashboard Status:
    Ray Cluster Status:
      Head:
  Service Status:  Running
Events:
  Type    Reason   Age                      From                   Message
  ----    ------   ----                     ----                   -------
  Normal  Running  2m15s (x29791 over 16h)  rayservice-controller  The Serve applicaton is now running and healthy.
```

## Updating the application

To update the `RayService`, modify the manifest and apply it use `kubectl apply`.
There are two types of updates that can occur:
- *Application-level updates*: when only the Serve config options are changed, the update is applied _in-place_ on the same Ray cluster. This enables [lightweight updates](serve-in-production-lightweight-update) such as scaling a deployment up or down or modifying autoscaling parameters.
- *Cluster-level updates*: when the `RayCluster` config options are changed, such as updating the container image for the cluster, it may result in a cluster-level update. In this case, a new cluster is started, and the application is deployed to it. Once the new cluster is ready, the Kubernetes service is updated to point to the new cluster and the previous cluster is terminated. There should not be any downtime for the application, but note that this requires the Kubernetes cluster to be large enough to schedule both Ray clusters.

### Example: Serve config update

In the Text ML example above, change the language of the Translator in the Serve config to German:

```yaml
  - name: Translator
    num_replicas: 1
    user_config:
      language: german
```

Now to update the application we apply the modified manifest:

```console
$ kubectl apply -f ray-service.text-ml.yaml

$ kubectl describe rayservice rayservice-sample
...
  Serve Deployment Statuses:
    text_ml_app_Translator:
      Health Last Update Time:  2023-09-07T18:21:36Z
      Last Update Time:         2023-09-07T18:21:36Z
      Status:                   UPDATING
...
```

Query the application to see a different translation in German:

```console
$ curl -X POST -H "Content-Type: application/json" localhost:8000/summarize_translate -d '"It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness, it was the epoch of belief"'
Es war die beste Zeit, es war die schlimmste Zeit .
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
$ kubectl apply -f ray-service.text-ml.yaml

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

## Autoscaling
You can configure autoscaling for your Serve application by setting the autoscaling field in the Serve config. Learn more about the configuration options in the [Serve Autoscaling Guide](serve-autoscaling).

To enable autoscaling in a KubeRay Cluster, you need to set `enableInTreeAutoscaling` to True. Additionally, there are other options available to configure the autoscaling behavior. For further details, please refer to the documentation [here](serve-autoscaling).


:::{note}
In most use cases, it is recommended to enable Kubernetes autoscaling to fully utilize the resources in your cluster. If you are using GKE, you can utilize the AutoPilot Kubernetes cluster. For instructions, see [Create an Autopilot Cluster](https://cloud.google.com/kubernetes-engine/docs/how-to/creating-an-autopilot-cluster). For EKS, you can enable Kubernetes cluster autoscaling by utilizing the Cluster Autoscaler. For detailed information, see [Cluster Autoscaler on AWS](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/aws/README.md). To understand the relationship between Kubernetes autoscaling and Ray autoscaling, see [Ray Autoscaler with Kubernetes Cluster Autoscaler](kuberay-autoscaler-with-ray-autoscaler).
:::

## Load balancer
Set up ingress to expose your Serve application with a load balancer. See [this configuration](https://github.com/ray-project/kuberay/blob/v1.0.0/ray-operator/config/samples/ray-service-alb-ingress.yaml)

:::{note}
- Ray Serve runs HTTP proxy on every node, allowing you to use `/-/routes` as the endpoint for node health checks.
- Ray Serve uses port 8000 as the default HTTP proxy traffic port. You can change the port by setting `http_options` in the Serve config. Learn more details [here](serve-multi-application).
:::

## Monitoring
Monitor your Serve application using the Ray Dashboard.
- Learn more about how to configure and manage Dashboard [here](observability-configure-manage-dashboard).
- Learn about the Ray Serve Dashboard [here](serve-monitoring).
- Learn how to set up [Prometheus](prometheus-setup) and [Grafana](grafana) for Dashboard.
- Learn about the [Ray Serve logs](serve-logging) and how to [persistent logs](kuberay-logging) on Kubernetes.

:::{note}
- To troubleshoot application deployment failures in Serve, you can check the KubeRay operator logs by running `kubectl logs -f <kuberay-operator-pod-name>` (e.g., `kubectl logs -f kuberay-operator-7447d85d58-lv7pf`). The KubeRay operator logs contain information about the Serve application deployment event and Serve application health checks.
- You can also check the controller log and deployment log, which are located under `/tmp/ray/session_latest/logs/serve/` in both the head node pod and worker node pod. These logs contain information about specific deployment failure reasons and autoscaling events.
:::

## Next Steps

See [Add End-to-End Fault Tolerance](serve-e2e-ft) to learn more about Serve's failure conditions and how to guard against them.

[KubeRay]: kuberay-quickstart
[RayService]: kuberay-rayservice-quickstart
