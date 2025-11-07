(kuberay-rayservice)=

# Deploy Ray Serve Applications

## Prerequisites

This guide mainly focuses on the behavior of KubeRay v1.4.0 and Ray 2.46.0.

## What's a RayService?

A RayService manages two components:

* **RayCluster**: Manages resources in a Kubernetes cluster.
* **Ray Serve Applications**: Manages users' applications.

## What does the RayService provide?

* **Kubernetes-native support for Ray clusters and Ray Serve applications:** After using a Kubernetes configuration to define a Ray cluster and its Ray Serve applications, you can use `kubectl` to create the cluster and its applications.
* **In-place updates for Ray Serve applications:** Users can update the Ray Serve configuration in the RayService CR configuration and use `kubectl apply` to update the applications. See [Step 7](#step-7-in-place-update-for-ray-serve-applications) for more details.
* **Zero downtime upgrades for Ray clusters:** Users can update the Ray cluster configuration in the RayService CR configuration and use `kubectl apply` to update the cluster. RayService temporarily creates a pending cluster and waits for it to be ready, then switches traffic to the new cluster and terminates the old one. See [Step 8](#step-8-zero-downtime-upgrade-for-ray-clusters) for more details.
* **High-availabilable services:** See [RayService high availability](kuberay-rayservice-ha) for more details.

## Example: Serve two simple Ray Serve applications using RayService

## Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator using Helm repository.

## Step 3: Install a RayService

```sh
curl -O https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-service.sample.yaml
kubectl apply -f ray-service.sample.yaml
```

Look at the Ray Serve configuration `serveConfigV2` embedded in the RayService YAML. Notice two high-level applications: a fruit stand application and a calculator application. Take note of some details about the fruit stand application:
  * `import_path`: The path to import the Serve application. For `fruit_app`, [fruit.py](https://github.com/ray-project/test_dag/blob/master/fruit.py) defines the application in the `deployment_graph` variable.
  * `route_prefix`: See [Ray Serve API](serve-api) for more details.
  * `working_dir`: The working directory points to the [test_dag](https://github.com/ray-project/test_dag/) repository, which RayService downloads at runtime and uses to start your application. See {ref}`Runtime Environments <runtime-environments>`. for more details.
  * `deployments`: See [Ray Serve Documentation](https://docs.ray.io/en/master/serve/configure-serve-deployment.html).
  ```yaml
  serveConfigV2: |
    applications:
      - name: fruit_app
        import_path: fruit.deployment_graph
        route_prefix: /fruit
        runtime_env:
          working_dir: "https://github.com/ray-project/test_dag/archive/....zip"
        deployments: ...
      - name: math_app
        import_path: conditional_dag.serve_dag
        route_prefix: /calc
        runtime_env:
          working_dir: "https://github.com/ray-project/test_dag/archive/....zip"
        deployments: ...
  ```

## Step 4: Verify the Kubernetes cluster status

```sh
# Step 4.1: List all RayService custom resources in the `default` namespace.
kubectl get rayservice

# [Example output]
# NAME                SERVICE STATUS   NUM SERVE ENDPOINTS
# rayservice-sample   Running          1

# Step 4.2: List all RayCluster custom resources in the `default` namespace.
kubectl get raycluster

# [Example output]
# NAME                                 DESIRED WORKERS   AVAILABLE WORKERS   CPUS    MEMORY   GPUS   STATUS   AGE
# rayservice-sample-raycluster-fj2gp   1                 1                   2500m   4Gi      0      ready    75s

# Step 4.3: List all Ray Pods in the `default` namespace.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                                                          READY   STATUS    RESTARTS   AGE
# rayservice-sample-raycluster-fj2gp-head-6wwqp                 1/1     Running   0          93s
# rayservice-sample-raycluster-fj2gp-small-group-worker-hxrxc   1/1     Running   0          93s

# Step 4.4: Check whether the RayService is ready to serve requests.
kubectl describe rayservices.ray.io rayservice-sample

# [Example output]
# Conditions:
#   Last Transition Time:  2025-02-13T18:28:51Z
#   Message:               Number of serve endpoints is greater than 0
#   Observed Generation:   1
#   Reason:                NonZeroServeEndpoints
#   Status:                True <--- RayService is ready to serve requests
#   Type:                  Ready

# Step 4.5: List services in the `default` namespace.
kubectl get services

# NAME                                          TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                                   AGE
# ...
# rayservice-sample-head-svc                    ClusterIP   10.96.34.90     <none>        10001/TCP,8265/TCP,52365/TCP,6379/TCP,8080/TCP,8000/TCP   4m58s
# rayservice-sample-raycluster-6mj28-head-svc   ClusterIP   10.96.171.184   <none>        10001/TCP,8265/TCP,52365/TCP,6379/TCP,8080/TCP,8000/TCP   6m21s
# rayservice-sample-serve-svc                   ClusterIP   10.96.161.84    <none>        8000/TCP                                                  4m58s
```

KubeRay creates a RayCluster based on `spec.rayClusterConfig` defined in the RayService YAML for a RayService custom resource.
Next, once the head Pod is running and ready, KubeRay submits a request to the head's dashboard port to create the Ray Serve applications defined in `spec.serveConfigV2`.

Users can access the head Pod through both RayService’s head service `rayservice-sample-head-svc` and RayCluster’s head service `rayservice-sample-raycluster-xxxxx-head-svc`.

However, during a zero downtime upgrade, KubeRay creates a new RayCluster and a new head service `rayservice-sample-raycluster-yyyyy-head-svc` for the new RayCluster.

If you don't use `rayservice-sample-head-svc`, you need to update the ingress configuration to point to the new head service.
However, if you use `rayservice-sample-head-svc`, KubeRay automatically updates the selector to point to the new head Pod, eliminating the need to update the ingress configuration.


> Note: Default ports and their definitions.

| Port  | Definition          |
|-------|---------------------|
| 6379  | Ray GCS             |
| 8265  | Ray Dashboard       |
| 10001 | Ray Client          |
| 8000  | Ray Serve           |

## Step 5: Verify the status of the Serve applications

```sh
# Step 5.1: Check the status of the RayService.
kubectl describe rayservices rayservice-sample

# [Example output: Ray Serve application statuses]
# Status:
#   Active Service Status:
#     Application Statuses:
#       fruit_app:
#         Serve Deployment Statuses:
#           Fruit Market:
#             Status:  HEALTHY
#           ...
#         Status:      RUNNING
#       math_app:
#         Serve Deployment Statuses:
#           Adder:
#             Status:  HEALTHY
#           ...
#         Status:        RUNNING

# [Example output: RayService conditions]
# Conditions:
#   Last Transition Time:  2025-02-13T18:28:51Z
#   Message:               Number of serve endpoints is greater than 0
#   Observed Generation:   1
#   Reason:                NonZeroServeEndpoints
#   Status:                True
#   Type:                  Ready
#   Last Transition Time:  2025-02-13T18:28:00Z
#   Message:               Active Ray cluster exists and no pending Ray cluster
#   Observed Generation:   1
#   Reason:                NoPendingCluster
#   Status:                False
#   Type:                  UpgradeInProgress


# Step 5.2: Check the Serve applications in the Ray dashboard.
# (1) Forward the dashboard port to localhost.
# (2) Check the Serve page in the Ray dashboard at http://localhost:8265/#/serve.
kubectl port-forward svc/rayservice-sample-head-svc 8265:8265
```

* See [rayservice-troubleshooting.md](kuberay-raysvc-troubleshoot) for more details on RayService observability.
Below is a screenshot example of the Serve page in the Ray dashboard.
  ![Ray Serve Dashboard](../images/dashboard_serve.png)

## Step 6: Send requests to the Serve applications by the Kubernetes serve service

```sh
# Step 6.1: Run a curl Pod.
# If you already have a curl Pod, you can use `kubectl exec -it <curl-pod> -- sh` to access the Pod.
kubectl run curl --image=radial/busyboxplus:curl -i --tty

# Step 6.2: Send a request to the fruit stand app.
curl -X POST -H 'Content-Type: application/json' rayservice-sample-serve-svc:8000/fruit/ -d '["MANGO", 2]'
# [Expected output]: 6

# Step 6.3: Send a request to the calculator app.
curl -X POST -H 'Content-Type: application/json' rayservice-sample-serve-svc:8000/calc/ -d '["MUL", 3]'
# [Expected output]: "15 pizzas please!"
```

* `rayservice-sample-serve-svc` does traffic routing among all the workers which have Ray Serve replicas.

(step-7-in-place-update-for-ray-serve-applications)=
## Step 7: In-place update for Ray Serve applications

You can update the configurations for the applications by modifying `serveConfigV2` in the RayService configuration file. Reapplying the modified configuration with `kubectl apply` reapplies the new configurations to the existing RayCluster instead of creating a new RayCluster.

Update the price of Mango from `3` to `4` for the fruit stand app in [ray-service.sample.yaml](https://github.com/ray-project/kuberay/blob/v1.5.0/ray-operator/config/samples/ray-service.sample.yaml).
This change reconfigures the existing MangoStand deployment, and future requests are going to use the updated mango price.

```sh
# Step 7.1: Update the price of mangos from 3 to 4.
# [ray-service.sample.yaml]
# - name: MangoStand
#   num_replicas: 1
#   max_replicas_per_node: 1
#   user_config:
#     price: 4

# Step 7.2: Apply the updated RayService config.
kubectl apply -f ray-service.sample.yaml

# Step 7.3: Check the status of the RayService.
kubectl describe rayservices rayservice-sample
# [Example output]
# Serve Deployment Statuses:
#   Mango Stand:
#     Status: UPDATING

# Step 7.4: Send a request to the fruit stand app again after the Serve deployment status changes from UPDATING to HEALTHY.
# (Execute the command in the curl Pod from Step 6)
curl -X POST -H 'Content-Type: application/json' rayservice-sample-serve-svc:8000/fruit/ -d '["MANGO", 2]'
# [Expected output]: 8
```

(step-8-zero-downtime-upgrade-for-ray-clusters)=
## Step 8: Zero downtime upgrade for Ray clusters

In Step 7, modifying `serveConfigV2` doesn't trigger a zero downtime upgrade for Ray clusters.
Instead, it reapplies the new configurations to the existing RayCluster.
However, if you modify `spec.rayClusterConfig` in the RayService YAML file, it triggers a zero downtime upgrade for Ray clusters.
RayService temporarily creates a new RayCluster and waits for it to be ready, then switches traffic to the new RayCluster by updating the selector of the head service managed by RayService `rayservice-sample-head-svc` and terminates the old one.

During the zero downtime upgrade process, RayService creates a new RayCluster temporarily and waits for it to become ready.
Once the new RayCluster is ready, RayService updates the selector of the head service managed by RayService `rayservice-sample-head-svc` to point to the new RayCluster to switch the traffic to the new RayCluster.
Finally, KubeRay deletes the old RayCluster.

Certain exceptions don't trigger a zero downtime upgrade.
Only the fields managed by Ray autoscaler—`replicas`, `minReplicas`, `maxReplicas`, and `scaleStrategy.workersToDelete`—don't trigger a zero downtime upgrade.
When you update these fields, KubeRay doesn't propagate the update from RayService to RayCluster custom resources, so nothing happens.

```sh
# Step 8.1: Update `spec.rayClusterConfig.workerGroupSpecs[0].replicas` in the RayService YAML file from 1 to 2.
# This field is an exception that doesn't trigger a zero-downtime upgrade, and KubeRay doesn't update the
# RayCluster as a result. Therefore, no changes occur.
kubectl apply -f ray-service.sample.yaml

# Step 8.2: Check RayService CR
kubectl describe rayservices rayservice-sample
# Worker Group Specs:
#   ...
#   Replicas:  2

# Step 8.3: Check RayCluster CR. The update doesn't propagate to the RayCluster CR.
kubectl describe rayclusters $YOUR_RAY_CLUSTER
# Worker Group Specs:
#   ...
#   Replicas:  1

# Step 8.4: Update `spec.rayClusterConfig.rayVersion` to `2.100.0`.
# This field determines the Autoscaler sidecar image, and triggers a zero downtime upgrade.
kubectl apply -f ray-service.sample.yaml

# Step 8.5: List all RayCluster custom resources in the `default` namespace.
# Note that the new RayCluster is created based on the updated RayService config to have 2 workers.
kubectl get raycluster

# NAME                                 DESIRED WORKERS   AVAILABLE WORKERS   CPUS    MEMORY   GPUS   STATUS   AGE
# rayservice-sample-raycluster-fj2gp   1                 1                   2500m   4Gi      0      ready    40m
# rayservice-sample-raycluster-pddrb   2                 2                   3       6Gi      0               13s

# Step 8.6: Wait for the old RayCluster terminate.

# Step 8.7: Submit a request to the fruit stand app via the same serve service.
curl -X POST -H 'Content-Type: application/json' rayservice-sample-serve-svc:8000/fruit/ -d '["MANGO", 2]'
# [Expected output]: 8
```

## Step 9: Clean up the Kubernetes cluster

```sh
# Delete the RayService.
kubectl delete -f ray-service.sample.yaml

# Uninstall the KubeRay operator.
helm uninstall kuberay-operator

# Delete the curl Pod.
kubectl delete pod curl
```

## Next steps

* See [RayService high availability](kuberay-rayservice-ha) for more details on RayService HA.
* See [RayService troubleshooting guide](kuberay-raysvc-troubleshoot) if you encounter any issues.
* See [Examples](kuberay-examples) for more RayService examples.
The [MobileNet example](kuberay-mobilenet-rayservice-example) is a good example to start with because it doesn't require GPUs and is easy to run on a local machine.
