(rayservice-incremental-upgrade)=
# RayService Zero-Downtime Incremental Upgrades

This guide details how to configure and use the `NewClusterWithIncrementalUpgrade` strategy for a `RayService` with KubeRay. This feature was proposed in a [Ray Enhancement Proposal (REP)](https://github.com/ray-project/enhancements/blob/main/reps/2024-12-4-ray-service-incr-upgrade.md) and implemented with alpha support in KubeRay v1.5.0. If unfamiliar with RayServices and KubeRay, see the [RayService Quickstart](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayservice-quick-start.html).

In previous versions of KubeRay, zero-downtime upgrades were supported only through the `NewCluster` strategy. This upgrade strategy involved scaling up a pending RayCluster with equal capacity as the active cluster, waiting until the updated Serve applications were healthy, and then switching traffic to the new RayCluster. While this upgrade strategy is reliable, it required users to scale 200% of their original clusters compute which can be prohibitive when dealing with expensive accelerator resources.

The `NewClusterWithIncrementalUpgrade` strategy is designed for large-scale deployments, such as LLM serving, where duplicating resources for a standard blue/green deployment is not feasible due to resource constraints. Rather than creating a new `RayCluster` at 100% capacity, this strategy creates a new cluster and gradually scales its capacity up while simultaneously shifting user traffic from the old cluster to the new one. This gradual traffic migration enables users to safely scale their updated RayService while the old cluster auto-scales down, enabling users to save expensive compute resources and exert fine-grained control over the pace of their upgrade. This process relies on the Kubernetes Gateway API for fine-grained traffic splitting.

## Quickstart: Performing an Incremental Upgrade

### 1. Prerequisites

Before you can use this feature, you **must** have the following set up in your Kubernetes cluster:

1.  **Gateway API CRDs:** The K8s Gateway API resources must be installed. You can typically install them with:
    ```bash
    kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.4.0/standard-install.yaml
    ```

    The RayService controller utilizes GA Gateway API resources such as a [Gateway](https://kubernetes.io/docs/concepts/services-networking/gateway/#api-kind-gateway) and [HTTPRoute](https://kubernetes.io/docs/concepts/services-networking/gateway/#api-kind-httproute) to safely split traffic during the upgrade.

2.  **A Gateway Controller:** Users must install a Gateway controller that implements the Gateway API, such as Istio, Contour, or a cloud-native implementation like GKE's Gateway controller. This feature should support any controller that implements Gateway API with support for `Gateway` and `HTTPRoute` CRDs, but is an alpha feature that's primarily been tested utilizing [Istio](https://istio.io/latest/docs/tasks/traffic-management/ingress/gateway-api/).
3.  **A `GatewayClass` Resource:** Your cluster admin must create a `GatewayClass` resource that defines which controller to use. KubeRay will use this to create `Gateway` and `HTTPRoute` objects.

    **Example: Istio `GatewayClass`**
    ```yaml
    apiVersion: gateway.networking.k8s.io/v1
    kind: GatewayClass
    metadata:
        name: istio
    spec:
        controllerName: istio.io/gateway-controller
    ```
    You will need to use the `metadata.name` (e.g. `istio`) in the `gatewayClassName` field of the `RayService` spec.

4.  **Ray Autoscaler:** Incremental upgrades require the Ray Autoscaler to be enabled in your `RayCluster` spec, as KubeRay manages the upgrade by adjusting the `target_capacity` for Ray Serve which adjusts the number of Serve replicas for each deployment. These Serve replicas are translated into a resource load which the Ray autoscaler considers when determining the number of Pods to provision with KubeRay. For information on enabling and configuring Ray autoscaling on Kubernetes, see [KubeRay Autoscaling](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/configuring-autoscaling.html).

### 2. How it Works: The Upgrade Process

Understanding the lifecycle of an incremental upgrade helps in monitoring and configuration.

1.  **Trigger:** You trigger an upgrade by updating the `RayService` spec, such as changing the container `image` or updating the `serveConfigV2`.
2.  **Pending Cluster Creation:** KubeRay detects the change and creates a new, *pending* `RayCluster`. It sets this cluster's initial `target_capacity` (the percentage of serve replicas it should run) to `0%`.
3.  **Gateway and Route Creation:** KubeRay creates a `Gateway` resource for your `RayService` and an `HTTPRoute` resource that initially routes 100% of traffic to the old, *active* cluster and 0% to the new, *pending* cluster.
4.  **The Upgrade Loop Begins:**
    The KubeRay controller now enters a loop that repeats three phases until the upgrade is complete. This loop ensures that the total cluster capacity only exceeds 100% by at most `maxSurgePercent`, preventing resource starvation.

    Let's use an example: `maxSurgePercent: 20` and `stepSizePercent: 5`.

    * **Initial State:**
        * Active Cluster `target_capacity`: 100%
        * Pending Cluster `target_capacity`: 0%
        * **Total Capacity: 100%**

    ---

    **The Upgrade Cycle**

    * **Phase 1: Scale Up Pending Cluster (Capacity)**
        * KubeRay checks the total capacity (100%) and sees it's $\le$ 100%. It increases the **pending** cluster's `target_capacity` by `maxSurgePercent`.
        * Active `target_capacity`: 100%
        * Pending `target_capacity`: 0% $\rightarrow$ **20%**
        * **Total Capacity: 120%**
        * The Ray Autoscaler begins provisioning pods for the pending cluster to handle 20% of the target load.

    * **Phase 2: Shift Traffic (HTTPRoute)**
        * KubeRay waits for the pending cluster's new pods to be ready.
        * Once ready, it begins to *gradually* shift traffic. Every `intervalSeconds`, it updates the `HTTPRoute` weights, moving `stepSizePercent` (5%) of traffic from the active to the pending cluster.
        * This continues until the *actual* traffic (`trafficRoutedPercent`) "catches up" to the *pending* cluster's `target_capacity` (20% in this example).

    * **Phase 3: Scale Down Active Cluster (Capacity)**
        * Once Phase 2 is complete (`trafficRoutedPercent` == 20%), the loop runs again.
        * KubeRay checks the total capacity (120%) and sees it's > 100%. It decreases the **active** cluster's `target_capacity` by `maxSurgePercent`.
        * Active `target_capacity`: 100% $\rightarrow$ **80%**
        * Pending `target_capacity`: 20%
        * **Total Capacity: 100%**
        * The Ray Autoscaler terminates pods on the active cluster as they become idle.

    ---

5.  **Completion & Cleanup:**
    This cycle of **(Scale Up Pending $\rightarrow$ Shift Traffic $\rightarrow$ Scale Down Active)** continues until the pending cluster is at 100% `target_capacity` and 100% `trafficRoutedPercent`, and the active cluster is at 0%.

    KubeRay then promotes the pending cluster to active, updates the `HTTPRoute` to send 100% of traffic to it, and safely terminates the old `RayCluster`.

### 3. Example `RayService` Configuration

To use the feature, set the `upgradeStrategy.type` to `NewClusterWithIncrementalUpgrade` and provide the required options.

```yaml
apiVersion: ray.io/v1
kind: RayService
metadata:
  name: example-rayservice
spec:
  # This is the main configuration block for the upgrade
  upgradeStrategy:
    # 1. Set the type to NewClusterWithIncrementalUpgrade
    type: "NewClusterWithIncrementalUpgrade"
    clusterUpgradeOptions:
      # 2. The name of your K8s GatewayClass
      gatewayClassName: "istio"

      # 3. Capacity scaling: Increase new cluster's target_capacity
      #    by 20% in each scaling step.
      maxSurgePercent: 20

      # 4. Traffic shifting: Move 5% of traffic from old to new
      #    cluster every intervalSeconds.
      stepSizePercent: 5

      # 5. Interval seconds controls the pace of traffic migration during the upgrade.
      intervalSeconds: 10

  # This is your Serve config
  serveConfigV2: |
    applications:
      - name: my_app
        import_path: my_model:app
        route_prefix: /
        deployments:
          - name: MyModel
            num_replicas: 10
            ray_actor_options:
              resources: { "GPU": 1 }
            autoscaling_config:
              min_replicas: 0
              max_replicas: 20

  # This is your RayCluster config (autoscaling must be enabled)
  rayClusterSpec:
    enableInTreeAutoscaling: true
    headGroupSpec:
      # ... head spec ...
    workerGroupSpecs:
    - groupName: gpu-worker
      replicas: 0
      minReplicas: 0
      maxReplicas: 20
      template:
        # ... pod spec with GPU requests ...
```

### 4. Monitoring the Upgrade

You can monitor the progress of the upgrade by inspecting the `RayService` status and the `HTTPRoute` object.

1.  **Check `RayService` Status:**
    ```bash
    kubectl describe rayservice example-rayservice
    ```
    Look at the `Status` section. You will see both `Active Service Status` and `Pending Service Status`, which show the state of both clusters. Pay close attention to these two new fields:
    * **`Target Capacity`:** The percentage of replicas KubeRay is *telling* this cluster to scale to.
    * **`Traffic Routed Percent`:** The percentage of traffic KubeRay is *currently* sending to this cluster via the Gateway.

    During an upgrade, you will see `Target Capacity` on the pending cluster increase in steps (e.g., 20%, 40%) and `Traffic Routed Percent` gradually climb to meet it.

2.  **Check `HTTPRoute` Weights:**
    You can also see the traffic weights directly on the `HTTPRoute` resource KubeRay manages.
    ```bash
    kubectl get httproute example-rayservice-httproute -n <your-namespace> -o yaml
    ```
    Look at the `spec.rules.backendRefs`. You will see the `weight` for the old and new services change in real-time as the traffic shift (Phase 2) progresses.

### 5. Rollback Support

To roll back a failing or poorly performing upgrade, simply **update the `RayService` manifest back to the original configuration** (e.g., change the `image` back to the old tag).

KubeRay's controller will detect that the "goal state" now matches the *active* (old) cluster. It will reverse the process:
1.  Scale the active cluster's `target_capacity` back to 100%.
2.  Shift all traffic back to the active cluster.
3.  Scale down and terminate the *pending* (new) cluster.

---

## API Overview (Reference)

This section details the new and updated fields in the `RayService` CRD.

### `RayService.spec.upgradeStrategy`

| Field | Type | Description | Required | Default |
| :--- | :--- | :--- | :--- | :--- |
| `type` | `string` | The strategy to use for upgrades. Can be `NewCluster`, `None`, or `NewClusterWithIncrementalUpgrade`. | No | `NewCluster` |
| `clusterUpgradeOptions` | `object` | Container for incremental upgrade settings. **Required if `type` is `NewClusterWithIncrementalUpgrade`.** The `RayServiceIncrementalUpgrade` feature gate must be enabled. | No | `nil` |

### `RayService.spec.upgradeStrategy.clusterUpgradeOptions`

This block is required *only* if `type` is set to `NewClusterWithIncrementalUpgrade`.

| Field | Type | Description | Required | Default |
| :--- | :--- | :--- | :--- | :--- |
| `maxSurgePercent` | `int32` | The percentage of *capacity* (Serve replicas) to add to the new cluster in each scaling step. For example, a value of `20` means the new cluster's `target_capacity` will increase in 20% increments (0% -> 20% -> 40%...). Must be between 0 and 100. | No | `100` |
| `stepSizePercent` | `int32` | The percentage of *traffic* to shift from the old to the new cluster during each interval. Must be between 0 and 100. | **Yes** | N/A |
| `intervalSeconds` | `int32` | The time in seconds to wait between shifting traffic by `stepSizePercent`. | **Yes** | N/A |
| `gatewayClassName` | `string` | The `metadata.name` of the `GatewayClass` resource KubeRay should use to create `Gateway` and `HTTPRoute` objects. | **Yes** | N/A |

### `RayService.status.activeServiceStatus` & `RayService.status.pendingServiceStatus`

Three new fields are added to both the `activeServiceStatus` and `pendingServiceStatus` blocks to provide visibility into the upgrade process.

| Field | Type | Description |
| :--- | :--- | :--- |
| `targetCapacity` | `int32` | The target percentage of Serve replicas this cluster is *configured* to handle (from 0 to 100). This is controlled by KubeRay based on `maxSurgePercent`. |
| `trafficRoutedPercent` | `int32` | The *actual* percentage of traffic (from 0 to 100) currently being routed to this cluster's endpoint. This is controlled by KubeRay during an upgrade based on `stepSizePercent` and `intervalSeconds`. |
| `lastTrafficMigratedTime` | `metav1.Time` | A timestamp indicating the last time `trafficRoutedPercent` was updated. |

#### Next steps:
* See [Deploy on Kubernetes](https://docs.ray.io/en/latest/serve/production-guide/kubernetes.html) for more information about deploying Ray Serve with KubeRay.
* See [Ray Serve Autoscaling](https://docs.ray.io/en/latest/serve/autoscaling-guide.html) to configure your Serve deployments to scale based on traffic load.