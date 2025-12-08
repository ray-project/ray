(kuberay-rayservice-incremental-upgrade)=
# RayService Zero-Downtime Incremental Upgrades

This guide details how to configure and use the `NewClusterWithIncrementalUpgrade` strategy for a `RayService` with KubeRay. This feature was proposed in a [Ray Enhancement Proposal (REP)](https://github.com/ray-project/enhancements/blob/main/reps/2024-12-4-ray-service-incr-upgrade.md) and implemented with alpha support in KubeRay v1.5.1. If unfamiliar with RayServices and KubeRay, see the [RayService Quickstart](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayservice-quick-start.html).

In previous versions of KubeRay, zero-downtime upgrades were supported only through the `NewCluster` strategy. This upgrade strategy involved scaling up a pending RayCluster with equal capacity as the active cluster, waiting until the updated Serve applications were healthy, and then switching traffic to the new RayCluster. While this upgrade strategy is reliable, it required users to scale 200% of their original cluster's compute resources which can be prohibitive when dealing with expensive accelerator resources.

The `NewClusterWithIncrementalUpgrade` strategy is designed for large-scale deployments, such as LLM serving, where duplicating resources for a standard blue/green deployment is not feasible due to resource constraints. This feature minimizes resource usage during RayService CR upgrades while maintaining service availability. Below we explain the design and usage.

Rather than creating a new `RayCluster` at 100% capacity, this strategy creates a new cluster and gradually scales its capacity up while simultaneously shifting user traffic from the old cluster to the new one. This gradual traffic migration enables users to safely scale their updated RayService while the old cluster auto-scales down, enabling users to save expensive compute resources and exert greater control over the pace of their upgrade. This process relies on the Kubernetes Gateway API for fine-grained traffic splitting.

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

#### Example: Setting up a RayService on kind:

The following instructions detail the minimal steps to configure a cluster with KubeRay and trigger a zero-downtime incremental upgrade for a RayService.

1. Create a kind cluster
```bash
kind create cluster --image=kindest/node:v1.29.0
```
We use `v1.29.0` which is known to be compatible with recent Istio versions.

2. Install istio
```
istioctl install --set profile=demo -y
```

3. Install Gateway API CRDs
```bash
kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.3.0/standard-install.yaml
```

4. Create a Gateway class with the following spec
```yaml
echo "apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: istio
spec:
  controllerName: istio.io/gateway-controller" | kubectl apply -f -
```

```yaml
kubectl get gatewayclass
NAME           CONTROLLER                    ACCEPTED   AGE
istio          istio.io/gateway-controller   True       4s
istio-remote   istio.io/unmanaged-gateway    True       3s
```

5. Install and Configure MetalLB for LoadBalancer on kind
```bash
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.14.7/config/manifests/metallb-native.yaml
```

Create an `IPAddressPool` with the following spec for MetalLB
```yaml
echo "apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: kind-pool
  namespace: metallb-system
spec:
  addresses:
  - 192.168.8.200-192.168.8.250 # adjust based on your subnets range
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: default
  namespace: metallb-system
spec:
  ipAddressPools:
  - kind-pool" | kubectl apply -f -
```

6. Install the KubeRay operator, following [these instructions](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/kuberay-operator-installation.html). The minimum version for this guide is v1.5.1. To use this feature, the `RayServiceIncrementalUpgrade` feature gate must be enabled. To enable the feature gate when installing the kuberay operator, run the following command:
```bash
helm install kuberay-operator kuberay/kuberay-operator --version v1.5.1 \
  --set featureGates\[0\].name=RayServiceIncrementalUpgrade \
  --set featureGates\[0\].enabled=true
```

7. Create a RayService with incremental upgrade enabled.
```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.incremental-upgrade.yaml
```

8. Update one of the fields under `rayClusterConfig` and re-apply the RayService to trigger a zero-downtime upgrade.

### 2. How it Works: The Upgrade Process

Understanding the lifecycle of an incremental upgrade helps in monitoring and configuration.

1.  **Trigger:** You trigger an upgrade by updating the `RayService` spec, such as changing the container `image` or updating the `resources` used by a worker group in the `rayClusterSpec`.
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
        * If the Ray Serve autoscaler is enabled, the Serve application will scale its `num_replicas` from `min_replicas` based on the new `target_capacity`. Without the Ray Serve autoscaler enabled, the new `target_capacity` value will directly adjust `num_replicas` for each Serve deployment. Depending on the updated value of`num_replicas`, the Ray Autoscaler will begin provisioning pods for the pending cluster to handle the updated resource load.

    * **Phase 2: Shift Traffic (HTTPRoute)**
        * KubeRay waits for the pending cluster's new pods to be ready. There may be a temporary drop in requests-per-second while worker Pods are being
        created for the updated Ray serve replicas.
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
  name: rayservice-incremental-upgrade
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

### 4. Trigger the Upgrade

Incremental upgrades are triggered exactly like standard zero-downtime upgrades in KubeRay: by modifying the `spec.rayClusterConfig` in your RayService Custom Resource.

When KubeRay detects a change in the cluster specification (such as a new container image, modified resource limits, or updated environment variables), it calculates a new hash. If the hash differs from the active cluster and incremental upgrades are enabled, the `NewClusterWithIncrementalUpgrade` strategy is automatically initiated.

Updates to the cluster specifications can occur by running `kubectl apply -f` on the updated YAML configuration file, or by directly editing the CR using `kubectl edit rayservice <your-rayservice-name>`.

### 5. Monitoring the Upgrade

You can monitor the progress of the upgrade by inspecting the `RayService` status and the `HTTPRoute` object.

1.  **Check `RayService` Status:**
    ```bash
    kubectl describe rayservice rayservice-incremental-upgrade
    ```
    Look at the `Status` section. You will see both `Active Service Status` and `Pending Service Status`, which show the state of both clusters. Pay close attention to these two new fields:
    * **`Target Capacity`:** The percentage of replicas KubeRay is *telling* this cluster to scale to.
    * **`Traffic Routed Percent`:** The percentage of traffic KubeRay is *currently* sending to this cluster via the Gateway.

    During an upgrade, you will see `Target Capacity` on the pending cluster increase in steps (e.g., 20%, 40%) and `Traffic Routed Percent` gradually climb to meet it.

2.  **Check `HTTPRoute` Weights:**
    You can also see the traffic weights directly on the `HTTPRoute` resource KubeRay manages.
    ```bash
    kubectl get httproute rayservice-incremental-upgrade-httproute -o yaml
    ```
    Look at the `spec.rules.backendRefs`. You will see the `weight` for the old and new services change in real-time as the traffic shift (Phase 2) progresses.

For example:
```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  creationTimestamp: "2025-12-07T07:42:24Z"
  generation: 10
  name: stress-test-serve-httproute
  namespace: default
  ownerReferences:
  - apiVersion: ray.io/v1
    blockOwnerDeletion: true
    controller: true
    kind: RayService
    name: stress-test-serve
    uid: 83a785cc-8745-4ccd-9973-2fc9f27000cc
  resourceVersion: "3714"
  uid: 660b14b5-78df-4507-b818-05989b1ef806
spec:
  parentRefs:
  - group: gateway.networking.k8s.io
    kind: Gateway
    name: stress-test-serve-gateway
    namespace: default
  rules:
  - backendRefs:
    - group: ""
      kind: Service
      name: stress-test-serve-f6z4w-serve-svc
      namespace: default
      port: 8000
      weight: 90
    - group: ""
      kind: Service
      name: stress-test-serve-xclvf-serve-svc
      namespace: default
      port: 8000
      weight: 10
    matches:
    - path:
        type: PathPrefix
        value: /
status:
  parents:
  - conditions:
    - lastTransitionTime: "2025-12-07T07:42:24Z"
      message: Route was valid
      observedGeneration: 10
      reason: Accepted
      status: "True"
      type: Accepted
    - lastTransitionTime: "2025-12-07T07:42:24Z"
      message: All references resolved
      observedGeneration: 10
      reason: ResolvedRefs
      status: "True"
      type: ResolvedRefs
    controllerName: istio.io/gateway-controller
    parentRef:
      group: gateway.networking.k8s.io
      kind: Gateway
      name: stress-test-serve-gateway
      namespace: default
```

## How to upgrade safely?

Since this feature is alpha and rollback is not yet supported, we recommend conservative parameter settings to minimize risk during upgrades.

### Recommended Parameters

To upgrade safely, you should:
1. Scale up 1 worker pod in the new cluster and scale down 1 worker pod in the old cluster at a time
2. Make the upgrade process gradual to allow the Ray Serve autoscaler and Ray autoscaler to adapt

Based on these principles, we recommend:
- **maxSurgePercent**: Calculate based on the formula below
- **stepSizePercent**: Set to a value less than `maxSurgePercent`
- **intervalSeconds**: 60

### Calculating maxSurgePercent

The `maxSurgePercent` determines the maximum percentage of additional resources that can be provisioned during the upgrade. To calculate the minimum safe value:

\begin{equation}
\text{maxSurgePercent} = \frac{\text{resources per pod}}{\text{total cluster resources}} \times 100
\end{equation}

#### Example

Consider a RayCluster with the following configuration:
- `excludeHeadService`: true
- Head pod: No GPU
- 5 worker pods, each with 1 GPU (total: 5 GPUs)

For this cluster:
\begin{equation}
\text{maxSurgePercent} = \frac{1 \text{ GPU}}{5 \text{ GPUs}} \times 100 = 20\%
\end{equation}

With `maxSurgePercent: 20`, the upgrade process ensures:
- The new cluster scales up **1 worker pod at a time** (20% of 5 = 1 pod)
- The old cluster scales down **1 worker pod at a time**
- Your cluster temporarily uses 6 GPUs during the transition (5 original + 1 new)

This configuration guarantees you have sufficient resources to run at least one additional worker pod during the upgrade without resource contention.

### Understanding intervalSeconds

Set `intervalSeconds` to 60 seconds to give the Ray Serve autoscaler and Ray autoscaler sufficient time to:
- Detect load changes
- Immediately scale replicas up or down to enforce new min_replicas and max_replicas limits (via target_capacity)
  - Scale down replicas immediately if they exceed the new max_replicas
  - Scale up replicas immediately if they fall below the new min_replicas
- Provision resources

A larger interval prevents the upgrade controller from making changes faster than the autoscaler can react, reducing the risk of service disruption.

### Example Configuration

```yaml
upgradeStrategy:
  maxSurgePercent: 20  # Calculated: (1 GPU / 5 GPUs) × 100
  stepSizePercent: 10  # Less than maxSurgePercent
  intervalSeconds: 60  # Wait 1 minute between steps
```

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

