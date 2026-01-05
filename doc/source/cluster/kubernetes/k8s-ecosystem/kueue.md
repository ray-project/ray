(kuberay-kueue)=
# Gang scheduling, Priority scheduling, and Autoscaling for KubeRay CRDs with Kueue

This guide demonstrates how to integrate KubeRay with [Kueue](https://kueue.sigs.k8s.io/) to enable advanced scheduling capabilities including gang scheduling and priority scheduling for Ray applications on Kubernetes.

For real-world use cases with RayJob, see [Priority Scheduling with RayJob and Kueue](kuberay-kueue-priority-scheduling-example) and [Gang Scheduling with RayJob and Kueue](kuberay-kueue-gang-scheduling-example).

## What's Kueue?
[Kueue](https://kueue.sigs.k8s.io/) is a Kubernetes-native job queueing system that manages resource quotas and job lifecycle. Kueue decides when:
* To make a job wait.
* To admit a job to start, which triggers Kubernetes to create pods.
* To preempt a job, which triggers Kubernetes to delete active pods.

## Supported KubeRay CRDs
Kueue has native support for the following KubeRay APIs:
- **RayJob**: Ideal for batch processing and model training workloads (covered in this guide)
- **RayCluster**: Perfect for managing long-running Ray clusters
- **RayService**: Designed for serving models and applications

*Note: This guide focuses on a detailed RayJob example on a kind cluster. For RayCluster and RayService examples, see the ["Working with RayCluster and RayService"](#working-with-raycluster-and-rayservice) section.*

## Prerequisites

Before you begin, ensure you have a Kubernetes cluster. This guide uses a local Kind cluster.

## Step 0: Create a Kind cluster

```bash
kind create cluster
```

## Step 1: Install the KubeRay operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

## Step 2: Install Kueue

```bash
VERSION=v0.13.4
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/$VERSION/manifests.yaml
```

See [Kueue Installation](https://kueue.sigs.k8s.io/docs/installation/#install-a-released-version) for more details on installing Kueue.
**Note**: Some limitations exist between Kueue and RayJob. See the [limitations of Kueue](https://kueue.sigs.k8s.io/docs/tasks/run_rayjobs/#c-limitations) for more details.


## Step 3: Create Kueue Resources

This manifest creates the necessary Kueue resources to manage scheduling and resource allocation.

```yaml
# kueue-resources.yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: "default-flavor"
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: "cluster-queue"
spec:
  preemption:
    withinClusterQueue: LowerPriority
  namespaceSelector: {} # Match all namespaces.
  resourceGroups:
  - coveredResources: ["cpu", "memory"]
    flavors:
    - name: "default-flavor"
      resources:
      - name: "cpu"
        nominalQuota: 3
      - name: "memory"
        nominalQuota: 6G
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  namespace: "default"
  name: "user-queue"
spec:
  clusterQueue: "cluster-queue"
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata:
  name: prod-priority
value: 1000
description: "Priority class for prod jobs"
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata:
  name: dev-priority
value: 100
description: "Priority class for development jobs"
```

The YAML manifest configures:

* **ResourceFlavor**
  * The ResourceFlavor `default-flavor` is an empty ResourceFlavor because the compute resources in the Kubernetes cluster are homogeneous. In other words, users can request 1 CPU without considering whether it's an ARM chip or an x86 chip.
* **ClusterQueue**
    * The ClusterQueue `cluster-queue` only has 1 ResourceFlavor `default-flavor` with quotas for 3 CPUs and 6G memory.
    * The ClusterQueue `cluster-queue` has a preemption policy `withinClusterQueue: LowerPriority`. This policy allows the pending RayJob that doesn’t fit within the nominal quota for its ClusterQueue to preempt active RayJob custom resources in the ClusterQueue that have lower priority.
* **LocalQueue**
  * The LocalQueue `user-queue` is a namespace-scoped object in the `default` namespace which belongs to a ClusterQueue. A typical practice is to assign a namespace to a tenant, team, or user of an organization. Users submit jobs to a LocalQueue, instead of to a ClusterQueue directly.
* **WorkloadPriorityClass**
  * The WorkloadPriorityClass `prod-priority` has a higher value than the WorkloadPriorityClass `dev-priority`. RayJob custom resources with the `prod-priority` priority class take precedence over RayJob custom resources with the `dev-priority` priority class.

Create the Kueue resources:
```bash
kubectl apply -f kueue-resources.yaml
```

## Step 4: Gang scheduling with Kueue

Kueue always admits workloads in “gang” mode.
Kueue admits workloads on an “all or nothing” basis, ensuring that Kubernetes never partially provisions a RayJob or RayCluster.
Use gang scheduling strategy to avoid wasting compute resources caused by inefficient scheduling of workloads.

Download the RayJob YAML manifest from the KubeRay repository.

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-job.kueue-toy-sample.yaml
```

Before creating the RayJob, modify the RayJob metadata with the following:

```yaml
metadata:
  generateName: rayjob-sample-
  labels:
    kueue.x-k8s.io/queue-name: user-queue
    kueue.x-k8s.io/priority-class: dev-priority
```

Create two RayJob custom resources with the same priority `dev-priority`.
Note these important points for RayJob custom resources:
* The RayJob custom resource includes 1 head Pod and 1 worker Pod, with each Pod requesting 1 CPU and 2G of memory.
* The RayJob runs a simple Python script that demonstrates a loop running 600 iterations, printing the iteration number and sleeping for 1 second per iteration. Hence, the RayJob runs for about 600 seconds after the submitted Kubernetes Job starts.
* Set `shutdownAfterJobFinishes` to true for RayJob to enable automatic cleanup. This setting triggers KubeRay to delete the RayCluster after the RayJob finishes.
  * Kueue doesn't handle the RayJob custom resource with the `shutdownAfterJobFinishes` set to false. See the [limitations of Kueue](https://kueue.sigs.k8s.io/docs/tasks/run_rayjobs/#c-limitations) for more details.

```yaml
kubectl create -f ray-job.kueue-toy-sample.yaml
```

Each RayJob custom resource requests 2 CPUs and 4G of memory in total.
However, the ClusterQueue only has 3 CPUs and 6G of memory in total.
Therefore, the second RayJob custom resource remains pending, and KubeRay doesn't create Pods from the pending RayJob, even though the remaining resources are sufficient for a Pod.
You can also inspect the `ClusterQueue` to see available and used quotas:

```bash
$ kubectl get clusterqueues.kueue.x-k8s.io
NAME            COHORT   PENDING WORKLOADS
cluster-queue            1

$ kubectl get clusterqueues.kueue.x-k8s.io cluster-queue -o yaml
Status:
  Admitted Workloads:  1 # Workloads admitted by queue.
  Conditions:
    Last Transition Time:  2024-02-28T22:41:28Z
    Message:               Can admit new workloads
    Reason:                Ready
    Status:                True
    Type:                  Active
  Flavors Reservation:
    Name:  default-flavor
    Resources:
      Borrowed:  0
      Name:      cpu
      Total:     2
      Borrowed:  0
      Name:      memory
      Total:     4Gi
  Flavors Usage:
    Name:  default-flavor
    Resources:
      Borrowed:         0
      Name:             cpu
      Total:            2
      Borrowed:         0
      Name:             memory
      Total:            4Gi
  Pending Workloads:    1
  Reserving Workloads:  1
```

Kueue admits the pending RayJob custom resource when the first RayJob custom resource finishes.
Check the status of the RayJob custom resources and delete them after they finish:

```bash
$ kubectl get rayjobs.ray.io
NAME                  JOB STATUS   DEPLOYMENT STATUS   START TIME             END TIME               AGE
rayjob-sample-ckvq4   SUCCEEDED    Complete            xxxxx                  xxxxx                  xxx
rayjob-sample-p5msp   SUCCEEDED    Complete            xxxxx                  xxxxx                  xxx

$ kubectl delete rayjob rayjob-sample-ckvq4
$ kubectl delete rayjob rayjob-sample-p5msp
```

## Step 5: Priority scheduling with Kueue

This step creates a RayJob with a lower priority class `dev-priority` first and a RayJob with a higher priority class `prod-priority` later.
The RayJob with higher priority class `prod-priority` takes precedence over the RayJob with lower priority class `dev-priority`.
Kueue preempts the RayJob with a lower priority to admit the RayJob with a higher priority.

If you followed the previous step, the RayJob YAML manifest `ray-job.kueue-toy-sample.yaml` should already be set to the `dev-priority` priority class.
Create a RayJob with the lower priority class `dev-priority`:

```bash
kubectl create -f ray-job.kueue-toy-sample.yaml
```

Before creating the RayJob with the higher priority class `prod-priority`, modify the RayJob metadata with the following:

```yaml
metadata:
  generateName: rayjob-sample-
  labels:
    kueue.x-k8s.io/queue-name: user-queue
    kueue.x-k8s.io/priority-class: prod-priority
```

Create a RayJob with the higher priority class `prod-priority`:

```bash
kubectl create -f ray-job.kueue-toy-sample.yaml
```

You can see that KubeRay operator deletes the Pods belonging to the RayJob with the lower priority class `dev-priority` and creates the Pods belonging to the RayJob with the higher priority class `prod-priority`.

## Working with RayCluster and RayService

### RayCluster with Kueue
For gang scheduling with RayCluster resources, Kueue ensures that all cluster components (head and worker nodes) are provisioned together. This prevents partial cluster creation and resource waste.
**For detailed RayCluster integration**: See the [Kueue documentation for RayCluster](https://kueue.sigs.k8s.io/docs/tasks/run/rayclusters/).
### RayService with Kueue
RayService integration with Kueue enables gang scheduling for model serving workloads, ensuring consistent resource allocation for serving infrastructure.
**For detailed RayService integration**: See the [Kueue documentation for RayService](https://kueue.sigs.k8s.io/docs/tasks/run/rayservices/).

## Ray Autoscaler with Kueue

Kueue can treat a **RayCluster** or the underlying cluster of a **RayService** as an
**elastic workload**.  Kueue manages queueing and quota for the entire
cluster, while the in‑tree Ray autoscaler scales worker Pods up and down
based on the resource demand. This section shows how to enable
autoscaling for Ray workloads managed by Kueue using a step‑by‑step
approach similar to the existing Kueue integration guides.

> **Supported resources** – At the time of writing, the Kueue
> autoscaler integration supports `RayCluster` and `RayService`.  Support
> for `RayJob` autoscaling is under development; see the Kueue issue
> tracker for updates: [issue](https://github.com/kubernetes-sigs/kueue/issues/7605).


### Prerequisites

Make sure you have already:
- Installed the [KubeRay operator](kuberay-operator-deploy).
- Installed **Kueue** (See [Kueue Installation](https://kueue.sigs.k8s.io/docs/installation/#install-a-released-version) for more details, note that it's recommended to install Kueue version >= v0.13)

---

### Step 1: Create Kueue resources

Define a **ResourceFlavor**, **ClusterQueue**, and **LocalQueue** so that
Kueue knows how many CPUs and how much memory it can allocate. The
manifest below creates an 8‑CPU/16‑GiB pool called `default-flavor`,
registers it in a `ClusterQueue` named `ray-cq`, and defines a
`LocalQueue` named `ray-lq`:

```yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: default-flavor
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: ray-cq
spec:
  cohort: ray-example
  namespaceSelector: {}
  resourceGroups:
  - coveredResources: ["cpu", "memory"]
    flavors:
    - name: default-flavor
      resources:
      - name: cpu
        nominalQuota: 8
      - name: memory
        nominalQuota: 16Gi
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  name: ray-lq
  namespace: default
spec:
  clusterQueue: ray-cq
```

Apply the resources:

```bash
kubectl apply -f kueue-resources.yaml
```

### Step 2: Enable elastic workloads in Kueue

Autoscaling only works when Kueue’s `ElasticJobsViaWorkloadSlices`
feature gate is enabled.

Run the following command to add the feature gate flag to the
`kueue-controller-manager` Deployment:

```bash
kubectl -n kueue-system patch deploy kueue-controller-manager \
  --type='json' \
  -p='[
    {
      "op": "add",
      "path": "/spec/template/spec/containers/0/args/-",
      "value": "--feature-gates=ElasticJobsViaWorkloadSlices=true"
    }
  ]'
```
### Autoscaling with RayCluster
#### Step 1: Configure an elastic RayCluster

An elastic RayCluster is one that can change its worker count at
runtime. Kueue requires three changes to recognize a RayCluster as
elastic:

1. **Queue label** – set
   `metadata.labels.kueue.x-k8s.io/queue-name: <localqueue>` so that
   Kueue queues this cluster.
2. **Elastic-job annotation** – add
   `metadata.annotations.kueue.x-k8s.io/elastic-job: "true"` to mark
   this cluster as elastic. Kueue creates **WorkloadSlices** for
   scaling up and down.
3. **Enable the Ray autoscaler** – set
   `spec.enableInTreeAutoscaling: true` in the `RayCluster` spec and
   optionally configure `autoscalerOptions` such as
   `idleTimeoutSeconds`.

Here is a minimal manifest for an elastic RayCluster:

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-kueue-autoscaler
  namespace: default
  labels:
    kueue.x-k8s.io/queue-name: ray-lq
  annotations:
    kueue.x-k8s.io/elastic-job: "true"  # Mark as elastic
spec:
  rayVersion: "2.46.0"
  enableInTreeAutoscaling: true          # Turn on the Ray autoscaler
  autoscalerOptions:
    idleTimeoutSeconds: 60              # Delete idle workers after 60 s
  headGroupSpec:
    serviceType: ClusterIP
    rayStartParams:
      dashboard-host: "0.0.0.0"
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.46.0
          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
            limits:
              cpu: "2"
              memory: "5Gi"
  workerGroupSpecs:
  - groupName: workers
    replicas: 0       # start with no workers; autoscaler will add them
    minReplicas: 0    # lower bound
    maxReplicas: 4    # upper bound
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.46.0
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"
            limits:
              cpu: "2"
              memory: "5Gi"
```

Apply this manifest and verify that Kueue admits the associated
Workload:

```bash
kubectl apply -f raycluster-kueue-autoscaler.yaml
kubectl get workloads.kueue.x-k8s.io -A
```

The `ADMITTED` column should show `True` once the `RayCluster`
has been scheduled by Kueue.
```bash
NAMESPACE   NAME                                           QUEUE    RESERVED IN   ADMITTED   FINISHED   AGE
default     raycluster-raycluster-kueue-autoscaler-21c46   ray-lq   ray-cq        True                  26s
```

(step-2-verify-autoscaling-for-a-raycluster)=
#### Step 2: Verify autoscaling for a RayCluster

To observe autoscaling, create load on the cluster and watch worker
Pods appear. The following procedure runs a CPU‑bound workload from
inside the head Pod and monitors scaling:

1. **Enter the head Pod:**

    ```bash
    HEAD_POD=$(kubectl get pod -l ray.io/node-type=head,ray.io/cluster=raycluster-kueue-autoscaler \
      -o jsonpath='{.items[0].metadata.name}')
    kubectl exec -it "$HEAD_POD" -- bash
    ```

2. **Run a workload:** execute the following Python script inside the
   head container. It submits 20 tasks that each consume a full CPU
   for about one minute.

    ```bash
    python << 'EOF'
    import ray, time

    ray.init(address="auto")

    @ray.remote(num_cpus=1)
    def busy():
        end = time.time() + 60
        while time.time() < end:
            x = 0
            for i in range(100_000):
                x += i * i
        return 1

    tasks = [busy.remote() for _ in range(20)]
    print(sum(ray.get(tasks)))
    EOF
    ```

   Because the head Pod has a single CPU, the tasks queue up and the
   autoscaler raises the worker replicas toward the `maxReplicas`.

3. **Monitor worker Pods:** in another terminal, watch the worker
    Pods scale up and down:

    ```bash
    kubectl get pods -w \
      -l ray.io/cluster=raycluster-kueue-autoscaler,ray.io/node-type=worker
    ```

    New worker Pods should appear as the tasks run and vanish once the
    workload finishes and the idle timeout elapses.

### Autoscaling with RayService
#### Step 1: Configure an elastic RayService

A `RayService` deploys a Ray Serve application by materializing the
`spec.rayClusterConfig` into a managed `RayCluster`. Kueue doesn't
interact with the RayService object directly. Instead, the KubeRay
operator propagates relevant metadata from the RayService to the
managed RayCluster, and **Kueue queues and admits that RayCluster**.

To make a RayService work with Kueue and the Ray autoscaler:

1. **Queue label**
   `metadata.labels.kueue.x-k8s.io/queue-name` on the `RayService`.
   KubeRay passes service labels to the underlying `RayCluster`,
   allowing Kueue to queue it.
2. **Elastic-job annotation**
   `metadata.annotations.kueue.x-k8s.io/elastic-job: "true"`.  This
   annotation propagates to the `RayCluster` and instructs Kueue to
   treat it as an elastic workload.
3. **Enable the Ray autoscaler**
   `spec.rayClusterConfig`, set `enableInTreeAutoscaling: true` and
   specify worker `minReplicas`/`maxReplicas`.

The following manifest deploys a simple Ray Serve app with autoscaling.
The Serve application (`demo_app`) and deployment names (`ServiceA` and
`ServiceB`) are placeholders to avoid implying a specific KubeRay
example. Adjust the deployments and resources for your own
application.

```yaml
apiVersion: ray.io/v1
kind: RayService
metadata:
  name: rayservice-kueue-autoscaler
  namespace: default
  labels:
    kueue.x-k8s.io/queue-name: ray-lq       # copy to RayCluster
  annotations:
    kueue.x-k8s.io/elastic-job: "true"       # mark as elastic
spec:
  # A simple Serve config with two deployments
  serveConfigV2: |
    applications:
      - name: fruit_app
        import_path: fruit.deployment_graph
        route_prefix: /fruit
        runtime_env:
          working_dir: "https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip"
        deployments:
          - name: MangoStand
            num_replicas: 2
            max_replicas_per_node: 1
            user_config:
              price: 3
            ray_actor_options:
              num_cpus: 0.1
          - name: OrangeStand
            num_replicas: 1
            user_config:
              price: 2
            ray_actor_options:
              num_cpus: 0.1
          - name: PearStand
            num_replicas: 1
            user_config:
              price: 1
            ray_actor_options:
              num_cpus: 0.1
          - name: FruitMarket
            num_replicas: 1
            ray_actor_options:
              num_cpus: 0.1
      - name: math_app
        import_path: conditional_dag.serve_dag
        route_prefix: /calc
        runtime_env:
          working_dir: "https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip"
        deployments:
          - name: Adder
            num_replicas: 1
            user_config:
              increment: 3
            ray_actor_options:
              num_cpus: 0.1
          - name: Multiplier
            num_replicas: 1
            user_config:
              factor: 5
            ray_actor_options:
              num_cpus: 0.1
          - name: Router
            num_replicas: 1
  rayClusterConfig:
    rayVersion: "2.46.0"
    enableInTreeAutoscaling: true
    autoscalerOptions:
      idleTimeoutSeconds: 60
    headGroupSpec:
      serviceType: ClusterIP
      rayStartParams:
        dashboard-host: "0.0.0.0"
      template:
        spec:
          containers:
          - name: ray-head
            image: rayproject/ray:2.46.0
            resources:
              requests:
                cpu: "1"
                memory: "2Gi"
              limits:
                cpu: "2"
                memory: "5Gi"
    workerGroupSpecs:
    - groupName: workers
      replicas: 1            # initial workers
      minReplicas: 1         # lower bound
      maxReplicas: 5         # upper bound
      rayStartParams: {}
      template:
        spec:
          containers:
          - name: ray-worker
            image: rayproject/ray:2.46.0
            resources:
              requests:
                cpu: "1"
                memory: "1Gi"
              limits:
                cpu: "2"
                memory: "5Gi"
```

Apply the manifest and verify that the service's RayCluster is
admitted by Kueue:

```bash
kubectl apply -f rayservice-kueue-autoscaler.yaml
kubectl get workloads.kueue.x-k8s.io -A
```


The `ADMITTED` column should show `True` once the `RayService`
has been scheduled by Kueue.
```bash
NAMESPACE   NAME                                                 QUEUE    RESERVED IN   ADMITTED   FINISHED   AGE
default     raycluster-rayservice-kueue-autoscaler-9xvcr-d7add   ray-lq   ray-cq        True                  21s
```


#### Step 2: Verify autoscaling for a RayService

Autoscaling for a `RayService` is ultimately driven by load on the
managed RayCluster. The verification procedure is the same as for a
plain `RayCluster`.

To verify autoscaling:

1. Follow the steps in
   [Step 2: Verify autoscaling for a RayCluster](#step-2-verify-autoscaling-for-a-raycluster),
   but use the RayService name in the label selector. Concretely:
   - when selecting the head Pod, use (remember to replace your cluster name):
      ```bash
      HEAD_POD=$(kubectl get pod \
      -l ray.io/node-type=head,ray.io/cluster=rayservice-kueue-autoscaler-9xvcr \
      -o jsonpath='{.items[0].metadata.name}')
      kubectl exec -it "$HEAD_POD" -- bash
      ```
   - inside the head container, run the same CPU-bound Python script
     used in the RayCluster example.
      ```bash
      python << 'EOF'
      import ray, time

      ray.init(address="auto")

      @ray.remote(num_cpus=1)
      def busy():
          end = time.time() + 60
          while time.time() < end:
              x = 0
              for i in range(100_000):
                  x += i * i
          return 1

      tasks = [busy.remote() for _ in range(20)]
      print(sum(ray.get(tasks)))
      EOF
      ```
   - in another terminal, watch the worker Pods with:
      ```bash
      kubectl get pods -w \
        -l ray.io/cluster=rayservice-kueue-autoscaler,ray.io/node-type=worker
      ```

As in the RayCluster case, the worker Pods scale up toward
`maxReplicas` while the CPU-bound tasks are running and scale back
down toward `minReplicas` after the tasks finish and the idle timeout
elapses. The only difference is that the `ray.io/cluster` label now
matches the RayService name (`rayservice-kueue-autoscaler-9xvcr`) instead of the
stand-alone `RayCluster` name (`raycluster-kueue-autoscaler`).

### Limitations

* **Feature status** – The `ElasticJobsViaWorkloadSlices` feature gate
  is currently **alpha**. Elastic autoscaling only applies to
  RayClusters that are annotated with
  `kueue.x-k8s.io/elastic-job: "true"` and configured with
  `enableInTreeAutoscaling: true` when ray image < 2.47.0.

* **RayJob support** – Autoscaling for `RayJob` isn't yet supported.
  The Kueue maintainers are actively tracking this work and will update
  their documentation when it becomes available.

* **Kueue versions prior to v0.13** – If you are using a Kueue version
  earlier than v0.13, restart the Kueue controller once after
  installation to ensure RayCluster management works correctly.
