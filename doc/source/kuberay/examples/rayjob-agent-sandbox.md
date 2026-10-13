---
myst:
  html_meta:
    description: "Run sandboxed code execution with KubeRay and Agent Sandbox on GKE with gVisor, covering RBAC, sandbox infrastructure, and suspending and resuming sandboxes between agent turns."
---

(kuberay-agent-sandbox)=

# Sandboxed code execution with Ray and Agent Sandbox

This example shows how to use the [Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox) with Ray and KubeRay to orchestrate code execution in a secure sandboxed environment, and how to suspend and resume sandboxes between agent turns to reclaim their capacity while they're idle. This example uses GKE and gVisor, but you can modify it to work with other sandbox runtimes.

The setup below enables memory-snapshot suspend and resume by default. Suspension is opt-in at runtime, and sandboxes behave normally until you suspend one. Each snapshot-specific setup piece is noted, so you can skip it if you only want sandboxed code execution.

---

## What is Agent Sandbox?

[Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox) is a Kubernetes project to streamline the management of sandboxes on Kubernetes. Agent Sandbox provides declarative Kubernetes APIs that can be used with KubeRay to manage sandbox environments that can be invoked from a Ray cluster.

Agent Sandbox is compatible with multiple runtimes that offer strong isolation guarantees such as [gVisor](https://github.com/google/gvisor) and [Kata containers](https://github.com/kata-containers/kata-containers). Consider using Ray and Agent Sandbox for agentic reinforcement learning (RL) use cases where you need to securely execute code generated from a model during its post-training phase.

Agent Sandbox provides a collection of declarative Kubernetes APIs to manage Sandbox runtimes. This example uses the following custom resources provided by Agent Sandbox:
- `Sandbox`: the foundational unit. It manages a single Pod with a stable hostname and network identity. Unlike standard Pods, a Sandbox can be configured with persistent storage through `volumeClaimTemplates` that survives restarts.
- `SandboxClaim`: creates `Sandboxes` from a `SandboxTemplate`, abstracting away the details of the underlying Sandbox configuration.
- `SandboxTemplate`: defines reusable templates for creating `Sandboxes`, making it easier to manage large numbers of similar `Sandboxes`.
- `SandboxWarmPool`: manages a pool of pre-warmed `Sandboxes` that can be allocated to users in under 200 ms, reducing the time it takes to get a new Sandbox up and running.

The Agent Sandbox project also provides a [Python SDK](https://github.com/kubernetes-sigs/agent-sandbox/tree/main/clients/python/agentic-sandbox-client) that you can use from Ray actors to create Sandboxes and run code securely inside them.

## Why suspend and resume sandboxes?

In agentic RL rollouts, a sandbox is typically held for a whole multi-turn trajectory but only executes commands for a small fraction of that time. A turn runs a few seconds of code, then waits many seconds for model inference. The sandbox is idle, but its pod still holds its full CPU and memory reservation, which is what bounds cluster density.

The turn boundary is the natural suspend signal. The orchestrator, here a Ray actor, knows the exact moment it has a command result and is waiting on the model. Suspending at that boundary returns the pod's reservation to the scheduler for the duration of the inference call. Resuming restores the sandbox before the next turn's command arrives.

Agent Sandbox supports two suspension mechanisms, demonstrated in the optional Steps 8 and 9 at the end of this guide:

1. **Pause and resume with `spec.operatingMode`** — built into Agent Sandbox and works on any Kubernetes cluster, with no snapshot infrastructure at all. Suspending terminates the sandbox pod and frees its CPU and memory reservation while keeping the `Sandbox` resource and its volumes. Resuming recreates the pod and remounts storage. Process state and memory are not preserved.
2. **Memory snapshots** — Agent Sandbox supports snapshotting a sandbox's full memory state through the Python SDK's snapshot extension, which checkpoints the entire guest to object storage before suspending and restores it intact on resume. The checkpoint covers the process tree, memory, open file descriptors, and `tmpfs`. This guide uses GKE Pod Snapshots as the reference snapshot backend, set up by the optional snapshot pieces of Steps 1, 4, and 5 below.

Use plain `operatingMode` pause and resume when the sandbox's durable state lives on its persistent volumes. Use memory snapshots when the agent depends on live process state that must survive the gap, such as running background processes, in-memory data, or open files.

## Deploying KubeRay with Agent Sandbox

The following example creates a KubeRay RayJob, which runs a Ray job that uses the Agent Sandbox SDK to invoke code execution in a secure sandbox. Keep Pods used for sandboxing decoupled from the Ray cluster itself.

### Step 1: Create a GKE cluster and node pools

Create a GKE Standard cluster with [Pod Snapshots](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots) enabled. Pod Snapshots power the memory-snapshot suspend and resume shown later in this guide, and they require version `1.35.3-gke.1234000` or later with Workload Identity Federation. If you don't plan to use suspend and resume, you can omit `--enable-pod-snapshots` and the Workload Identity flags:

```bash
gcloud container clusters create <YOUR_CLUSTER_NAME> \
    --enable-pod-snapshots \
    --cluster-version=<CLUSTER_VERSION> \
    --workload-pool=<PROJECT_ID>.svc.id.goog \
    --workload-metadata=GKE_METADATA \
    --location=<LOCATION>
```

See [Prepare for Pod snapshots](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/pod-snapshots-prepare) for enabling the feature on an existing cluster.

Create two separate node pools, one for KubeRay-provisioned Pods and one for sandbox Pods using the gVisor runtime. The gVisor pool uses `n2-standard-4` because E2 machines don't support whole-pod snapshots. Any machine type works if you skipped Pod Snapshots:

```bash
gcloud container node-pools create ray-worker-pool \
    --cluster=<YOUR_CLUSTER_NAME> \
    --machine-type=e2-standard-4 \
    --num-nodes=2

gcloud container node-pools create ray-gvisor-pool \
    --cluster=<YOUR_CLUSTER_NAME> \
    --sandbox type=gvisor \
    --machine-type=n2-standard-4 \
    --image-type=cos_containerd \
    --num-nodes=1
```

### Step 2: Install KubeRay operator

Follow the instructions in [KubeRay operator](kuberay-operator-deploy) to install the KubeRay operator.

### Step 3: Deploy Agent Sandbox

Install the Custom Resource Definitions (CRDs), controllers, and extensions from the official Agent Sandbox release. This guide uses `v1.0.0`, the minimum version for the SDK's snapshot extension:

```bash
export VERSION="v1.0.0"

kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${VERSION}/sandbox.yaml
kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${VERSION}/extensions.yaml
```

### Step 4: Apply RBAC permissions for the Ray workers

The Agent Sandbox Python SDK running inside Ray workers needs to talk to the Kubernetes API to claim and delete sandboxes. This example uses the default service account token in the default namespace to grant Ray workers the ability to create Sandboxes.

Create a file named `rbac.yaml` with the following content:
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ray-sandbox-manager-role
  namespace: default
rules:
- apiGroups: ["extensions.agents.x-k8s.io"]
  resources: ["sandboxclaims"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["agents.x-k8s.io"]
  resources: ["sandboxes"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ray-sandbox-manager-binding
  namespace: default
subjects:
- kind: ServiceAccount
  name: default
  namespace: default
roleRef:
  kind: Role
  name: ray-sandbox-manager-role
  apiGroup: rbac.authorization.k8s.io
```

Apply the RBAC configurations:
```bash
kubectl apply -f rbac.yaml
```

The suspend and resume path needs additional permissions. The SDK snapshot extension patches the `spec.operatingMode` field on `Sandbox` resources, reads Pods across the suspend and resume cycle, and manages `podsnapshot.gke.io` triggers and snapshots. Apply the additional permissions below, or skip them if you don't plan to use suspend and resume:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/snapshots/rbac.yaml
```

### Step 5: Deploy sandbox infrastructure

Run the following command to create sandbox infrastructure using Agent Sandbox:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/sandbox.yaml
```

The command creates the following resources:
- `SandboxTemplate`: defines the per-sandbox podSpec. The Pod is configured to use gVisor, sets `automountServiceAccountToken: false` so untrusted code inside the sandbox cannot read a Kubernetes ServiceAccount token, and sets `networkPolicyManagement: Unmanaged` because the NetworkPolicy below is stricter than the controller's Secure Default. The template also labels every sandbox Pod with `app: python-runtime-pool` so other selectors, such as the NetworkPolicy podSelector or your own `kubectl get` queries, can target them by a stable, human-readable label.
- `SandboxWarmPool` (`python-runtime-pool`): keeps six pre-booted sandbox Pods ready so the Ray actors' claims complete in under 200 ms.
- `NetworkPolicy` (`python-runtime-pool-restrict-egress`): default-denies egress for every sandbox pod except DNS. This provides concrete containment: the CNI drops packets at the node rather than relying on cluster-default policies.

Verify the warm pool pods are running:

```bash
kubectl get pods -l app=python-runtime-pool
```

The SandboxWarmPool configuration keeps six gVisor Pods running:

```bash
GVISOR_POD=$(kubectl get pod -l app=python-runtime-pool -o jsonpath='{.items[0].metadata.name}')
kubectl get pod "$GVISOR_POD" -o jsonpath='{.spec.automountServiceAccountToken}{"\n"}'   # expect: false
kubectl exec "$GVISOR_POD" -- ls /var/run/secrets/kubernetes.io/serviceaccount/ 2>&1     # expect: No such file or directory
```

#### (Optional) Snapshot storage and sandbox pool

This part provisions the storage and warm pool used by memory-snapshot suspend and resume. Skip it if you don't plan to use suspend and resume. Snapshot state is written to a Cloud Storage bucket, which needs hierarchical namespaces, uniform bucket-level access, and the same location as the cluster:

```bash
gcloud storage buckets create "gs://<BUCKET_NAME>" \
    --uniform-bucket-level-access \
    --enable-hierarchical-namespace \
    --soft-delete-duration=0d \
    --location=<LOCATION>
```

Grant access via Workload Identity Federation to the `sandbox-snapshot-ksa` Kubernetes ServiceAccount that the sandbox pods run as. The manifest applied below creates this ServiceAccount:

```bash
gcloud storage buckets add-iam-policy-binding "gs://<BUCKET_NAME>" \
    --member="principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/default/sa/sandbox-snapshot-ksa" \
    --role="roles/storage.bucketViewer"

gcloud storage buckets add-iam-policy-binding "gs://<BUCKET_NAME>" \
    --member="principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/default/sa/sandbox-snapshot-ksa" \
    --role="roles/storage.objectUser"
```

Grant the GKE Pod Snapshot controller access to the bucket:

```bash
gcloud projects add-iam-policy-binding "<PROJECT_ID>" \
    --member="serviceAccount:service-<PROJECT_NUMBER>@container-engine-robot.iam.gserviceaccount.com" \
    --role="roles/storage.objectUser" \
    --condition='expression=resource.name.startsWith("projects/_/buckets/<BUCKET_NAME>"),title=restrict_to_bucket'
```

Then download [`sandbox-snapshot.yaml`](https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/snapshots/sandbox-snapshot.yaml), replace `SNAPSHOT_BUCKET_NAME` with your bucket name, and apply it. It creates:

- `ServiceAccount` (`sandbox-snapshot-ksa`): the identity the sandbox pods run as, used by Pod Snapshots to write to the bucket. The token is not mounted into the sandbox container, so untrusted code can't use it, and the egress policy below closes the metadata-server path to the pool's cloud credentials.
- `PodSnapshotStorageConfig`: points Pod Snapshots at your bucket.
- `PodSnapshotPolicy`: selects the pool's pods, uses `manual` triggers (the SDK creates `PodSnapshotManualTrigger` resources), and groups snapshots by the `agents.x-k8s.io/sandbox-name-hash` label. The SDK requires the grouping rule because it guarantees a sandbox restores only from its own snapshots.
- `SandboxTemplate` and `SandboxWarmPool` (`python-snapshot-pool`): four pre-warmed gVisor sandboxes, separate from the pool above. The template sets `networkPolicyManagement: Unmanaged` because the controller's default Managed policy only admits ingress via the sandbox-router, while the suspend and resume example's Ray actors connect to the sandbox pod IP directly. On a NetworkPolicy-enforcing cluster the managed policy would block them. The manifest instead ships its own `python-snapshot-pool-restrict-egress` NetworkPolicy: default-deny egress with DNS only, which blocks untrusted code from reaching the GKE metadata server and using the pool's Workload Identity credentials. Snapshot checkpoints are unaffected because uploads run through the node-side snapshot agent.

```bash
kubectl apply -f sandbox-snapshot.yaml
kubectl get pods -l app=python-snapshot-pool
```

### Step 6: Create the RayJob

Run the following command to create a RayJob resource:

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/ray-cluster.yaml
```

The RayJob is configured to do the following:
1. Create a RayCluster.
2. Submit a Ray job that runs `sandboxed_code_execution.py` on the Ray cluster.
3. Run Ray actors in the driver script that use the Agent Sandbox Python SDK to create Sandboxes.
4. Execute code in each sandboxed environment from the actors and verify the output.

### Step 7: Verify the output

Monitor the status and query the execution logs of the submitted RayJob:

```sh
# List running job pods
kubectl get pods -l job-name=agent-sandbox-code-execution-demo

# Stream the demo logs
kubectl logs -f -l job-name=agent-sandbox-code-execution-demo
```

Once the job starts, two `SandboxExecutor` Ray actors each claim one Pod from `python-runtime-pool`. The SDK reports per-actor adoption latency, which stays under 200 ms when the warm pool is healthy. Every Python snippet that follows runs inside the sandbox pod, never on the Ray worker. gVisor isolates the syscall surface, the `python-runtime-pool-restrict-egress` NetworkPolicy applied in Step 5 default-denies all egress except DNS, and `sandbox.commands.run(..., timeout=5)` bounds wall-clock blast radius per call.

Expected output, abridged:

```
Starting 2 SandboxExecutors...
Dispatching 2 code executors...
(SandboxExecutor pid=457, ip=10.72.5.24) [executor-1] claimed sandbox 'sandbox-claim-6d4504d8' in 0.257s

--- Execution Results ---

[compute_fib.py] (Exit Code: 0)
  Stdout: fib(20) = 6765

[json_aggregation.py] (Exit Code: 0)
  Stdout: {"mean": 11.0, "max": 25}

Cleaning up sandboxes...
(SandboxExecutor pid=342, ip=10.72.1.10) [executor-0] claimed sandbox 'sandbox-claim-3a93b626' in 0.212s
```

### Step 8 (optional): Pause and resume sandboxes with `spec.operatingMode`

This mechanism needs no SDK and no GKE-specific features. It's a one-field patch on the `Sandbox` resource, and any orchestrator that applies manifests can issue it directly against the sandboxes created earlier in this guide:

```bash
# Suspend: terminates the pod, keeps the Sandbox resource and volumes.
kubectl patch sandbox <SANDBOX_NAME> --type=merge -p '{"spec":{"operatingMode":"Suspended"}}'

# Resume: recreates the pod and remounts storage.
kubectl patch sandbox <SANDBOX_NAME> --type=merge -p '{"spec":{"operatingMode":"Running"}}'
```

The field declares intent. Observed progress is reported on the Sandbox's conditions:

```bash
# Suspension is complete when the Suspended condition is True (reason: PodTerminated).
kubectl get sandbox <SANDBOX_NAME> -o jsonpath='{.status.conditions[?(@.type=="Suspended")]}'

# After resuming, wait for Ready=True before reconnecting.
kubectl wait sandbox/<SANDBOX_NAME> --for=condition=Ready --timeout=180s
```

While suspended, the sandbox reports `Ready=False` with reason `SandboxSuspended` and its pod is gone from the node. After the resume patch, a fresh pod reaches `Ready` in about 15 to 20 seconds on an idle cluster.

Semantics to plan around:

- The pod is deleted and recreated. Process state and memory are lost, and anything that must survive belongs on the sandbox's persistent volumes.
- The pod IP and pod name change across the cycle. Re-resolve them from the Sandbox status after resume, and don't cache connections across a suspend.
- The pod's CPU and memory reservation is returned to the scheduler while suspended. This is where the density win comes from.

### Step 9 (optional): Suspend and resume with memory snapshots

The example runs a RayJob in which each Ray actor claims a sandbox from the `python-snapshot-pool` warm pool and drives a simulated multi-turn rollout. At the turn boundary the actor calls `sandbox.suspend()`, which snapshots and terminates the pod while the model is thinking, then calls `sandbox.resume()` before the next turn. The first turn writes state to `/tmp` inside the sandbox and the second turn reads it back after the suspend and resume cycle. A plain pod restart wipes `/tmp`, so the read succeeding, together with the SDK's `restored_from_snapshot` check, is what distinguishes a memory snapshot from a plain pause.

The suspend and resume calls come from the Agent Sandbox Python SDK's snapshot extension ([`k8s_agent_sandbox.gke_extensions.snapshots`](https://github.com/kubernetes-sigs/agent-sandbox/tree/main/clients/python/agentic-sandbox-client/k8s_agent_sandbox/gke_extensions/snapshots)), which layers GKE Pod Snapshots on top of the `operatingMode` mechanism above. The SDK package `k8s-agent-sandbox>=1.0.0` requires Python 3.11 or later on the Ray image.

Run the suspend and resume RayJob and stream its logs:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/snapshots/ray-job.yaml
kubectl logs -f -l job-name=agent-sandbox-snapshot-demo
```

Turn 1's `read_state.py` succeeds after the pod was terminated and recreated. That success is the proof that the snapshot restored the guest rather than cold-starting it. The example also asserts the SDK's `restored_from_snapshot` flag on every resume. Expected output, abridged:

```
Starting 2 SandboxExecutors...
(SandboxExecutor pid=904) [executor-1] claimed sandbox 'sandbox-claim-c4861c64' in 0.208s
[turn 0][executor-0] write_state.py (exit=0): fib(20) = 6765 (saved to /tmp)
[turn 0][executor-1] write_state.py (exit=0): fib(20) = 6765 (saved to /tmp)
[executor-0] suspended in 5.6s
[executor-1] suspended in 6.2s
Sandboxes suspended, waiting 10s for 'model inference'...
[executor-0] resumed from snapshot in 4.3s
[executor-1] resumed from snapshot in 4.3s
[turn 1][executor-0] read_state.py (exit=0): state from the previous turn survived the snapshot: fib(20) = 6765
[turn 1][executor-1] read_state.py (exit=0): state from the previous turn survived the snapshot: fib(20) = 6765

All turns completed: /tmp state survived the suspend/resume cycle.
```

Observed on an idle GKE Standard cluster with n2-standard-4 gVisor nodes and a small sandbox working set, suspend completes in about 5 seconds, covering the snapshot, the upload, and pod termination. Resume, which recreates the pod with memory restored, completes in about 4 seconds. Larger working sets snapshot and restore more slowly, so measure with your own workload before deciding between suspending every turn or only at long gaps.

You can also watch the machinery directly while the job runs:

```bash
# Snapshots being written per sandbox
kubectl get podsnapshots.podsnapshot.gke.io

# Suspend and resume state reflected on the Sandbox resources
kubectl get sandboxes -o custom-columns=NAME:.metadata.name,MODE:.spec.operatingMode
```

### Notes and current limitations

- **Connections don't survive.** The pod IP and pod name change across a suspend and resume cycle even with snapshots. The SDK closes and re-establishes its own connection, but anything else holding a connection to the sandbox must reconnect.
- **Wall-clock time jumps forward** inside the guest on restore. Tasks whose correctness depends on wall-clock progress between turns, such as background builds or polled services, are poor suspension candidates. Let the task declare that and opt out rather than inferring it at runtime.
- **Restore picks the most recent `Ready` snapshot** in the sandbox's group. `suspend()` waits for the snapshot to be taken before terminating the pod, but a resume issued immediately after a suspend can race the snapshot upload. Check `restored_from_snapshot` on the resume response, as the example does.
- **Suspend and resume latency is not free.** Each end of the cycle costs seconds, depending on snapshot size. It pays off when the inference gap it bridges is longer than the cycle cost, or when reclaiming the pod's reservation matters more than latency.
