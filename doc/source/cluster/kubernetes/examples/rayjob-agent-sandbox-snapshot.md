---
myst:
  html_meta:
    description: "Suspend and resume Agent Sandbox pods between agent turns from Ray, using operatingMode pause/resume or GKE Pod Snapshots for full memory restore."
---

(kuberay-agent-sandbox-snapshot)=

# Suspend and Resume Agent Sandboxes with Ray and GKE Pod Snapshots

This example extends {ref}`Sandboxed Code Execution with Ray and Agent Sandbox <kuberay-agent-sandbox>` with sandbox **suspend and resume**. It shows two mechanisms:

1. **Pause/resume with `spec.operatingMode`** — built into Agent Sandbox, works on any Kubernetes cluster. Suspending terminates the sandbox pod (freeing its CPU/memory reservation) while keeping the `Sandbox` resource and its volumes; resuming recreates the pod and remounts storage. Process state and memory are **not** preserved.
2. **Memory snapshots with GKE Pod Snapshots** — the Agent Sandbox Python SDK's snapshot extension checkpoints the entire gVisor guest (process tree, memory, open file descriptors, `tmpfs`) to Cloud Storage before suspending, and restores it intact on resume.

## Why suspend sandboxes between agent turns?

In agentic RL rollouts, a sandbox is typically held for a whole multi-turn trajectory but only executes commands for a small fraction of that time: a turn runs a few seconds of code, then waits many seconds for model inference. The sandbox is idle — but its pod still holds its full CPU and memory reservation, which is what bounds cluster density.

The turn boundary is the natural suspend signal: the orchestrator (here, a Ray actor) knows the exact moment it has a command result and is now waiting on the model. Suspending at that boundary returns the pod's reservation to the scheduler for the duration of the inference call; resuming restores the sandbox before the next turn's command arrives.

Use plain `operatingMode` pause/resume when the sandbox's durable state lives on its persistent volumes. Use memory snapshots when the agent depends on live process state — running background processes, in-memory data, open files — that must survive the gap.

## Option 1: Pause and resume with `spec.operatingMode`

This mechanism needs no SDK and no GKE-specific features — it's a one-field patch on the `Sandbox` resource, which any orchestrator that applies manifests can issue directly:

```bash
# Suspend: terminates the pod, keeps the Sandbox resource and volumes.
kubectl patch sandbox <SANDBOX_NAME> --type=merge -p '{"spec":{"operatingMode":"Suspended"}}'

# Resume: recreates the pod and remounts storage.
kubectl patch sandbox <SANDBOX_NAME> --type=merge -p '{"spec":{"operatingMode":"Running"}}'
```

The field declares intent; observed progress is reported on the Sandbox's conditions:

```bash
# Suspension is complete when the Suspended condition is True (reason: PodTerminated).
kubectl get sandbox <SANDBOX_NAME> -o jsonpath='{.status.conditions[?(@.type=="Suspended")]}'

# After resuming, wait for Ready=True before reconnecting.
kubectl wait sandbox/<SANDBOX_NAME> --for=condition=Ready --timeout=180s
```

Expected output for the full cycle:

```
{"lastTransitionTime":"...","message":"Pod has been terminated. Sandbox is suspended","observedGeneration":2,"reason":"PodTerminated","status":"True","type":"Suspended"}
sandbox.agents.x-k8s.io/<SANDBOX_NAME> condition met
```

While suspended, the sandbox reports `Ready=False` with reason `SandboxSuspended` and its pod is gone from the node; after the resume patch, a fresh pod reaches `Ready` in seconds (~15–20s observed on an idle cluster).

Semantics to plan around:

- The pod is deleted and recreated: **process state and memory are lost**; anything that must survive belongs on the sandbox's persistent volumes.
- The **pod IP and pod name change** across the cycle. Re-resolve them from the Sandbox status after resume; don't cache connections across a suspend.
- The pod's CPU/memory reservation is returned to the scheduler while suspended — this is where the density win comes from.

You can try this against the sandboxes from the base example with nothing but `kubectl`. The rest of this page covers option 2, which preserves memory.

## Option 2: Memory snapshots with GKE Pod Snapshots

The demo runs a RayJob in which each Ray actor claims a sandbox from a warm pool and drives a simulated multi-turn rollout: at the turn boundary it calls `sandbox.suspend()` (snapshot + pod termination) while the "model is thinking", and `sandbox.resume()` before the next turn. The first turn writes state to `/tmp` inside the sandbox and the second turn reads it back after the suspend/resume cycle — `/tmp` is wiped by a plain pod restart, so the read succeeding (together with the SDK's `restored_from_snapshot` check) is what distinguishes a memory snapshot from a plain pause.

The suspend/resume calls come from the Agent Sandbox Python SDK's snapshot extension ([`k8s_agent_sandbox.gke_extensions.snapshots`](https://github.com/kubernetes-sigs/agent-sandbox/tree/main/clients/python/agentic-sandbox-client/k8s_agent_sandbox/gke_extensions/snapshots)), which layers [GKE Pod Snapshots](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots) on top of the `operatingMode` mechanism from option 1.

### Step 1: Create a GKE cluster with Pod Snapshots enabled

Pod Snapshots require a GKE Standard cluster on version `1.35.3-gke.1234000` or later, with Workload Identity Federation, and a gVisor node pool:

```bash
gcloud container clusters create <YOUR_CLUSTER_NAME> \
    --enable-pod-snapshots \
    --cluster-version=<CLUSTER_VERSION> \
    --workload-pool=<PROJECT_ID>.svc.id.goog \
    --workload-metadata=GKE_METADATA \
    --location=<LOCATION>

# Node pool for the Ray cluster pods.
gcloud container node-pools create ray-worker-pool \
    --cluster=<YOUR_CLUSTER_NAME> \
    --machine-type=e2-standard-4 \
    --num-nodes=2 \
    --location=<LOCATION>

# gVisor node pool for the sandbox pods.
gcloud container node-pools create ray-gvisor-pool \
    --cluster=<YOUR_CLUSTER_NAME> \
    --sandbox type=gvisor \
    --machine-type=n2-standard-4 \
    --image-type=cos_containerd \
    --num-nodes=1 \
    --location=<LOCATION>
```

See [Prepare for Pod snapshots](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/pod-snapshots-prepare) for enabling the feature on an existing cluster.

### Step 2: Install the KubeRay operator

Follow the instructions in [KubeRay operator](kuberay-operator-deploy) to install the KubeRay operator.

### Step 3: Deploy Agent Sandbox

Install the Agent Sandbox CRDs, controller, and extensions:

```bash
export VERSION="v1.0.0"
kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${VERSION}/sandbox.yaml
kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${VERSION}/extensions.yaml
```

### Step 4: Create the snapshot storage bucket and grant access

Snapshot state is written to a Cloud Storage bucket. The bucket needs hierarchical namespaces, uniform bucket-level access, and the same location as the cluster:

```bash
gcloud storage buckets create "gs://<BUCKET_NAME>" \
    --uniform-bucket-level-access \
    --enable-hierarchical-namespace \
    --soft-delete-duration=0d \
    --location=<LOCATION>
```

Grant access to the `sandbox-snapshot-ksa` Kubernetes ServiceAccount the sandbox pods run as (created in Step 6), via Workload Identity Federation:

```bash
gcloud storage buckets add-iam-policy-binding "gs://<BUCKET_NAME>" \
    --member="principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/default/sa/sandbox-snapshot-ksa" \
    --role="roles/storage.bucketViewer"

gcloud storage buckets add-iam-policy-binding "gs://<BUCKET_NAME>" \
    --member="principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/default/sa/sandbox-snapshot-ksa" \
    --role="roles/storage.objectUser"
```

And grant the GKE Pod Snapshot controller access to the bucket:

```bash
gcloud projects add-iam-policy-binding "<PROJECT_ID>" \
    --member="serviceAccount:service-<PROJECT_NUMBER>@container-engine-robot.iam.gserviceaccount.com" \
    --role="roles/storage.objectUser" \
    --condition="expression=resource.name.startsWith(\"projects/_/buckets/<BUCKET_NAME>\"),title=restrict_to_bucket"
```

### Step 5: Apply RBAC for the Ray workers

Beyond claiming sandboxes, the SDK snapshot extension patches `Sandbox` resources (`spec.operatingMode`), reads Pods across the suspend/resume cycle, and manages `podsnapshot.gke.io` triggers and snapshots:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox-snapshots/rbac.yaml
```

### Step 6: Deploy the sandbox and snapshot infrastructure

Download [`sandbox-snapshot.yaml`](https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox-snapshots/sandbox-snapshot.yaml), replace `SNAPSHOT_BUCKET_NAME` with your bucket name, and apply it. It creates:

- **`ServiceAccount`** (`sandbox-snapshot-ksa`) — the identity the sandbox pods run as, used by Pod Snapshots to write to the bucket. The token is not mounted into the sandbox container, so untrusted code can't use it.
- **`PodSnapshotStorageConfig`** — points Pod Snapshots at your bucket.
- **`PodSnapshotPolicy`** — selects the pool's pods, uses `manual` triggers (the SDK creates `PodSnapshotManualTrigger` resources), and groups snapshots by the `agents.x-k8s.io/sandbox-name-hash` label. The grouping rule is required by the SDK: it guarantees a sandbox restores only from its own snapshots.
- **`SandboxTemplate`** and **`SandboxWarmPool`** (`python-snapshot-pool`) — 4 pre-warmed gVisor sandboxes, as in the base example. The template sets `networkPolicyManagement: Unmanaged` because the controller's default Managed policy only admits ingress via the sandbox-router, while this demo's Ray actors connect to the sandbox pod IP directly; on a NetworkPolicy-enforcing cluster the managed policy would block them. For containment, layer on an egress NetworkPolicy like the one in the base example.

```bash
kubectl apply -f sandbox-snapshot.yaml
kubectl get pods -l app=python-snapshot-pool
```

### Step 7: Run the RayJob

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox-snapshots/ray-job.yaml
kubectl logs -f -l job-name=agent-sandbox-snapshot-demo
```

### Step 8: Verify the output

Turn 1's `read_state.py` succeeding — after the pod was terminated and recreated — is the proof that the snapshot restored the guest rather than cold-starting it (the demo also asserts the SDK's `restored_from_snapshot` flag on every resume). Expected output (abridged):

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

Observed on an idle GKE Standard cluster (n2-standard-4 gVisor nodes, small sandbox working set): suspend — snapshot taken, uploaded, and pod terminated — completes in ~5s, and resume — pod recreated with memory restored — in ~4s. Larger working sets snapshot and restore more slowly; measure with your own workload before deciding between suspending every turn or only long gaps.

You can also watch the machinery directly while the job runs:

```bash
# Snapshots being written per sandbox
kubectl get podsnapshots.podsnapshot.gke.io

# Suspend/resume reflected on the Sandbox resources
kubectl get sandboxes -o custom-columns=NAME:.metadata.name,MODE:.spec.operatingMode
```

## Notes and current limitations

- **Connections don't survive.** The pod IP and pod name change across a suspend/resume cycle even with snapshots; the SDK closes and re-establishes its own connection, but anything else holding a connection to the sandbox must reconnect.
- **Wall-clock time jumps forward** inside the guest on restore. Tasks whose correctness depends on wall-clock progress between turns (background builds, polled services) are poor suspension candidates — let the task declare that and opt out rather than inferring it at runtime.
- **Restore picks the most recent `Ready` snapshot** in the sandbox's group. `suspend()` waits for the snapshot to be taken before terminating the pod, but a resume issued immediately after a suspend can race snapshot upload; check `restored_from_snapshot` on the resume response, as the demo does.
- **Suspend/resume latency is not free** — each end of the cycle costs seconds (snapshot size dependent). It pays off when the inference gap it bridges is longer than the cycle cost, or when reclaiming the pod's reservation matters more than latency.
