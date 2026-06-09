(kuberay-agent-sandbox)=

# Sandboxed Code Execution with Ray and Agent Sandbox

This example shows how to use the [Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox) with Ray and KubeRay to execute LLM-generated code in a secure sandboxed environment. This example uses GKE and gVisor but can be modified to work on other providers.

---

## What is Agent Sandbox?

[Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox) is a Kubernetes project to streamline the management of sandboxes on Kubernetes. Agent sandbox provides declarative Kubernetes APIs that can be used with KubeRay to manage sandbox environments that can be invoked from a Ray cluster.

Agent sandbox is compatible with multiple runtimes that offer strong isolation guarantees such as [gVisor](https://github.com/google/gvisor) and [Kata containers](https://github.com/kata-containers/kata-containers). Consider using Ray and Agent Sandbox for agentic RL use-cases where you need to securely execute code generated from a model during its post-training phase.

Agent Sandbox provides a collection of declarative Kubernetes APIs to easily manage Sandbox runtimes. This example uses the following custom resources provided by Agent Sandbox:
- `Sandbox`: This is the foundational unit, it manages a single Pod with a stable hostname and network identity. Unlike standard Pods, a Sandbox can be configured with persistent storage via volumeClaimTemplates that survives restarts
- `SandboxClaim`: Allows users to create `Sandboxes` from a `SandboxTemplate`, abstracting away the details of the underlying Sandbox configuration. 
- `SandboxTemplate`: Provides a way to define reusable templates for creating `Sandboxes`, making it easier to manage large numbers of similar `Sandboxes`.
- `SandboxWarmPool`: Manages a pool of pre-warmed `Sandboxes` that can be quickly (<200ms) allocated to users, reducing the time it takes to get a new Sandbox up and running.   

The Agent Sandbox project also provides a [Python SDK](https://github.com/kubernetes-sigs/agent-sandbox/tree/main/clients/python/agentic-sandbox-client) which can be used from within Ray actors to invoke Sandbox creation and secure code execution on sandboxes.

## Deploying KubeRay with Agent Sandbox

The following example creates a KubeRay RayJob, which runs a Ray job that uses the Agent Sandbox SDK to invoke code execution in a secure sandbox. It is highly recommended to keep Pods used for sandboxing decoupled from the Ray cluster itself.

### Step 1: Create a GKE cluster and Node Pools   

Run the following command to create a GKE cluster. In this example we will create two separate node pools, one for KubeRay provisioned Pods and one for Sandbox pods using the gVisor runtime:

```bash
# 1. The System Pool (For Ray Head and Ray Workers)
# We use e2-standard-4 (4 vCPU, 16GB RAM) to give the Ray orchestration processes enough headroom.

gcloud container node-pools create ray-system-pool \
    --cluster=<YOUR_CLUSTER_NAME> \
    --machine-type=e2-standard-4 \
    --num-nodes=2

# 2. The Sandbox Pool (For untrusted code execution)
# Explicitly enable gVisor on this pool. GKE will automatically taint
# these nodes so regular workloads aren't placed here.
gcloud container node-pools create ray-gvisor-pool \
    --cluster=<YOUR_CLUSTER_NAME> \
    --sandbox type=gvisor \
    --machine-type=e2-standard-4 \
    --num-nodes=1
```

### Step 2: Install KubeRay Operator

If you haven't already, install the KubeRay Operator using Helm:

```bash
# Add the KubeRay Helm repository
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

# Deploy KubeRay Operator
helm install kuberay-operator kuberay/kuberay-operator --version 1.1.0 --namespace default
```

Verify that the operator is running:
```bash
kubectl get pods -l app.kubernetes.io/name=kuberay-operator
```

### Step 3: Deploy Agent Sandbox 

Install the Custom Resource Definitions (CRDs), controllers, and extensions from the official Agent Sandbox release.

```bash
# Set the desired version of Agent Sandbox
export VERSION="v0.1.0"

# Install core manifests and API extensions
kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${VERSION}/manifest.yaml
kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${VERSION}/extensions.yaml
```

### Step 4: Apply RBAC Permissions for the Ray Workers

The Agent Sandbox Python SDK running inside Ray Workers needs to talk to the Kubernetes API to claim and delete sandboxes. In this example we will use the default service account token in the default namespace to grant Ray workers the ability to spawn Sandboxes:

Create a file named `rbac.yaml` with the following content:
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ray-sandbox-manager-role
  namespace: default
rules:
# 1. Permission to claim and delete sandboxes (the pool controller
#    provisions fresh ones to backfill, no SDK-side recycling involved)
- apiGroups: ["extensions.agents.x-k8s.io"]
  resources: ["sandboxclaims"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]

# 2. Permission to watch the underlying sandbox status
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

### Step 5: Deploy Sandbox Infrastructure

Run the following command to create sandbox infrastructure using Agent Sandbox:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/sandbox.yaml
```

The following resources are created:
- **`SandboxTemplate`**: defines the per-sandbox podSpec. The Pod is configured to use gVisor, sets `automountServiceAccountToken: false` so untrusted code inside the sandbox cannot read a Kubernetes ServiceAccount token, and sets `networkPolicyManagement: Unmanaged` because the NetworkPolicy below is stricter than the controller's Secure Default. The template also labels every sandbox pod with `app: ray-native-pool` so other selectors (the NetworkPolicy podSelector, your own `kubectl get` queries) can target them by a stable, human-readable label.
- **`SandboxWarmPool`** (`ray-native-pool`) — keeps 6 pre-booted sandbox pods ready so the Ray actors' claims complete in under 200ms.
- **`NetworkPolicy`** (`ray-native-pool-restrict-egress`) — default-denies egress for every sandbox pod except DNS. This is what makes the demo's containment claims in Step 7 concrete: the `lateral_internal_probe.py` and `reverse_shell_attempt.py` snippets hit TCP timeouts because the CNI drops their packets at the node, not because of any cluster-default policy you may or may not have.

Verify the warm pool pods are running:

```bash
kubectl get pods -l app=ray-native-pool
```

Based on the configuration of the SandboxWarmpool, we expect 6 gVisor Pods to be running:

```bash
GVISOR_POD=$(kubectl get pod -l app=ray-native-pool -o jsonpath='{.items[0].metadata.name}')
kubectl get pod "$GVISOR_POD" -o jsonpath='{.spec.automountServiceAccountToken}{"\n"}'   # expect: false
kubectl exec "$GVISOR_POD" -- ls /var/run/secrets/kubernetes.io/serviceaccount/ 2>&1     # expect: No such file or directory
```

### Step 6: Create the RayJob

Run the following command to create a RayJob resource:

```sh
# Create the RayJob
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/ray-standard-setup.yaml
```

The RayJob is configured to do the following:
1. Create a RayCluster 
2. Submit a Ray job that runs `sandboxed_code_execution.py` on the Ray cluster
3. The driver script will run Ray actors that use the Agent Sandbox Python SDK to invoke Sandbox creation. 
4. Once the Sandbox environments are created, the actor will execute some code in the sandboxed environment and verify its output. 

### Step 7: Verify the output

Monitor the status and query the execution logs of the submitted RayJob:

```sh
# List running job pods
kubectl get pods -l job-name=agent-sandbox-code-execution-demo

# Stream the demo logs
kubectl logs -f -l job-name=agent-sandbox-code-execution-demo
```
Once the job starts, three `SandboxExecutor` Ray actors each claim one pod from `ray-native-pool` (the SDK reports per-actor adoption latency — sub-200ms when the warm pool is healthy). Every Python snippet that follows runs **inside the sandbox pod, never on the Ray worker**: gVisor isolates the syscall surface, the `ray-native-pool-restrict-egress` NetworkPolicy applied in Step 5 default-denies all egress except DNS, and `sandbox.commands.run(..., timeout=5)` bounds wall-clock blast radius per call. 

Six snippets are dispatched to the executors in parallel via `ray.get` three safe and three deliberately dangerous. The driver prints one verdict line per snippet — `[OK]` if a safe snippet succeeded or a dangerous one was **caught by the sandbox**, `[UNEXPECTED]` otherwise — followed by an `X/6 snippets behaved as expected` summary.

Expected output (abridged):

```
... cli.py:66 -- Job 'agent-sandbox-code-execution-demo-xxxxx' submitted successfully
...
Spinning up 3 SandboxExecutor actors, each claiming one sandbox from the 'ray-native-pool' WarmPool...
(SandboxExecutor pid=...) [executor-2] adopted sandbox 'sandbox-claim-...' in 0.164s
(SandboxExecutor pid=...) [executor-0] adopted sandbox 'sandbox-claim-...' in 0.147s
(SandboxExecutor pid=...) [executor-1] adopted sandbox 'sandbox-claim-...' in 0.141s

Dispatching 6 snippets across 3 sandbox executors (Ray fans them out in parallel)...

============================================================
Execution Results
============================================================

[OK] compute_fib.py (expected: safe) -> SUCCEEDED
  stdout: fib(20) = 6765

[OK] json_aggregation.py (expected: safe) -> SUCCEEDED
  stdout: {"mean": 11.0, "max": 25}

[OK] string_processing.py (expected: safe) -> SUCCEEDED
  stdout: {'words': 3, 'chars': 18}

[OK] lateral_internal_probe.py (expected: dangerous) -> EXITED (1)
  stderr: Traceback (most recent call last):
  File "/app/lateral_internal_probe.py", line 6, in <module>
    s.connect(('10.0.0.1', 6379))
TimeoutError: timed out

[OK] reverse_shell_attempt.py (expected: dangerous) -> EXITED (1)
  stderr: Traceback (most recent call last):
  File "/app/reverse_shell_attempt.py", line 4, in <module>
    s.connect(('192.0.2.1', 4444))
TimeoutError: timed out

[OK] cpu_burn_loop.py (expected: dangerous) -> RAISED
  error: SandboxRequestError: Failed to communicate with the sandbox at http://...:8888/execute.

============================================================
Summary: 6/6 snippets behaved as expected
============================================================
```


---

## Clean Up

```bash
# Delete the RayJob
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/ray-standard-setup.yaml

# Delete the SandboxTemplate, SandboxWarmPool, and NetworkPolicy
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/sandbox.yaml

# Delete RBAC rules
kubectl delete -f rbac.yaml

# Delete node pools
gcloud container node-pools delete ray-gvisor-pool --cluster=<YOUR_CLUSTER_NAME>
gcloud container node-pools delete ray-system-pool --cluster=<YOUR_CLUSTER_NAME>
```
