(kuberay-agent-sandbox)=

# Sandboxed Code Execution with Ray and Agent Sandbox

This example shows how to use the [Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox) with Ray and KubeRay to orchestrate code execution in a secure sandboxed environment. This example uses GKE and gVisor but can be modified to work on other sandbox runtimes.

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
gcloud container node-pools create ray-worker-pool \
    --cluster=<YOUR_CLUSTER_NAME> \
    --machine-type=e2-standard-4 \
    --num-nodes=2

gcloud container node-pools create ray-gvisor-pool \
    --cluster=<YOUR_CLUSTER_NAME> \
    --sandbox type=gvisor \
    --machine-type=e2-standard-4 \
    --num-nodes=1
```

### Step 2: Install KubeRay operator

Follow the instructions in [KubeRay operator](kuberay-operator-deploy) to install the KubeRay operator.

### Step 3: Deploy Agent Sandbox 

Install the Custom Resource Definitions (CRDs), controllers, and extensions from the official Agent Sandbox release.

```bash
export VERSION="v0.5.0"

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

### Step 5: Deploy Sandbox Infrastructure

Run the following command to create sandbox infrastructure using Agent Sandbox:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/sandbox.yaml
```

The following resources are created:
- **`SandboxTemplate`**: defines the per-sandbox podSpec. The Pod is configured to use gVisor, sets `automountServiceAccountToken: false` so untrusted code inside the sandbox cannot read a Kubernetes ServiceAccount token, and sets `networkPolicyManagement: Unmanaged` because the NetworkPolicy below is stricter than the controller's Secure Default. The template also labels every sandbox pod with `app: python-runtime-pool` so other selectors (the NetworkPolicy podSelector, your own `kubectl get` queries) can target them by a stable, human-readable label.
- **`SandboxWarmPool`** (`python-runtime-pool`) — keeps 6 pre-booted sandbox pods ready so the Ray actors' claims complete in under 200ms.
- **`NetworkPolicy`** (`python-runtime-pool-restrict-egress`) — default-denies egress for every sandbox pod except DNS. This is what provides concrete containment, ensuring packets are dropped by the CNI at the node rather than relying on cluster-default policies.

Verify the warm pool pods are running:

```bash
kubectl get pods -l app=python-runtime-pool
```

Based on the configuration of the SandboxWarmpool, we expect 6 gVisor Pods to be running:

```bash
GVISOR_POD=$(kubectl get pod -l app=python-runtime-pool -o jsonpath='{.items[0].metadata.name}')
kubectl get pod "$GVISOR_POD" -o jsonpath='{.spec.automountServiceAccountToken}{"\n"}'   # expect: false
kubectl exec "$GVISOR_POD" -- ls /var/run/secrets/kubernetes.io/serviceaccount/ 2>&1     # expect: No such file or directory
```

### Step 6: Create the RayJob

Run the following command to create a RayJob resource:

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/agent-sandbox/ray-cluster.yaml
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

Once the job starts, two `SandboxExecutor` Ray actors each claim one pod from `python-runtime-pool` (the SDK reports per-actor adoption latency — sub-200ms when the warm pool is healthy). Every Python snippet that follows runs **inside the sandbox pod, never on the Ray worker**: gVisor isolates the syscall surface, the `python-runtime-pool-restrict-egress` NetworkPolicy applied in Step 5 default-denies all egress except DNS, and `sandbox.commands.run(..., timeout=5)` bounds wall-clock blast radius per call. 

Expected output (abridged):

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
