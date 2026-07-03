(kuberay-rayjob-sidecar-submitter-restart)=
# RayJob SidecarSubmitterRestart

This guide walks through enabling the `SidecarSubmitterRestart` feature gate and verifying that the submitter container recovers from a simulated crash without failing the RayJob. If you are unfamiliar with RayJob and KubeRay, see the {ref}`RayJob Quickstart <kuberay-rayjob-quickstart>` first.

## Prerequisites

* This feature requires Kubernetes v1.35+.
  * KubeRay v1.7.0 or higher: Ray v2.54.0 or higher.

## Verifying SidecarSubmitterRestart on kind

### Step 1: Create a Kubernetes v1.35+ cluster on kind

```sh
kind create cluster --name rayjob-test --image kindest/node:v1.35.0
```

### Step 2: Install the KubeRay operator with `SidecarSubmitterRestart` enabled

```sh
helm upgrade --install kuberay-operator kuberay/kuberay-operator --version v1.7.0 \
  --set "featureGates[0].name=SidecarSubmitterRestart" \
  --set "featureGates[0].enabled=true"
```

### Step 3: Create a long-running RayJob in `SidecarMode`

The job runs for ~5 minutes so there is time to simulate a crash mid-run.

```sh
kubectl apply -f - <<'EOF'
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sidecar-restart
spec:
  submissionMode: SidecarMode
  entrypoint: python /home/ray/samples/sample_code.py
  submitterConfig:
    backoffLimit: 3
  rayClusterSpec:
    rayVersion: '2.56.0'
    headGroupSpec:
      rayStartParams: {}
      template:
        spec:
          containers:
          - name: ray-head
            image: rayproject/ray:2.56.0
            resources:
              limits:
                cpu: "1"
                memory: "2Gi"
            volumeMounts:
            - mountPath: /home/ray/samples
              name: code-sample
          volumes:
          - name: code-sample
            configMap:
              name: ray-job-code-sample
    workerGroupSpecs:
    - replicas: 1
      groupName: small-group
      rayStartParams: {}
      template:
        spec:
          containers:
          - name: ray-worker
            image: rayproject/ray:2.56.0
            resources:
              limits:
                cpu: "1"
                memory: "1Gi"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: ray-job-code-sample
data:
  sample_code.py: |
    import ray, time
    ray.init()

    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0
        def inc(self):
            self.count += 1
        def get(self):
            return self.count

    c = Counter.remote()
    for _ in range(5):
        ray.get(c.inc.remote())
        print(f"count={ray.get(c.get.remote())}")

    print("Entering long-running phase...")
    for i in range(300):
        ray.get(c.inc.remote())
        if i % 30 == 0:
            print(f"tick={i}")
        time.sleep(1)
    print("Done.")
EOF
```

### Step 4: Simulate a submitter crash

Wait until `jobDeploymentStatus` is `Running`, then force-stop the submitter container to mimic a transient failure:

```sh
JOB_ID=$(kubectl get rayjob rayjob-sidecar-restart -o jsonpath='{.status.jobId}')
CLUSTER=$(kubectl get rayjob rayjob-sidecar-restart -o jsonpath='{.status.rayClusterName}')
HEAD_POD=$(kubectl get pods -l ray.io/cluster=$CLUSTER,ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
CONTAINER_ID=$(kubectl get pod $HEAD_POD \
  -o jsonpath='{.status.containerStatuses[?(@.name=="ray-job-submitter")].containerID}' \
  | sed 's|containerd://||')

docker exec rayjob-test-control-plane crictl stop $CONTAINER_ID
```

### Step 5: Verify recovery

The submitter container should restart and reattach to the log stream. The RayJob should remain `Running`:

```sh
# restartCount for ray-job-submitter should increment to 1
kubectl get pod $HEAD_POD \
  -o jsonpath='{range .status.containerStatuses[*]}{.name}{" restartCount="}{.restartCount}{"\n"}{end}'

# RayJob deployment status should still be Running
kubectl get rayjob rayjob-sidecar-restart -o jsonpath='{.status.jobDeploymentStatus}'

# Ray job should still be RUNNING with the same job ID
kubectl exec $HEAD_POD -c ray-head -- \
  ray job status --address=http://127.0.0.1:8265 "$JOB_ID"
```

### Step 6: Clean up

```sh
kubectl delete rayjob rayjob-sidecar-restart
kubectl delete configmap ray-job-code-sample
helm uninstall kuberay-operator
kind delete cluster --name rayjob-test
```
