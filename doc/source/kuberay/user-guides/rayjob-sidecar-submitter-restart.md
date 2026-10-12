(kuberay-rayjob-sidecar-submitter-restart)=
# RayJob SidecarSubmitterRestart

`SidecarMode` runs the RayJob submitter as a container inside the Ray head Pod. Unlike `K8sJobMode`, `SidecarMode` does not require pulling a duplicated Ray image and avoids the need for inter-pod communication as the submitter container reaches the head container through localhost. Keeping everything in one Pod also makes it easier for batch schedulers to reason about, and lets KubeRay check the submitter's status from the Pod's container status, which it already watches via its informer cache, instead of polling the Ray dashboard on every reconciliation. Compared to `HTTPMode`, users can also see logs directly in STDOUT/STDERR. Despite these benefits, it couples the submitter's lifecycle to the head Pod's. This guide walks through enabling the `SidecarSubmitterRestart` feature gate, which softens that coupling by letting the submitter container recover from transient failures without failing the RayJob. If you are unfamiliar with RayJob and KubeRay, see the {ref}`RayJob Quickstart <kuberay-rayjob-quickstart>` first.

## Prerequisites

* This feature requires Kubernetes v1.35+.
* KubeRay v1.7.0 or higher.
* Ray v2.54.0 or higher.

## Behavior and caveats

* The submitter container's `restartPolicy` is set to `OnFailure` at the container level, independent of the head Pod's pod-level `restartPolicy: Never`. A non-zero exit code restarts only the submitter container in place without restarting the `ray-head` container. A failure in the Ray job's own code doesn't make the submitter exit non-zero, so it won't trigger a restart by itself.
* On restart, the submitter checks the Ray job status first. If the Ray job is still running, the submitter reattaches to the log stream instead of resubmitting, so a dropped log-follow connection doesn't force-kill a running job.
* The KubeRay operator only validates the API server version. Per the Kubernetes version skew policy, worker node kubelets can be up to 3 minor versions older, so the node running the Ray head Pod must also be on v1.35+. If that kubelet doesn't support `ContainerRestartRules`, the per-container restart policy is silently ignored, and the operator's default 30-second submitter-finished timeout can mark the RayJob `Failed` even though the Ray job is still running.
* Exceeding `submitterConfig.backoffLimit` still marks the RayJob as failed even if the Ray job itself is still running. The default is 2, and it currently can't be overridden for `SidecarMode` as the KubeRay validating webhook rejects any `submitterConfig` when `submissionMode` is `SidecarMode`.

## Verifying SidecarSubmitterRestart on kind

### Step 1: Create a Kubernetes v1.35+ cluster on kind

```sh
kind create cluster --name rayjob-test --image kindest/node:v1.35.0
```

### Step 2: Install the KubeRay operator with `SidecarSubmitterRestart` enabled

```sh
helm upgrade --install kuberay-operator kuberay/kuberay-operator --version 1.7.0 \
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
                memory: "5Gi"
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
# restartCount for ray-job-submitter should increment to 1 while ray-head should remain 0
kubectl get pod $HEAD_POD \
  -o jsonpath='{range .status.containerStatuses[*]}{.name}{" restartCount="}{.restartCount}{"\n"}{end}'
# ray-head restartCount=0
# ray-job-submitter restartCount=1

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
