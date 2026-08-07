(jax-tpu-profiling)=

# JAX profiler for Ray on Kubernetes

This guide explains how to profile JAX workloads running on Ray TPU workers in a Kubernetes cluster.

## Prerequisites and image setup

To profile JAX workloads on TPU workers, ensure your environment meets the following requirements:

- Use a Ray Docker image with Ray 2.57+, or a custom image built with JAX profiler support, that has `jax`, `tensorflow`, `tensorboard`, and `tensorboard-plugin-profile` installed in the container's base Python environment. Note that `tensorflow` is required by the Ray Dashboard `ReporterAgent` on worker nodes to capture JAX profiles.
  :::{note}
  Installing `tensorflow` dynamically using `runtime_env` isn't supported for this feature.
  :::
- Set the environment variable `RAY_DASHBOARD_ENABLE_PROFILING=1` on the Ray head node container in your KubeRay `RayJob` or `RayCluster` YAML specification. The Ray Dashboard profiling endpoints are disabled by default for security reasons.

## Initialize the JAX profiler in user code

In your remote Ray task or actor executing JAX code on the TPU worker, call `init_jax_profiler()` after importing Ray. This starts an in-process gRPC profiling server inside the worker process, which defaults to port 9999, and automatically registers the port in the Ray GCS internal KV store so the Ray Dashboard can discover it:

```python
import ray
from ray.util.tpu import init_jax_profiler

ray.init()

@ray.remote(resources={"TPU": 4})
def train_step():
    # Initialize the in-process JAX profiler server and register with Ray GCS
    init_jax_profiler()

    # Your JAX training / XLA execution code here...
```

:::{note}
`init_jax_profiler()` listens on port 9999 by default and assumes at most one JAX worker process per host, which is the standard topology for multi-host TPU VM training.
:::

## Deploy a KubeRay RayJob

Deploy a TPU training `RayJob` using the sample configuration from the `ray-project/kuberay` repository:

```bash
# Clean up any previously deployed job with the same name:
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-job.tpu-jax.yaml --ignore-not-found

# Apply the RayJob specification:
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-job.tpu-jax.yaml
```

If you use your own `RayJob` manifest, ensure `RAY_DASHBOARD_ENABLE_PROFILING` is set to `"1"` on the head pod container so the dashboard profiling endpoints are accessible:

```yaml
# Under headGroupSpec.template.spec.containers:
- name: ray-head
  env:
    - name: RAY_DASHBOARD_ENABLE_PROFILING
      value: "1"
```

### Wait for pods to start
Check that all head and worker pods are running:

```bash
kubectl get pods -w
```

The output shows a head pod, such as `<RAY_JOB_NAME>-head-...`, and a worker TPU pod, such as `<RAY_JOB_NAME>-worker-...`, in the `Running` state.

## Port-forward the Ray Dashboard

Expose the head node dashboard port locally so you can invoke API endpoints:

```bash
kubectl port-forward svc/<RAY_JOB_NAME>-head-svc 8265:8265
```

Keep this port-forwarding process running in a dedicated shell session.

## Locate target worker PID and node ID

To trigger profiling dynamically, identify the worker node ID or IP address and the running worker process PID.

### Get the node ID
Query the Ray State API from your local terminal through the port-forwarded dashboard:

```bash
RAY_ADDRESS=http://localhost:8265 ray list nodes --detail
```

Alternatively, open the Ray Dashboard in your browser at `http://localhost:8265` and copy the hexadecimal node ID from the **Cluster** nodes view.

### Get the worker process PID
You can find the PID of the Python worker process executing your JAX task or actor in two ways:
- Open the Ray Dashboard at `http://localhost:8265` and navigate to the **Workers**, **Tasks**, or **Actors** tab to view the PID and node ID.
- Alternatively, use the Ray State CLI:

  ```bash
  RAY_ADDRESS=http://localhost:8265 ray list workers --detail
  ```

## Trigger JAX profiling dynamically

Open a new terminal window and run this curl request to trigger JAX profiling through the Ray Dashboard. Replace `<WORKER_PID>` and `<NODE_ID_HEX>` with your resolved values:

```bash
curl -G "http://localhost:8265/worker/jax_profile" \
  --data-urlencode "pid=<WORKER_PID>" \
  --data-urlencode "node_id=<NODE_ID_HEX>" \
  --data-urlencode "duration=5"
```

You can also pass `ip=<WORKER_IP>` instead of `node_id=<NODE_ID_HEX>`. If you specified a custom port when initializing the profiler, you can also pass `port=<PORT>` to override GCS auto-discovery.

### Expected dynamic endpoint response
The dashboard head looks up the JAX profiler port in the GCS registry, queries the TPU worker's in-process profiling server, collects the trace, and returns:

```json
{
  "result": true,
  "msg": "JAX profiling finished.",
  "data": {
    "traceDirectory": "profiles"
  }
}
```

## Verify trace outputs

Verify that the JAX trace file was captured and saved on the local filesystem of the TPU worker pod where your JAX workload executed. Replace `<TPU_WORKER_POD>` with the name of the worker pod that ran the task:

```bash
kubectl exec -it <TPU_WORKER_POD> -c ray-worker -- ls -la /tmp/ray/session_latest/logs/profiles
```

:::{tip}
If your cluster has only a single worker pod, you can retrieve its name with `kubectl get pods -l ray.io/node-type=worker -o jsonpath='{.items[0].metadata.name}'`. In multi-pod clusters, ensure you target the worker pod matching the `node_id` or IP address profiled in the previous step.
:::

Expected output:

```
-rw-r--r-- 1 ray users 79526813 Jun  2 15:26 /tmp/ray/session_latest/logs/profiles/plugins/profile/2026_06_02_15_26_15/localhost_9999.xplane.pb
```

The trace directory contains a `.xplane.pb` file with JAX execution and TPU hardware usage events.

## Visualize profiling trace in TensorBoard

Follow these steps to copy the JAX trace files locally and visualize TPU performance inside the TensorBoard profile dashboard.

### Copy trace folder from worker pod to local machine
Run this command from your local machine to download the captured profiling traces from the target TPU worker pod:

```bash
kubectl cp <TPU_WORKER_POD>:/tmp/ray/session_latest/logs/profiles/ ./tensorboard_logs/ -c ray-worker
```

### Install TensorBoard and profile plugin
Ensure you have TensorBoard and the official Google TPU profile plugin installed in your local Python environment:

```bash
pip install tensorboard tensorboard-plugin-profile
```

### Start TensorBoard server
Point TensorBoard's log directory parameter to the downloaded folder:

```bash
tensorboard --logdir ./tensorboard_logs/
```

### View the dashboard
Open your web browser and navigate to `http://localhost:6006/#profile` to analyze TPU compilation timelines, operators, and hardware execution metrics.

Sample output:

![TensorBoard TPU Profiler Overview](../images/jax-tpu-profiler-1.png)
![TensorBoard TPU Trace View](../images/jax-tpu-profiler-2.png)
