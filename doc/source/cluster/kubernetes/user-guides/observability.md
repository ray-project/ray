(kuberay-observability)=

# KubeRay Observability

## Ray Dashboard

* To view the Ray dashboard running on the head Pod, users can follow [these instructions](kuberay-port-forward-dashboard).
* The Ray dashboard also integrates with Prometheus and Grafana. See [Using Prometheus and Grafana](kuberay-prometheus-grafana) for more details.
* The Ray dashboard also supports the "CPU Flame Graph" and "Stack Trace" features. Refer to [Profiling with py-spy](kuberay-pyspy-integration) to enable these features.

## KubeRay Observability

### Method 1: Check KubeRay operator's logs for errors

```bash
kubectl logs $KUBERAY_OPERATOR_POD -n $YOUR_NAMESPACE | tee operator-log
```

The command redirects the operator's logs to a file called `operator-log`. You can then search for errors in the file.

### Method 2: Check custom resource status

```bash
kubectl describe [raycluster|rayjob|rayservice] $CUSTOM_RESOURCE_NAME -n $YOUR_NAMESPACE
```

Check the status and events of the custom resource to see if there are any errors.

### Method 3: Check logs of Ray Pods

Check the Ray logs directly by accessing the log files on the Pods. See [Ray Logging](configure-logging) for more details.

```bash
kubectl exec -it $RAY_POD -n $YOUR_NAMESPACE -- bash
# Check the logs under /tmp/ray/session_latest/logs/
```

(kuberay-port-forward-dashboard)=
### Method 4: Check Dashboard

```bash
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl port-forward $RAY_POD -n $YOUR_NAMESPACE --address 0.0.0.0 8265:8265
# Check $YOUR_IP:8265 in your browser
```

### Method 5: Ray State CLI

You can use the [Ray State CLI](state-api-cli-ref) on the head Pod to check the status of Ray Serve applications.

```bash
# Log into the head Pod
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -it $HEAD_POD -- ray summary actors

# [Example output]:
# ======== Actors Summary: 2023-07-11 17:58:24.625032 ========
# Stats:
# ------------------------------------
# total_actors: 14


# Table (group by class):
# ------------------------------------
#     CLASS_NAME                          STATE_COUNTS
# 0   ...                                 ALIVE: 1
# 1   ...                                 ALIVE: 1
# 2   ...                                 ALIVE: 3
# 3   ...                                 ALIVE: 1
# 4   ...                                 ALIVE: 1
# 5   ...                                 ALIVE: 1
# 6   ...                                 ALIVE: 1
# 7   ...                                 ALIVE: 1
# 8   ...                                 ALIVE: 1
# 9   ...                                 ALIVE: 1
# 10  ...                                 ALIVE: 1
# 11  ...                                 ALIVE: 1
```