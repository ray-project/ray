(kuberay-observability)=

# KubeRay Observability

## Ray Dashboard

* To view the [Ray dashboard](observability-getting-started) running on the head Pod, follow [these instructions](kuberay-port-forward-dashboard).
* To integrate the Ray dashboard with Prometheus and Grafana, see [Using Prometheus and Grafana](kuberay-prometheus-grafana) for more details.
* To enable the "CPU Flame Graph" and "Stack Trace" features, see [Profiling with py-spy](kuberay-pyspy-integration).

## KubeRay Observability

Methods 1 and 2 address control plane observability, while methods 3, 4, and 5 focus on data plane observability.

### Method 1: Check KubeRay operator's logs for errors

```bash
# Typically, the operator's Pod name is kuberay-operator-xxxxxxxxxx-yyyyy.
kubectl logs $KUBERAY_OPERATOR_POD -n $YOUR_NAMESPACE | tee operator-log
```

Use this command to redirect the operator's logs to a file called `operator-log`. Then search for errors in the file.

### Method 2: Check custom resource status

```bash
kubectl describe [raycluster|rayjob|rayservice] $CUSTOM_RESOURCE_NAME -n $YOUR_NAMESPACE
```

After running this command, check the status and events of the custom resource for any errors.

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
kubectl port-forward $RAY_POD -n $YOUR_NAMESPACE 8265:8265
# Check $YOUR_IP:8265 in your browser to access the dashboard.
# For most cases, 127.0.0.1:8265 or localhost:8265 should work.
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