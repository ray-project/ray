(kubectl-plugin)=

# Use Kubectl Plugin (Alpha Feature)

Today it is incredibly challenging for data scientists and AI researchers unfamiliar with Kubernetes to start using Ray on Kubernetes. However, running Ray on Kubernetes is advantageous or necessary in certain environments.

Therefore, starting from KubeRay v1.2.2, we have introduced a `kubectl ray` plugin to simplify the process of running Ray on Kubernetes. The plugin is designed to help users who are not familiar with Kubernetes to run Ray on Kubernetes with ease.

## Installation

Please follow the instructions in the [KubeRay Kubectl Plugin](https://github.com/ray-project/kuberay/tree/master/kubectl-plugin) to install the plugin.

## Usage

After installing the plugin, you can use `kubectl ray --help` to see the available commands and options.

## Example

Here assume you have a Ray cluster running on Kubernetes. Please see {ref}`RayCluster Quickstart <kuberay-raycluster-quickstart>` if you don't have a Ray cluster running on Kubernetes.

### Get all Ray clusters

```shell
$ kubectl ray cluster get

NAME                  NAMESPACE   DESIRED WORKERS   AVAILABLE WORKERS   CPUS   GPUS   TPUS   MEMORY   AGE
ray-cluster-kuberay   default     1                                     2      0      0      3G       24s
```

### Forward local ports to Ray cluster

```shell
$ kubectl ray session ray-cluster-kuberay

Forwarding ports to service ray-cluster-kuberay-head-svc
Ray Dashboard: http://localhost:8265
Ray Interactive Client: http://localhost:10001

Forwarding from 127.0.0.1:8265 -> 8265
Forwarding from [::1]:8265 -> 8265
Forwarding from 127.0.0.1:10001 -> 10001
Forwarding from [::1]:10001 -> 10001
```

### Get Ray cluster logs

```shell
$ kubectl ray log ray-cluster-kuberay

No output directory specified, creating dir under current directory using cluster name.
Downloading file ./ for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./old/ for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./gcs_server.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./gcs_server.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./events/ for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./events/event_GCS.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./events/event_AUTOSCALER.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./events/event_RAYLET.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./debug_state_gcs.txt for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./monitor.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./monitor.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./ray_client_server.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./ray_client_server.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./dashboard.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./dashboard.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./monitor.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./raylet.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./raylet.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./log_monitor.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./debug_state.txt for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./log_monitor.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./dashboard_agent.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./agent-424238335.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./agent-424238335.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./runtime_env_agent.out for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./runtime_env_agent.err for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./runtime_env_agent.log for Ray Head ray-cluster-kuberay-head-qkrv8
Downloading file ./nsight/ for Ray Head ray-cluster-kuberay-head-qkrv8
```

## Submit a Ray job

This is a wrapper around the `ray job submit` command. The plugin will automatically forward the ports to the Ray cluster and submit the job.

Assume that under the current directory, you have a file named `task.py`.

```python
import ray
ray.init(address="auto")

@ray.remote
def f(x):
    return x * x

futures = [f.remote(i) for i in range(4)]
print(ray.get(futures)) # [0, 1, 4, 9]
```

And a file named `ray-job.usermode.yaml`

```yaml
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-usermode
spec:
  submissionMode: UserMode
  rayClusterSpec:
    rayVersion: '2.34.0'
    headGroupSpec:
      rayStartParams:
        dashboard-host: '0.0.0.0'
      template:
        spec:
          containers:
            - name: ray-head
              image: rayproject/ray:2.34.0
              ports:
                - containerPort: 6379
                  name: gcs-server
                - containerPort: 8265
                  name: dashboard
                - containerPort: 10001
                  name: client
              resources:
                limits:
                  cpu: "1"
                requests:
                  cpu: "200m"
    workerGroupSpecs:
      - replicas: 1
        minReplicas: 1
        maxReplicas: 3
        groupName: small-group
        rayStartParams: {}
        template:
          spec:
            containers:
              - name: ray-worker
                image: rayproject/ray:2.34.0
                lifecycle:
                  preStop:
                    exec:
                      command: [ "/bin/sh","-c","ray stop" ]
                resources:
                  limits:
                    cpu: "1"
                  requests:
                    cpu: "200m"
```

Note that in the RayJob spec, `submissionMode` must be set to `UserMode`.

And then you can submit the job by running:

```shell
$ kubectl ray job submit -f ray-job.usermode.yaml --working-dir . -- python task.py

Submitted RayJob rayjob-usermode.
Waiting for RayCluster
Checking Cluster Status for cluster rayjob-usermode-raycluster-phmfm...
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Cluster is not ready: Cannot determine cluster state
Waiting for portforwarding...Port Forwarding service rayjob-usermode-raycluster-phmfm-head-svc
Forwarding from 127.0.0.1:8265 -> 8265
Forwarding from [::1]:8265 -> 8265
Handling connection for 8265
Portforwarding started on http://localhost:8265
Ray command: [ray job submit --address http://localhost:8265 --working-dir . -- python task.py]
E0929 00:42:54.347828  393161 portforward.go:398] "Unhandled Error" err="error copying from local connection to remote stream: writeto tcp4 127.0.0.1:8265->127.0.0.1:48620: read tcp4 127.0.0.1:8265->127.0.0.1:48620: read: connection reset by peer" logger="UnhandledError"
Running ray submit job command...
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
2024-09-29 00:42:54,770 INFO dashboard_sdk.py:338 -- Uploading package gcs://_ray_pkg_b1cb9c96f2e8cb80.zip.
2024-09-29 00:42:54,770 INFO packaging.py:530 -- Creating a file package for local directory '.'.
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
Handling connection for 8265
2024-09-29 00:42:54,767 INFO cli.py:39 -- Job submission server address: http://localhost:8265
2024-09-29 00:42:55,085 SUCC cli.py:63 -- -------------------------------------------------------
2024-09-29 00:42:55,085 SUCC cli.py:64 -- Job 'raysubmit_XJ15mqiwvXQhaQ5k' submitted successfully
2024-09-29 00:42:55,085 SUCC cli.py:65 -- -------------------------------------------------------
2024-09-29 00:42:55,085 INFO cli.py:289 -- Next steps
2024-09-29 00:42:55,085 INFO cli.py:290 -- Query the logs of the job:
2024-09-29 00:42:55,085 INFO cli.py:292 -- ray job logs raysubmit_XJ15mqiwvXQhaQ5k
2024-09-29 00:42:55,085 INFO cli.py:294 -- Query the status of the job:
2024-09-29 00:42:55,085 INFO cli.py:296 -- ray job status raysubmit_XJ15mqiwvXQhaQ5k
2024-09-29 00:42:55,085 INFO cli.py:298 -- Request the job to be stopped:
2024-09-29 00:42:55,085 INFO cli.py:300 -- ray job stop raysubmit_XJ15mqiwvXQhaQ5k
2024-09-29 00:42:55,087 INFO cli.py:307 -- Tailing logs until the job exits (disable with --no-wait):
2024-09-28 09:42:55,587 INFO worker.py:1456 -- Using address 10.42.0.10:6379 set in the environment variable RAY_ADDRESS
2024-09-28 09:42:55,588 INFO worker.py:1596 -- Connecting to existing Ray cluster at address: 10.42.0.10:6379...
2024-09-28 09:42:55,591 INFO worker.py:1772 -- Connected to Ray cluster. View the dashboard at 10.42.0.10:8265
[0, 1, 4, 9]
2024-09-29 00:42:59,130 SUCC cli.py:63 -- ------------------------------------------
2024-09-29 00:42:59,131 SUCC cli.py:64 -- Job 'raysubmit_XJ15mqiwvXQhaQ5k' succeeded
2024-09-29 00:42:59,131 SUCC cli.py:65 -- ------------------------------------------
```
